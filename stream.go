package oxinsider

// Incremental reader for GET /api/v1/stream.
//
// The endpoint answers an unbounded text/event-stream. The generated
// GetStreamWithResponse reads the body to EOF before it returns, so on a
// healthy connection it never returns and its buffer grows without bound;
// OpenStream is the streaming contract instead. It delivers each frame as it
// arrives, bounds the bytes held for one undelivered frame, and closes the
// connection when the context is cancelled or the reader is closed.
//
// Wire format (the OpenAPI document, paths./api/v1/stream, response 200):
//   - a data frame is "id: <seq>\ndata: <json-envelope>\n\n", where the
//     envelope is {seq, published_at, type, ...event-specific} and the SSE id
//     equals seq;
//   - a resync marker is "event: resync\nid: <seq>\ndata: <json>\n\n", where
//     the JSON is {type: "resync", completeness, from_sequence, to_sequence};
//     it means the resume point is outside the retained window or a live gap
//     was observed, so refetch current state;
//   - an idle connection sends ": keep-alive" comment frames.
//
// Protocol validity matches the TypeScript SDK's decoder (0xinsider#16248):
// a 2xx that is not text/event-stream, a data frame whose payload is not a
// JSON object, a frame with no usable sequence, a malformed resync marker and
// a frame past MaxFrameBytes without its delimiter each end the stream with a
// *StreamProtocolError. A malformed frame never moves LastSeq, so a caller can
// decide whether to resume from LastSeq (which replays the frame while it is
// retained), resume after the offending FrameID, or refetch state.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// DefaultMaxStreamFrameBytes bounds the bytes held for one undelivered frame:
// 1 MiB. The largest real frame is a few KB.
const DefaultMaxStreamFrameBytes = 1 << 20

// The stream's own time bounds. A total deadline would cut a healthy
// connection, so the stream is bounded where it can stall instead: opening it,
// and silence on it. Both are overridable, and 0 removes either one.
const (
	// DefaultStreamStartTimeout bounds the wait from the stream request to its
	// response headers. It is shorter than DefaultResponseHeaderTimeout, the
	// transport's backstop for the same wait, so a stream that does not open
	// fails with *StreamTimeoutError rather than a transport error.
	DefaultStreamStartTimeout = 10 * time.Second
	// DefaultStreamIdleTimeout ends a connection that has sent nothing for a
	// minute. The API sends a keep-alive comment every 5 seconds, so a minute
	// of silence is twelve missed keep-alives, not a quiet feed. The clock
	// runs only while the reader is waiting for bytes, so a slow consumer does
	// not trip it.
	DefaultStreamIdleTimeout = 60 * time.Second
)

// isStreamRequest reports whether req is GET /api/v1/stream, the unbounded
// event stream: the one request that gets no total deadline and that a client
// from New refuses outside OpenStream.
func isStreamRequest(req *http.Request) bool {
	return req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/api/v1/stream")
}

// maxStreamErrorBodyBytes bounds the body read for a non-2xx answer to the
// stream request, so a startup error stays typed and small.
const maxStreamErrorBodyBytes = 64 << 10

// streamOwnerKey marks a context whose /api/v1/stream request may pass the
// guard installed by New; see ErrStreamBuffered.
type streamOwnerKey struct{}

// RawStreamContext returns a context that lets GetStream hand you the raw
// *http.Response of GET /api/v1/stream through a client built with New. You
// then own the body: read it incrementally and close it. Without this marker
// New's client refuses the request with ErrStreamBuffered, because the
// generated GetStreamWithResponse would read the unbounded body to EOF.
// OpenStream is the supported path; this is the escape hatch.
func RawStreamContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, streamOwnerKey{}, true)
}

func streamAllowed(ctx context.Context) bool {
	allowed, ok := ctx.Value(streamOwnerKey{}).(bool)
	return ok && allowed
}

// ErrStreamBuffered is returned by a client built with New when GetStream or
// GetStreamWithResponse is called on GET /api/v1/stream outside OpenStream.
// GetStreamWithResponse reads the body to EOF, which a healthy stream never
// reaches, so the call would neither return nor bound its memory. Use
// OpenStream, or RawStreamContext with GetStream to own the raw body.
var ErrStreamBuffered = errors.New("oxinsider: GET /api/v1/stream is an unbounded event stream; GetStreamWithResponse would buffer it to EOF and never return. Use OpenStream, or GetStream with RawStreamContext to own the raw body")

// StreamProtocolErrorReason names what the stream did that the SSE contract
// does not allow.
type StreamProtocolErrorReason string

const (
	// StreamUnexpectedMediaType: a 2xx whose Content-Type is not text/event-stream.
	StreamUnexpectedMediaType StreamProtocolErrorReason = "unexpected_media_type"
	// StreamInvalidJSON: a data frame whose payload is not JSON, or is empty.
	StreamInvalidJSON StreamProtocolErrorReason = "invalid_json"
	// StreamInvalidEnvelope: a data frame whose payload is JSON but not an object.
	StreamInvalidEnvelope StreamProtocolErrorReason = "invalid_envelope"
	// StreamUnusableSequence: a data frame with no integer seq in the envelope and no integer SSE id.
	StreamUnusableSequence StreamProtocolErrorReason = "unusable_sequence"
	// StreamInvalidResync: a resync frame whose payload is not an object, or whose type is not "resync".
	StreamInvalidResync StreamProtocolErrorReason = "invalid_resync"
	// StreamFrameTooLarge: a frame grew past MaxFrameBytes without reaching its delimiter.
	StreamFrameTooLarge StreamProtocolErrorReason = "frame_too_large"
)

// StreamProtocolError reports that the stream broke the SSE contract. The
// connection is closed before it is returned. LastSeq is the last sequence
// delivered on this connection (HasLastSeq false when none was): no malformed
// frame moves the cursor. FrameID is the SSE id of the offending frame when it
// carried one, Event its SSE event name, Bytes its size, and MediaType the
// response's Content-Type for StreamUnexpectedMediaType. The raw payload is
// deliberately not carried: it may hold data you would not want in a log, and
// FrameID plus LastSeq name the frame exactly.
type StreamProtocolError struct {
	Reason     StreamProtocolErrorReason
	LastSeq    int64
	HasLastSeq bool
	FrameID    int64
	HasFrameID bool
	Event      string
	Bytes      int
	MediaType  string
}

func (e *StreamProtocolError) Error() string {
	where := "a frame"
	if e.HasFrameID {
		where = "frame id " + strconv.FormatInt(e.FrameID, 10)
	}
	after := "before any event was delivered"
	if e.HasLastSeq {
		after = "after seq " + strconv.FormatInt(e.LastSeq, 10)
	}
	switch e.Reason {
	case StreamUnexpectedMediaType:
		if e.MediaType == "" {
			return "oxinsider stream answered with no Content-Type instead of text/event-stream; not an SSE stream"
		}
		return "oxinsider stream answered Content-Type " + e.MediaType + " instead of text/event-stream; not an SSE stream"
	case StreamInvalidJSON:
		return fmt.Sprintf("oxinsider stream sent %s whose data is not JSON (%d bytes) %s", where, e.Bytes, after)
	case StreamInvalidEnvelope:
		return fmt.Sprintf("oxinsider stream sent %s whose data is not an envelope object %s", where, after)
	case StreamUnusableSequence:
		return fmt.Sprintf("oxinsider stream sent %s with no integer seq and no integer id %s", where, after)
	case StreamInvalidResync:
		return fmt.Sprintf("oxinsider stream sent a resync marker (%s) whose data is not a resync object %s", where, after)
	case StreamFrameTooLarge:
		return fmt.Sprintf("oxinsider stream sent a frame past %d bytes with no delimiter %s; the connection was closed", e.Bytes, after)
	default:
		return "oxinsider stream protocol error " + after
	}
}

// StreamHTTPError is the typed, bounded startup failure: the stream request
// was answered with a status other than 200. Response carries the decoded
// error body (JSON401, JSON429 and the other generated fields) and the parsed
// headers such as Retry-After; at most 64 KiB of the body was read.
type StreamHTTPError struct {
	StatusCode int
	Response   *GetStreamResponse
}

func (e *StreamHTTPError) Error() string {
	code := ""
	if e.Response != nil {
		for _, body := range []*ApiError{
			e.Response.JSON400, e.Response.JSON401, e.Response.JSON402, e.Response.JSON403,
			e.Response.JSON423, e.Response.JSON429, e.Response.JSON503,
		} {
			if body != nil {
				code = string(body.Error.Code)
				break
			}
		}
	}
	if code == "" {
		return fmt.Sprintf("oxinsider stream: HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("oxinsider stream: HTTP %d %s", e.StatusCode, code)
}

// StreamTimeoutPhase names which stream bound expired.
type StreamTimeoutPhase string

const (
	// StreamTimeoutStart: the response headers did not arrive within
	// WithStreamStartTimeout, so the stream never opened.
	StreamTimeoutStart StreamTimeoutPhase = "start"
	// StreamTimeoutIdle: the open stream sent nothing, not even a keep-alive,
	// for WithStreamIdleTimeout.
	StreamTimeoutIdle StreamTimeoutPhase = "idle"
)

// StreamTimeoutError reports that a stream bound expired. Phase says which
// one, Deadline is the bound, and LastSeq is the last sequence delivered on
// the connection (HasLastSeq false when none was), so an idle timeout can be
// resumed from where it stopped. Err is the underlying transport error when
// there was one. A context the caller cancelled is reported as ctx.Err(),
// never as this error.
type StreamTimeoutError struct {
	Phase      StreamTimeoutPhase
	Deadline   time.Duration
	LastSeq    int64
	HasLastSeq bool
	Err        error
}

func (e *StreamTimeoutError) Error() string {
	if e.Phase == StreamTimeoutStart {
		return fmt.Sprintf(
			"oxinsider stream: no response headers within %s, so the stream did not open (oxinsider.WithStreamStartTimeout changes it)",
			e.Deadline,
		)
	}
	after := "before any event was delivered"
	if e.HasLastSeq {
		after = "after seq " + strconv.FormatInt(e.LastSeq, 10)
	}
	return fmt.Sprintf(
		"oxinsider stream: nothing arrived for %s %s, not even a keep-alive; the connection was closed (oxinsider.WithStreamIdleTimeout changes it)",
		e.Deadline, after,
	)
}

// Unwrap exposes the transport error the bound produced, when there was one.
func (e *StreamTimeoutError) Unwrap() error { return e.Err }

// Timeout reports that this error is a timeout, matching the net.Error convention.
func (e *StreamTimeoutError) Timeout() bool { return true }

// StreamFrame is one delivered frame.
type StreamFrame struct {
	// Resync is true for an "event: resync" marker. Data is then the resync
	// object ({type, completeness, from_sequence, to_sequence}); refetch
	// current state and attach live.
	Resync bool
	// Seq is the cluster-shared sequence id: the envelope's seq, else the SSE
	// id. Pass the last delivered Seq as Last-Event-ID to resume. For a resync
	// marker Seq is the SSE id, and HasSeq is false when the marker carried
	// none.
	Seq int64
	// HasSeq is true when Seq is set. It is always true for a data frame.
	HasSeq bool
	// Type is the envelope's wire type ("WhaleTradesInserted",
	// "wallet_grade_changed", ...), or "resync" for a marker.
	Type string
	// PublishedAt is the envelope's published_at when present (ISO-8601).
	PublishedAt string
	// Data is the frame's JSON payload, a JSON object. Unmarshal it into the
	// struct for Type.
	Data json.RawMessage
}

// StreamOption configures OpenStream.
type StreamOption func(*streamConfig)

type streamConfig struct {
	maxFrameBytes int
	startTimeout  time.Duration
	idleTimeout   time.Duration
	editors       []RequestEditorFn
}

// WithMaxFrameBytes bounds the bytes held for one undelivered frame. The
// default is DefaultMaxStreamFrameBytes (1 MiB). A connection that sends more
// than this without a blank-line delimiter ends with StreamFrameTooLarge.
func WithMaxFrameBytes(n int) StreamOption {
	return func(c *streamConfig) { c.maxFrameBytes = n }
}

// WithStreamStartTimeout bounds the wait from the stream request to its
// response headers. The default is DefaultStreamStartTimeout (10 s); 0 leaves
// the opening bounded only by the transport's response-header timeout and the
// context. A stream that does not open in time fails with a
// *StreamTimeoutError whose Phase is StreamTimeoutStart.
func WithStreamStartTimeout(timeout time.Duration) StreamOption {
	return func(c *streamConfig) { c.startTimeout = timeout }
}

// WithStreamIdleTimeout ends the connection when nothing arrives on it, not
// even a keep-alive comment, for this long. The default is
// DefaultStreamIdleTimeout (60 s); 0 lets an idle connection stay open
// indefinitely. Next then returns a *StreamTimeoutError whose Phase is
// StreamTimeoutIdle and whose LastSeq says where to resume.
func WithStreamIdleTimeout(timeout time.Duration) StreamOption {
	return func(c *streamConfig) { c.idleTimeout = timeout }
}

// WithStreamRequestEditor adds a request editor to the stream request only,
// for example to set Last-Event-ID from a stored cursor when GetStreamParams
// is not convenient.
func WithStreamRequestEditor(fn RequestEditorFn) StreamOption {
	return func(c *streamConfig) { c.editors = append(c.editors, fn) }
}

// OpenStream opens GET /api/v1/stream and returns a reader that delivers
// frames as they arrive. The request carries the client's editors
// (WithBearerToken, the User-Agent) and Accept: text/event-stream. Cancel ctx
// to close the connection; Next then returns ctx.Err(). A non-200 answer is a
// *StreamHTTPError; a 200 that is not text/event-stream is a
// *StreamProtocolError with StreamUnexpectedMediaType. Close the reader when
// you are done with it.
//
// The stream carries no total deadline: it is meant to stay open. It is
// bounded where it can stall instead. Opening it is bounded by
// WithStreamStartTimeout, and silence on it by WithStreamIdleTimeout; either
// one ends the call with a *StreamTimeoutError, which names the phase and, for
// an idle timeout, the sequence to resume from.
//
//	reader, err := client.OpenStream(ctx, &oxinsider.GetStreamParams{LastEventID: &cursor})
//	if err != nil { ... }
//	defer reader.Close()
//	for {
//		frame, err := reader.Next()
//		if errors.Is(err, io.EOF) { break } // the server closed the stream
//		if err != nil { ... }               // *StreamProtocolError, ctx.Err(), or a transport error
//		if frame.Resync { refetch(); continue }
//		cursor = strconv.FormatInt(frame.Seq, 10)
//	}
func (c *ClientWithResponses) OpenStream(ctx context.Context, params *GetStreamParams, opts ...StreamOption) (*StreamReader, error) {
	cfg := streamConfig{
		maxFrameBytes: DefaultMaxStreamFrameBytes,
		startTimeout:  DefaultStreamStartTimeout,
		idleTimeout:   DefaultStreamIdleTimeout,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.maxFrameBytes < 1 {
		return nil, fmt.Errorf("oxinsider: MaxFrameBytes must be a positive integer, got %d", cfg.maxFrameBytes)
	}
	if cfg.startTimeout < 0 {
		return nil, fmt.Errorf("oxinsider: stream start timeout must not be negative, got %s", cfg.startTimeout)
	}
	if cfg.idleTimeout < 0 {
		return nil, fmt.Errorf("oxinsider: stream idle timeout must not be negative, got %s", cfg.idleTimeout)
	}
	editors := append([]RequestEditorFn{acceptEventStream}, cfg.editors...)

	// The reader owns this context for the life of the connection: the start
	// timer and the idle guard cancel it, and so does Close.
	streamCtx, cancel := context.WithCancel(ctx)
	var startExpired atomic.Bool
	var startTimer *time.Timer
	if cfg.startTimeout > 0 {
		startTimer = time.AfterFunc(cfg.startTimeout, func() {
			startExpired.Store(true)
			cancel()
		})
	}
	rsp, err := c.ClientInterface.GetStream(RawStreamContext(streamCtx), params, editors...)
	if startTimer != nil {
		startTimer.Stop()
	}
	if err != nil || startExpired.Load() {
		// startExpired with no error is the narrow race where the headers
		// landed as the timer fired: the context is already cancelled, so the
		// body is dead either way.
		if rsp != nil && rsp.Body != nil {
			err = closeWith(rsp.Body, err)
		}
		cancel()
		switch {
		case startExpired.Load() && ctx.Err() == nil:
			return nil, &StreamTimeoutError{Phase: StreamTimeoutStart, Deadline: cfg.startTimeout, Err: err}
		case err != nil:
			return nil, err
		default:
			return nil, ctx.Err()
		}
	}
	if rsp.StatusCode != http.StatusOK {
		limited := &http.Response{}
		*limited = *rsp
		limited.Body = struct {
			io.Reader
			io.Closer
		}{io.LimitReader(rsp.Body, maxStreamErrorBodyBytes), rsp.Body}
		parsed, err := ParseGetStreamResponse(limited)
		cancel()
		if err != nil {
			return nil, fmt.Errorf("oxinsider stream: HTTP %d, and the error body did not decode: %w", rsp.StatusCode, err)
		}
		return nil, &StreamHTTPError{StatusCode: rsp.StatusCode, Response: parsed}
	}
	mediaType, _, _ := mime.ParseMediaType(rsp.Header.Get("Content-Type"))
	if mediaType != "text/event-stream" {
		protocolErr := closeWith(rsp.Body, &StreamProtocolError{
			Reason:    StreamUnexpectedMediaType,
			MediaType: rsp.Header.Get("Content-Type"),
		})
		cancel()
		return nil, protocolErr
	}
	reader := &StreamReader{
		ctx:           ctx,
		cancel:        cancel,
		resp:          rsp,
		body:          rsp.Body,
		maxFrameBytes: cfg.maxFrameBytes,
		idleTimeout:   cfg.idleTimeout,
	}
	if cfg.idleTimeout > 0 {
		reader.idle = newIdleGuard(rsp.Body, cfg.idleTimeout, cancel)
		reader.body = reader.idle
	}
	reader.reader = bufio.NewReaderSize(reader.body, 64<<10)
	return reader, nil
}

// closeWith closes what the caller is giving up on and joins any close error
// onto the error it already has, so neither is dropped.
func closeWith(body io.Closer, err error) error {
	if closeErr := body.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}

// idleGuard cancels the stream when the connection sends nothing for too long.
// The clock runs only while a read is in flight, so a caller that takes its
// time between frames does not trip it; only the server's silence does.
type idleGuard struct {
	inner   io.ReadCloser
	timeout time.Duration
	timer   *time.Timer
	expired atomic.Bool
}

func newIdleGuard(body io.ReadCloser, timeout time.Duration, cancel context.CancelFunc) *idleGuard {
	guard := &idleGuard{inner: body, timeout: timeout}
	guard.timer = time.AfterFunc(timeout, func() {
		guard.expired.Store(true)
		cancel()
	})
	// Stopped until the first read: the clock is the server's silence, not the
	// time between OpenStream and the first Next.
	guard.timer.Stop()
	return guard
}

func (g *idleGuard) Read(p []byte) (int, error) {
	g.timer.Reset(g.timeout)
	n, err := g.inner.Read(p)
	g.timer.Stop()
	return n, err
}

func (g *idleGuard) Close() error {
	g.timer.Stop()
	return g.inner.Close()
}

func acceptEventStream(_ context.Context, req *http.Request) error {
	req.Header.Set("Accept", "text/event-stream")
	return nil
}

// StreamReader delivers the frames of one stream connection. It is not safe
// for concurrent use.
type StreamReader struct {
	ctx           context.Context
	cancel        context.CancelFunc
	resp          *http.Response
	body          io.ReadCloser
	idle          *idleGuard
	idleTimeout   time.Duration
	reader        *bufio.Reader
	maxFrameBytes int

	lastSeq    int64
	hasLastSeq bool
	retry      time.Duration
	hasRetry   bool
	closed     bool
	done       error
}

// Response returns the stream's HTTP response for its headers (X-Request-Id,
// the rate-limit headers). The body belongs to the reader; do not read it.
func (r *StreamReader) Response() *http.Response { return r.resp }

// LastSeq returns the sequence of the last data frame delivered on this
// connection, and false when none was. Resend it as Last-Event-ID to resume.
// A malformed frame never moves it.
func (r *StreamReader) LastSeq() (int64, bool) { return r.lastSeq, r.hasLastSeq }

// RetryHint returns the reconnection delay the server last sent in an SSE
// "retry:" field, and false when it sent none. The API does not send one
// today; a client that reconnects should also honor Retry-After on a
// *StreamHTTPError.
func (r *StreamReader) RetryHint() (time.Duration, bool) { return r.retry, r.hasRetry }

// Close closes the connection and releases the stream's context. Next returns
// io.EOF afterwards. Close is idempotent.
func (r *StreamReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.done == nil {
		r.done = io.EOF
	}
	r.cancel()
	return r.body.Close()
}

// fail records a terminal error, closes the connection and returns the error.
func (r *StreamReader) fail(err error) error {
	r.done = err
	if closeErr := r.Close(); closeErr != nil {
		return errors.Join(err, closeErr)
	}
	return err
}

// Next blocks until the next frame arrives and returns it. It returns io.EOF
// when the server closes the stream cleanly (a partial frame at EOF is
// discarded, as the SSE specification says) or after Close; ctx.Err() when
// the context given to OpenStream is cancelled; a *StreamTimeoutError when the
// connection sent nothing for the idle bound; a *StreamProtocolError when
// the stream breaks the contract; or the transport error. Keep-alive comment
// frames are consumed silently. Every error is terminal: the connection is
// closed and later calls return the same error.
func (r *StreamReader) Next() (StreamFrame, error) {
	if r.done != nil {
		return StreamFrame{}, r.done
	}
	var (
		event      string
		id         int64
		hasID      bool
		data       []byte
		hasData    bool
		sawField   bool
		frameBytes int
	)
	for {
		line, err := r.readLine(&frameBytes)
		if err != nil {
			if r.idle != nil && r.idle.expired.Load() && r.ctx.Err() == nil {
				// The idle guard cancelled the stream's context; the read
				// error below is that cancellation, not the server's doing.
				return StreamFrame{}, r.fail(&StreamTimeoutError{
					Phase:      StreamTimeoutIdle,
					Deadline:   r.idleTimeout,
					LastSeq:    r.lastSeq,
					HasLastSeq: r.hasLastSeq,
					Err:        err,
				})
			}
			if errors.Is(err, io.EOF) {
				return StreamFrame{}, r.fail(io.EOF)
			}
			if ctxErr := r.ctx.Err(); ctxErr != nil {
				return StreamFrame{}, r.fail(ctxErr)
			}
			var protocolErr *StreamProtocolError
			if errors.As(err, &protocolErr) {
				protocolErr.LastSeq, protocolErr.HasLastSeq = r.lastSeq, r.hasLastSeq
				protocolErr.Event = event
				if hasID {
					protocolErr.FrameID, protocolErr.HasFrameID = id, true
				}
			}
			return StreamFrame{}, r.fail(err)
		}
		if len(line) == 0 {
			// Blank line: dispatch the frame if it carried a field. A frame
			// made only of comments (": keep-alive") is consumed silently.
			if !sawField {
				frameBytes = 0
				continue
			}
			frame, err := r.decode(event, id, hasID, data, hasData)
			if err != nil {
				return StreamFrame{}, r.fail(err)
			}
			if !frame.Resync {
				r.lastSeq, r.hasLastSeq = frame.Seq, true
			}
			return frame, nil
		}
		if line[0] == ':' {
			continue
		}
		field, value := splitField(line)
		switch field {
		case "event":
			event = value
			sawField = true
		case "id":
			// The spec forbids an id that carries U+0000; the API's ids are
			// decimal. A non-integer id is ignored, as the TypeScript SDK
			// ignores a non-finite one.
			if n, err := strconv.ParseInt(value, 10, 64); err == nil {
				id, hasID = n, true
			}
			sawField = true
		case "data":
			if hasData {
				data = append(data, '\n')
			}
			data = append(data, value...)
			hasData = true
			sawField = true
		case "retry":
			if ms, err := strconv.ParseInt(value, 10, 64); err == nil && ms >= 0 {
				r.retry, r.hasRetry = time.Duration(ms)*time.Millisecond, true
			}
			sawField = true
		default:
			// Unknown SSE field: ignored per the specification.
		}
	}
}

// readLine returns the next line without its LF or CRLF terminator, adding
// its bytes to *frameBytes and failing with StreamFrameTooLarge when the
// frame passes maxFrameBytes without a delimiter. At EOF a partial line is
// discarded and io.EOF is returned.
func (r *StreamReader) readLine(frameBytes *int) ([]byte, error) {
	var line []byte
	for {
		chunk, err := r.reader.ReadSlice('\n')
		*frameBytes += len(chunk)
		if *frameBytes > r.maxFrameBytes {
			return nil, &StreamProtocolError{Reason: StreamFrameTooLarge, Bytes: *frameBytes}
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			line = append(line, chunk...)
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, io.EOF
			}
			return nil, err
		}
		line = append(line, chunk...)
		line = bytes.TrimSuffix(line, []byte("\n"))
		line = bytes.TrimSuffix(line, []byte("\r"))
		return line, nil
	}
}

func splitField(line []byte) (string, string) {
	colon := bytes.IndexByte(line, ':')
	if colon == -1 {
		return string(line), ""
	}
	value := line[colon+1:]
	if len(value) > 0 && value[0] == ' ' {
		value = value[1:]
	}
	return string(line[:colon]), string(value)
}

// decode turns one parsed frame into a StreamFrame or a *StreamProtocolError.
func (r *StreamReader) decode(event string, id int64, hasID bool, data []byte, hasData bool) (StreamFrame, error) {
	protocolErr := func(reason StreamProtocolErrorReason) error {
		return &StreamProtocolError{
			Reason: reason, LastSeq: r.lastSeq, HasLastSeq: r.hasLastSeq,
			FrameID: id, HasFrameID: hasID, Event: event, Bytes: len(data),
		}
	}
	if !hasData || len(bytes.TrimSpace(data)) == 0 || !json.Valid(data) {
		if event == "resync" {
			return StreamFrame{}, protocolErr(StreamInvalidResync)
		}
		return StreamFrame{}, protocolErr(StreamInvalidJSON)
	}
	var object map[string]json.RawMessage
	if bytes.TrimSpace(data)[0] != '{' || json.Unmarshal(data, &object) != nil {
		if event == "resync" {
			return StreamFrame{}, protocolErr(StreamInvalidResync)
		}
		return StreamFrame{}, protocolErr(StreamInvalidEnvelope)
	}
	var wireType string
	if raw, ok := object["type"]; ok {
		if err := json.Unmarshal(raw, &wireType); err != nil {
			wireType = ""
		}
	}
	if event == "resync" {
		if _, ok := object["type"]; ok && wireType != "resync" {
			return StreamFrame{}, protocolErr(StreamInvalidResync)
		}
		return StreamFrame{Resync: true, Seq: id, HasSeq: hasID, Type: "resync", Data: json.RawMessage(append([]byte(nil), data...))}, nil
	}
	seq, hasSeq := id, hasID
	if raw, ok := object["seq"]; ok {
		if n, err := strconv.ParseInt(string(bytes.TrimSpace(raw)), 10, 64); err == nil {
			seq, hasSeq = n, true
		}
	}
	if !hasSeq {
		return StreamFrame{}, protocolErr(StreamUnusableSequence)
	}
	var publishedAt string
	if raw, ok := object["published_at"]; ok {
		if err := json.Unmarshal(raw, &publishedAt); err != nil {
			publishedAt = ""
		}
	}
	return StreamFrame{
		Seq: seq, HasSeq: true, Type: wireType, PublishedAt: publishedAt,
		Data: json.RawMessage(append([]byte(nil), data...)),
	}, nil
}
