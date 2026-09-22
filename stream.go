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
	"time"
)

// DefaultMaxStreamFrameBytes bounds the bytes held for one undelivered frame:
// 1 MiB. The largest real frame is a few KB.
const DefaultMaxStreamFrameBytes = 1 << 20

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
	editors       []RequestEditorFn
}

// WithMaxFrameBytes bounds the bytes held for one undelivered frame. The
// default is DefaultMaxStreamFrameBytes (1 MiB). A connection that sends more
// than this without a blank-line delimiter ends with StreamFrameTooLarge.
func WithMaxFrameBytes(n int) StreamOption {
	return func(c *streamConfig) { c.maxFrameBytes = n }
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
	cfg := streamConfig{maxFrameBytes: DefaultMaxStreamFrameBytes}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.maxFrameBytes < 1 {
		return nil, fmt.Errorf("oxinsider: MaxFrameBytes must be a positive integer, got %d", cfg.maxFrameBytes)
	}
	editors := append([]RequestEditorFn{acceptEventStream}, cfg.editors...)
	rsp, err := c.ClientInterface.GetStream(RawStreamContext(ctx), params, editors...)
	if err != nil {
		return nil, err
	}
	if rsp.StatusCode != http.StatusOK {
		limited := &http.Response{}
		*limited = *rsp
		limited.Body = struct {
			io.Reader
			io.Closer
		}{io.LimitReader(rsp.Body, maxStreamErrorBodyBytes), rsp.Body}
		parsed, err := ParseGetStreamResponse(limited)
		if err != nil {
			return nil, fmt.Errorf("oxinsider stream: HTTP %d, and the error body did not decode: %w", rsp.StatusCode, err)
		}
		return nil, &StreamHTTPError{StatusCode: rsp.StatusCode, Response: parsed}
	}
	mediaType, _, _ := mime.ParseMediaType(rsp.Header.Get("Content-Type"))
	if mediaType != "text/event-stream" {
		closeErr := rsp.Body.Close()
		protocolErr := &StreamProtocolError{Reason: StreamUnexpectedMediaType, MediaType: rsp.Header.Get("Content-Type")}
		if closeErr != nil {
			return nil, errors.Join(protocolErr, closeErr)
		}
		return nil, protocolErr
	}
	return &StreamReader{
		ctx:           ctx,
		resp:          rsp,
		reader:        bufio.NewReaderSize(rsp.Body, 64<<10),
		maxFrameBytes: cfg.maxFrameBytes,
	}, nil
}

func acceptEventStream(_ context.Context, req *http.Request) error {
	req.Header.Set("Accept", "text/event-stream")
	return nil
}

// StreamReader delivers the frames of one stream connection. It is not safe
// for concurrent use.
type StreamReader struct {
	ctx           context.Context
	resp          *http.Response
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

// Close closes the connection. Next returns io.EOF afterwards. Close is
// idempotent.
func (r *StreamReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.done == nil {
		r.done = io.EOF
	}
	return r.resp.Body.Close()
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
// the context given to OpenStream is cancelled; a *StreamProtocolError when
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

// streamGuardDoer wraps the client's HTTP doer for New: a request to GET
// /api/v1/stream outside OpenStream (or RawStreamContext) is refused with
// ErrStreamBuffered before it is sent, so the generated GetStreamWithResponse
// fails at once instead of buffering an unbounded body.
type streamGuardDoer struct {
	inner HttpRequestDoer
}

func (d streamGuardDoer) Do(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/api/v1/stream") && !streamAllowed(req.Context()) {
		return nil, ErrStreamBuffered
	}
	return d.inner.Do(req)
}
