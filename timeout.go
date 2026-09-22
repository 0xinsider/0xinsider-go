package oxinsider

// How long a request may take.
//
// net/http has no default timeout: a zero http.Client.Timeout and a
// context.Background() mean a stalled connection waits forever
// (https://pkg.go.dev/net/http#Client). The documented quickstart uses
// context.Background(), so a client from New bounds every ordinary request
// itself:
//
//   - the transport bounds the connection: DefaultConnectTimeout to dial,
//     DefaultTLSHandshakeTimeout for the TLS handshake, and
//     DefaultResponseHeaderTimeout from the end of the request to the first
//     response header;
//   - the doer bounds the whole call, headers and body together:
//     DefaultRequestTimeout for an ordinary operation, DefaultDownloadTimeout
//     for the export download, whose body is a file this SDK does not size.
//
// A context that already carries a deadline is never shortened or extended:
// the caller's deadline is the whole bound for that call, so a shorter one
// wins and a longer one is honored. WithRequestTimeout changes the client's
// default, and 0 removes it.
//
// GET /api/v1/stream is excluded from the total bound: it is an unbounded
// event stream, and a total deadline would cut a healthy connection. It is
// bounded where a stream can stall instead, by OpenStream's start and idle
// timeouts (stream.go).
//
// http.Client.Timeout is deliberately not set. It bounds reading the response
// body too, so it would end the event stream on the ordinary-request deadline
// however the stream is read; the per-request context deadline here applies to
// the requests that should have one and to no others.
//
// Owned outside the generated file so regeneration cannot loosen it.

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Default bounds a client from New applies. Every one of them is a stall
// bound, not a service-level expectation: a documented operation answers in
// well under a second.
const (
	// DefaultRequestTimeout bounds an ordinary request end to end: connect,
	// request, response headers and response body, across any redirect.
	DefaultRequestTimeout = 30 * time.Second
	// DefaultDownloadTimeout bounds GET /api/v1/trader/{address}/export/download,
	// which redirects to a presigned file whose size this SDK does not know.
	DefaultDownloadTimeout = 5 * time.Minute
	// DefaultConnectTimeout bounds establishing the TCP connection.
	DefaultConnectTimeout = 10 * time.Second
	// DefaultTLSHandshakeTimeout bounds the TLS handshake.
	DefaultTLSHandshakeTimeout = 10 * time.Second
	// DefaultResponseHeaderTimeout bounds the wait from the end of the request
	// to the first response header. It applies to every request, the event
	// stream included: headers arrive before the first frame.
	DefaultResponseHeaderTimeout = 15 * time.Second
)

// RequestTimeoutError reports that a request passed the client's deadline for
// it. Deadline is the bound that expired, Method and URL name the request
// (scheme, host and path; no query, no credential), and Err is the underlying
// error, so errors.Is(err, context.DeadlineExceeded) holds and errors.As finds
// the *url.Error. A context the caller cancelled or gave its own deadline is
// reported as it is, never as this error, so a client deadline stays
// distinguishable from a caller's cancellation and from an HTTP status.
type RequestTimeoutError struct {
	Deadline time.Duration
	Method   string
	URL      string
	Err      error
}

func (e *RequestTimeoutError) Error() string {
	return fmt.Sprintf(
		"oxinsider: %s %s did not finish within the %s client deadline; give the context your own deadline, or pass oxinsider.WithRequestTimeout, to change it",
		e.Method, e.URL, e.Deadline,
	)
}

// Unwrap exposes the transport error, so errors.Is(err, context.DeadlineExceeded) holds.
func (e *RequestTimeoutError) Unwrap() error { return e.Err }

// Timeout reports that this error is a timeout, matching the net.Error convention.
func (e *RequestTimeoutError) Timeout() bool { return true }

// requestTimeoutKey carries a WithRequestTimeout override on the request
// context. It never reaches the wire.
type requestTimeoutKey struct{}

// WithRequestTimeout sets the total deadline a client from New applies to
// every request that is not the event stream, replacing both
// DefaultRequestTimeout and DefaultDownloadTimeout. Zero removes the bound and
// leaves the connection-level bounds (dial, TLS handshake, response header) in
// place; a negative duration is an error.
//
// A context that already carries a deadline still wins: this is the default
// for calls that bring none.
func WithRequestTimeout(timeout time.Duration) ClientOption {
	return func(c *Client) error {
		if timeout < 0 {
			return fmt.Errorf("oxinsider: request timeout must not be negative, got %s", timeout)
		}
		c.RequestEditors = append(c.RequestEditors, func(_ context.Context, req *http.Request) error {
			// The generated operations call req.WithContext before the
			// editors and hand the same *http.Request to the doer, so
			// replacing the request's own context here is what reaches Do.
			*req = *req.WithContext(context.WithValue(req.Context(), requestTimeoutKey{}, timeout))
			return nil
		})
		return nil
	}
}

// newDefaultTransport returns the transport a client from New uses: the
// standard library's defaults with the three connection bounds net/http leaves
// open (a shorter dial timeout, an explicit TLS handshake bound, and a
// response-header bound net/http has none of).
func newDefaultTransport() *http.Transport {
	dialer := &net.Dialer{Timeout: DefaultConnectTimeout, KeepAlive: 30 * time.Second}
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		transport := base.Clone()
		transport.DialContext = dialer.DialContext
		transport.TLSHandshakeTimeout = DefaultTLSHandshakeTimeout
		transport.ResponseHeaderTimeout = DefaultResponseHeaderTimeout
		return transport
	}
	// http.DefaultTransport was replaced by the program with something else.
	// Build the same shape rather than inherit an unknown one.
	return &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   DefaultTLSHandshakeTimeout,
		ExpectContinueTimeout: time.Second,
		ResponseHeaderTimeout: DefaultResponseHeaderTimeout,
	}
}

// newDefaultHTTPClient returns the HTTP client New installs unless the caller
// passes WithHTTPClient. Timeout stays zero on purpose: see the file comment.
func newDefaultHTTPClient() *http.Client {
	return &http.Client{Transport: newDefaultTransport()}
}

// deadlineDoer gives a request the client's deadline when the caller's context
// carries none. It wraps whichever doer the client ended up with, including a
// caller's own from WithHTTPClient, so the bound does not depend on who owns
// the transport.
type deadlineDoer struct {
	inner    HttpRequestDoer
	request  time.Duration
	download time.Duration
}

// timeoutFor returns the deadline this request gets, or 0 for none.
func (d deadlineDoer) timeoutFor(req *http.Request) time.Duration {
	if isStreamRequest(req) {
		// OpenStream owns the stream's bounds; a total deadline would cut a
		// healthy connection.
		return 0
	}
	if override, ok := req.Context().Value(requestTimeoutKey{}).(time.Duration); ok {
		return override
	}
	if isExportDownloadRequest(req) {
		return d.download
	}
	return d.request
}

func (d deadlineDoer) Do(req *http.Request) (*http.Response, error) {
	timeout := d.timeoutFor(req)
	parent := req.Context()
	if _, caller := parent.Deadline(); timeout <= 0 || caller {
		return d.inner.Do(req)
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	wrap := func(err error) error { return asRequestTimeout(err, req, parent, ctx, timeout) }
	rsp, err := d.inner.Do(req.WithContext(ctx))
	if err != nil {
		// Classify before cancel, so cancelling here cannot look like the
		// deadline expiring.
		err = wrap(err)
		cancel()
		return nil, err
	}
	// The deadline covers the body too, so it is released when the body is
	// closed, which every generated Parse function does.
	rsp.Body = &deadlineBody{ReadCloser: rsp.Body, cancel: cancel, wrap: wrap}
	return rsp, nil
}

// asRequestTimeout names the client's own deadline as the cause, and only when
// that deadline is what expired: own.Err() is the deadline this doer set, and
// parent.Err() being set means the caller cancelled or ran out of time first.
// Every other failure is returned exactly as it is, the caller's cancellation
// and the transport's own connect, handshake and response-header timeouts
// included, so a bound the SDK applied never stands in for one it did not.
func asRequestTimeout(err error, req *http.Request, parent, own context.Context, timeout time.Duration) error {
	if err == nil || parent.Err() != nil || !errors.Is(own.Err(), context.DeadlineExceeded) {
		return err
	}
	return &RequestTimeoutError{
		Deadline: timeout,
		Method:   req.Method,
		URL:      requestTarget(req.URL),
		Err:      err,
	}
}

// requestTarget renders a URL for an error message: scheme, host and path, and
// never the query, which carries the caller's filters.
func requestTarget(u *url.URL) string {
	if u == nil {
		return ""
	}
	return strings.ToLower(u.Scheme) + "://" + u.Host + u.Path
}

// deadlineBody releases the request's deadline when the body is closed, and
// names the client's deadline when the body read is what it cut short.
type deadlineBody struct {
	io.ReadCloser
	cancel context.CancelFunc
	wrap   func(error) error
	once   sync.Once
}

func (b *deadlineBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		err = b.wrap(err)
	}
	return n, err
}

func (b *deadlineBody) Close() error {
	defer b.once.Do(func() { b.cancel() })
	return b.ReadCloser.Close()
}

// isExportDownloadRequest reports whether req is
// GET /api/v1/trader/{address}/export/download, which redirects to a presigned
// file: a body this SDK does not size, so it gets the longer default.
func isExportDownloadRequest(req *http.Request) bool {
	return req.Method == http.MethodGet && strings.HasSuffix(req.URL.Path, "/export/download")
}
