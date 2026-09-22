package oxinsider

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

// DefaultServer is the production API origin. Every operation path in this
// package is relative to it (for example /api/v1/leaderboard).
const DefaultServer = "https://api.0xinsider.com"

// Version is this SDK's release, sent in the User-Agent header.
const Version = "0.2.0"

// WithBearerToken authenticates every request with an API key
// (oxi_sk_live_...) or an OAuth 2.1 access token (oxi_at_...). Discovery,
// health and the platform capability document need no credential.
//
// The credential is sent over https only, or over http to a loopback host
// (localhost, 127.0.0.1, [::1]) for a backend you run yourself. A request
// that would carry it anywhere else fails with *InsecureTransportError
// before it is sent; the error names the destination, never the token.
func WithBearerToken(token string) ClientOption {
	return WithRequestEditorFn(func(_ context.Context, req *http.Request) error {
		if strings.TrimSpace(token) == "" {
			return fmt.Errorf("oxinsider: empty bearer token")
		}
		if err := assertCredentialDestination(req.URL, false); err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		return nil
	})
}

// withUserAgent identifies the SDK to the API.
func withUserAgent() ClientOption {
	return WithRequestEditorFn(func(_ context.Context, req *http.Request) error {
		if req.Header.Get("User-Agent") == "" {
			req.Header.Set("User-Agent", "0xinsider-go/"+Version)
		}
		return nil
	})
}

// New returns a typed client for the production API. Pass WithBearerToken for
// authenticated operations; pass WithBaseURL to target another server.
//
// Every ordinary request is finitely bounded: DefaultRequestTimeout end to
// end (DefaultDownloadTimeout for the export download), over a transport that
// bounds the dial, the TLS handshake and the wait for response headers. A
// context deadline the caller brings wins, WithRequestTimeout changes the
// default, and a deadline this client applied is reported as
// *RequestTimeoutError. GET /api/v1/stream is bounded by OpenStream's start
// and idle timeouts instead, so a healthy stream is never cut by the
// ordinary-request deadline (timeout.go).
//
// The client refuses GET /api/v1/stream outside OpenStream with
// ErrStreamBuffered: the generated GetStreamWithResponse reads the unbounded
// event stream to EOF, so it would neither return nor bound its memory. Read
// the stream with OpenStream, or take the raw body through GetStream with
// RawStreamContext. It also refuses to send a bearer credential over plain
// HTTP to a host that is not loopback, on the request and on any redirect
// the default http.Client follows (*InsecureTransportError). The generated
// NewClientWithResponses carries none of this; New is the supported
// constructor.
//
// WithHTTPClient still replaces the HTTP client, and its owner then owns the
// transport bounds; the per-request deadline is applied around it either way.
func New(opts ...ClientOption) (*ClientWithResponses, error) {
	defaults := []ClientOption{WithHTTPClient(newDefaultHTTPClient()), withUserAgent()}
	client, err := NewClientWithResponses(DefaultServer, append(defaults, opts...)...)
	if err != nil {
		return nil, err
	}
	inner, ok := client.ClientInterface.(*Client)
	if !ok {
		return nil, fmt.Errorf("oxinsider: generated client is %T, not *Client", client.ClientInterface)
	}
	inner.Client = deadlineDoer{
		inner:    policyDoer{inner: withRedirectPolicy(inner.Client)},
		request:  DefaultRequestTimeout,
		download: DefaultDownloadTimeout,
	}
	return client, nil
}
