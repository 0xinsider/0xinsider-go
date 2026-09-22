package oxinsider

// Where a credential may go.
//
// The API key travels as a bearer header, so the destination decides who
// receives it. A credential is sent over https anywhere, and over http only
// to a loopback host (a backend on localhost, 127.0.0.1 or [::1]). Anything
// else is refused with *InsecureTransportError before the request is sent,
// so a mistyped base URL or a plain-http proxy never sees the key. The check
// runs in WithBearerToken on the final request URL, again at the HTTP doer a
// client from New installs (so an editor that rewrote the URL or set the
// header itself cannot bypass it), and on every redirect the default
// http.Client follows (Go copies Authorization to a same-domain redirect
// whatever its scheme, so an https-to-http downgrade is refused there).
//
// Owned outside the generated file so regeneration cannot loosen it.

import (
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
)

// InsecureTransportError reports that a credential would have been sent over
// plain HTTP to a host that is not loopback. Origin is the refused
// destination (scheme, host and port); the credential is never part of it.
type InsecureTransportError struct {
	Origin string
	// Redirect is true when the destination was a redirect target rather
	// than the request's own URL.
	Redirect bool
}

func (e *InsecureTransportError) Error() string {
	what := "the request"
	if e.Redirect {
		what = "a redirect"
	}
	return "oxinsider: refusing to send the bearer credential with " + what + " to " + e.Origin +
		": use https://, or http:// only on a loopback host (localhost, 127.0.0.1, [::1])"
}

// IsLoopbackHost reports whether host is localhost, an address in
// 127.0.0.0/8, or ::1 (bracketed or not).
func IsLoopbackHost(host string) bool {
	name := strings.ToLower(strings.Trim(host, "[]"))
	if name == "localhost" {
		return true
	}
	ip := net.ParseIP(name)
	return ip != nil && ip.IsLoopback()
}

// IsTrustedDestination reports whether u may receive a credential: https
// anywhere, or http to a loopback host only.
func IsTrustedDestination(u *url.URL) bool {
	if u == nil {
		return false
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
		return true
	case "http":
		return IsLoopbackHost(u.Hostname())
	default:
		return false
	}
}

func originOf(u *url.URL) string {
	return strings.ToLower(u.Scheme) + "://" + u.Host
}

// assertCredentialDestination returns *InsecureTransportError unless u may
// receive a credential.
func assertCredentialDestination(u *url.URL, redirect bool) error {
	if IsTrustedDestination(u) {
		return nil
	}
	return &InsecureTransportError{Origin: originOf(u), Redirect: redirect}
}

// policyDoer wraps the client's HTTP doer for New. It refuses a request that
// carries Authorization to an insecure destination, whichever editor set the
// header, and refuses GET /api/v1/stream outside OpenStream (or
// RawStreamContext) with ErrStreamBuffered, so the generated
// GetStreamWithResponse fails at once instead of buffering an unbounded body.
type policyDoer struct {
	inner HttpRequestDoer
}

func (d policyDoer) Do(req *http.Request) (*http.Response, error) {
	if req.Header.Get("Authorization") != "" {
		if err := assertCredentialDestination(req.URL, false); err != nil {
			return nil, err
		}
	}
	if isStreamRequest(req) && !streamAllowed(req.Context()) {
		return nil, ErrStreamBuffered
	}
	return d.inner.Do(req)
}

// refuseCredentialedDowngrade is the CheckRedirect installed on the
// http.Client a client from New uses. net/http copies Authorization onto a
// redirect to the same domain whatever the scheme; this refuses the hop when
// the header is present and the destination is not trusted, and hands every
// other hop to the client's own CheckRedirect (or the default ten-hop rule).
func refuseCredentialedDowngrade(next func(*http.Request, []*http.Request) error) func(*http.Request, []*http.Request) error {
	return func(req *http.Request, via []*http.Request) error {
		if req.Header.Get("Authorization") != "" {
			if err := assertCredentialDestination(req.URL, true); err != nil {
				return err
			}
		}
		if next != nil {
			return next(req, via)
		}
		if len(via) >= 10 {
			return errors.New("stopped after 10 redirects")
		}
		return nil
	}
}

// withRedirectPolicy returns the doer New should use: an *http.Client is
// copied (its Transport and Jar are shared) with refuseCredentialedDowngrade
// chained before its own CheckRedirect; any other HttpRequestDoer owns its
// redirects and is returned as is.
func withRedirectPolicy(doer HttpRequestDoer) HttpRequestDoer {
	httpClient, ok := doer.(*http.Client)
	if !ok {
		return doer
	}
	guarded := *httpClient
	guarded.CheckRedirect = refuseCredentialedDowngrade(httpClient.CheckRedirect)
	return &guarded
}
