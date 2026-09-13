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
const Version = "0.1.0"

// WithBearerToken authenticates every request with an API key
// (oxi_sk_live_...) or an OAuth 2.1 access token (oxi_at_...). Discovery,
// health and the platform capability document need no credential.
func WithBearerToken(token string) ClientOption {
	return WithRequestEditorFn(func(_ context.Context, req *http.Request) error {
		if strings.TrimSpace(token) == "" {
			return fmt.Errorf("oxinsider: empty bearer token")
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
func New(opts ...ClientOption) (*ClientWithResponses, error) {
	return NewClientWithResponses(DefaultServer, append([]ClientOption{withUserAgent()}, opts...)...)
}
