# 0xinsider Go SDK

The official Go client for the [0xinsider Developer API](https://0xinsider.com/developers): Polymarket analytics for sports and esports. Every tracked wallet is graded S to F from settled P&L; the API returns grades, large trades, sharp-money flow, positions, market snapshots, reports and search.

- Website and API keys: https://0xinsider.com/developers
- Authentication (API keys and OAuth 2.1): https://0xinsider.com/auth.md
- OpenAPI 3.1: https://0xinsider.com/api/v1/openapi.json
- Documentation: https://docs.0xinsider.com
- MCP server: https://api.0xinsider.com/api/v1/mcp

## Install

```sh
go get github.com/0xinsider/0xinsider-go
```

## Use

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	oxinsider "github.com/0xinsider/0xinsider-go"
)

func main() {
	client, err := oxinsider.New(oxinsider.WithBearerToken(os.Getenv("OXI_API_KEY")))
	if err != nil {
		log.Fatal(err)
	}
	limit := 10
	resp, err := client.ListLeaderboardWithResponse(context.Background(), &oxinsider.ListLeaderboardParams{Limit: &limit})
	if err != nil {
		log.Fatal(err)
	}
	if resp.JSON200 == nil {
		log.Fatalf("HTTP %d: %s", resp.StatusCode(), resp.Body)
	}
	for _, trader := range resp.JSON200.Data {
		fmt.Println(trader.Address)
	}
}
```

Every operation in the OpenAPI document has a typed `...WithResponse` method. `JSON200` (and the other status fields) hold the decoded body; `Body` keeps the raw bytes and `StatusCode()` the status.

- **Credentials.** `WithBearerToken` accepts an API key (`oxi_sk_live_...`) or an OAuth 2.1 access token (`oxi_at_...`). Discovery (`GetApiDiscovery`), health and the platform capability document need none. Data operations need an active Pro subscription.
- **Where the credential goes.** It is sent over `https://` only, or over `http://` to a loopback host (`localhost`, `127.0.0.1`, `[::1]`) for a backend you run yourself. A `WithBaseURL` that would send it anywhere else fails every credentialed request with `*InsecureTransportError` before it is sent, and a redirect that would downgrade a credentialed request to plain HTTP is refused the same way; the error names the destination, never the token. A client without `WithBearerToken` may still call the public operations on such a base.
- **Errors.** A 401 carries `WWW-Authenticate` with the protected-resource metadata URL; a 403 `insufficient_scope` names the OAuth scope a token lacks; a 429 carries `Retry-After`. Read the JSON error object's `error.code` and branch on it; `error.message` is prose.
- **Numbers.** Money and price fields keep the API's full precision. Do not round before you display them.
- **Missing values.** A missing, stale, partial or unavailable field means the provider did not report that value. Do not read it as zero.

## Stream

`GET /api/v1/stream` is an unbounded Server-Sent Events stream. Read it with `OpenStream`, which delivers each frame as it arrives, holds at most `WithMaxFrameBytes` (1 MiB by default) for one undelivered frame, and closes the connection when the context is cancelled or the reader is closed:

```go
cursor := "0" // the last seq you processed; omit LastEventID to attach live
reader, err := client.OpenStream(ctx, &oxinsider.GetStreamParams{LastEventID: &cursor})
if err != nil {
	log.Fatal(err) // *oxinsider.StreamHTTPError (401, 429 with Retry-After, ...) or *oxinsider.StreamProtocolError
}
defer reader.Close()
for {
	frame, err := reader.Next()
	if errors.Is(err, io.EOF) {
		break // the server closed the stream; reconnect with the last seq
	}
	if err != nil {
		log.Fatal(err) // ctx.Err(), a transport error, or *oxinsider.StreamProtocolError
	}
	if frame.Resync {
		// The resume point is outside the retained window: refetch state, then continue live.
		continue
	}
	cursor = strconv.FormatInt(frame.Seq, 10)
	fmt.Println(frame.Type, string(frame.Data))
}
```

A frame that breaks the SSE contract (not JSON, not an object, no usable sequence, a malformed `resync` marker, a `200` that is not `text/event-stream`, or a frame past the byte ceiling) ends the stream with a `*StreamProtocolError` whose `Reason`, `LastSeq` and `FrameID` say which frame and where to resume from; a malformed frame never moves `LastSeq`. Keep-alive comments are consumed silently, `retry:` is exposed as `RetryHint`, and the stream's headers are on `Response()`.

Do not read the stream with the generated `GetStreamWithResponse`: it reads the body to EOF, which a healthy stream never reaches, so it would neither return nor bound its memory. A client from `New` refuses that call with `ErrStreamBuffered`. To own the raw `*http.Response` instead, call `GetStream` with `RawStreamContext(ctx)`.

Run the example, which needs no key for discovery and health:

```sh
go run ./examples/discovery
OXI_API_KEY=oxi_sk_live_... go run ./examples/discovery
OXI_API_KEY=oxi_sk_live_... go run ./examples/stream
```

## Regenerate

`./scripts/generate.sh` fetches the published OpenAPI document, drops the bracketed `expand[]` query aliases (the SDK always sends `expand`), regenerates `oxinsider.gen.go` with oapi-codegen, and vets and builds the module. A weekly workflow opens a pull request when the published document changes.

## License

MIT
