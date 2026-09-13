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
- **Errors.** A 401 carries `WWW-Authenticate` with the protected-resource metadata URL; a 403 `insufficient_scope` names the OAuth scope a token lacks; a 429 carries `Retry-After`. Read the JSON error object's `error.code` and branch on it; `error.message` is prose.
- **Numbers.** Money and price fields keep the API's full precision. Do not round before you display them.
- **Missing values.** A missing, stale, partial or unavailable field means the provider did not report that value. Do not read it as zero.

Run the example, which needs no key for discovery and health:

```sh
go run ./examples/discovery
OXI_API_KEY=oxi_sk_live_... go run ./examples/discovery
```

## Regenerate

`./scripts/generate.sh` fetches the published OpenAPI document, drops the bracketed `expand[]` query aliases (the SDK always sends `expand`), regenerates `oxinsider.gen.go` with oapi-codegen, and vets and builds the module. A weekly workflow opens a pull request when the published document changes.

## License

MIT
