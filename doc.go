// Package oxinsider is the official Go client for the 0xinsider Developer API:
// Polymarket analytics for sports and esports, with every tracked wallet graded
// S to F from settled P&L, large trades, sharp-money flow, positions, market
// snapshots and reports.
//
// The client is generated from the published OpenAPI 3.1 document at
// https://0xinsider.com/api/v1/openapi.json. Authenticate with an API key from
// https://0xinsider.com/developers or an OAuth 2.1 access token; see
// https://0xinsider.com/auth.md.
//
//	client, err := oxinsider.New(oxinsider.WithBearerToken(os.Getenv("OXI_API_KEY")))
//	board, err := client.ListLeaderboardWithResponse(ctx, &oxinsider.ListLeaderboardParams{})
//
// Read the live event stream (GET /api/v1/stream) with OpenStream, which
// delivers frames as they arrive and bounds its memory; the generated
// GetStreamWithResponse reads the unbounded body to EOF and is refused by a
// client from New.
//
//	reader, err := client.OpenStream(ctx, nil)
//	frame, err := reader.Next()
package oxinsider
