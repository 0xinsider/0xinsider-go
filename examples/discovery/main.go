// Command discovery prints the live 0xinsider API index and health, which
// need no credential. Set OXI_API_KEY to also read the leaderboard.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	oxinsider "github.com/0xinsider/0xinsider-go"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	opts := []oxinsider.ClientOption{}
	key := os.Getenv("OXI_API_KEY")
	if key != "" {
		opts = append(opts, oxinsider.WithBearerToken(key))
	}
	client, err := oxinsider.New(opts...)
	if err != nil {
		log.Fatal(err)
	}

	discovery, err := client.GetApiDiscoveryWithResponse(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if discovery.JSON200 == nil {
		log.Fatalf("discovery: HTTP %d", discovery.StatusCode())
	}
	fmt.Printf("API base: %s\nOpenAPI: %s\nAuthenticated routes: %d\n",
		discovery.JSON200.Data.ApiBaseUrl,
		discovery.JSON200.Data.OpenapiUrl,
		len(discovery.JSON200.Data.AuthenticatedRoutes))

	health, err := client.GetHealthWithResponse(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Health: HTTP %d\n", health.StatusCode())

	if key == "" {
		return
	}
	limit := 5
	board, err := client.ListLeaderboardWithResponse(ctx, &oxinsider.ListLeaderboardParams{Limit: &limit})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Leaderboard: HTTP %d\n", board.StatusCode())
}
