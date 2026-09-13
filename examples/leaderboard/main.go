// Command leaderboard prints the first ten graded wallets. Needs OXI_API_KEY.
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
