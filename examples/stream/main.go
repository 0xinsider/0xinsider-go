// Command stream follows the live event stream and prints each frame with
// its sequence, resuming from the last delivered sequence after a clean
// close. Needs OXI_API_KEY. Stop it with Ctrl-C.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"

	oxinsider "github.com/0xinsider/0xinsider-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	client, err := oxinsider.New(oxinsider.WithBearerToken(os.Getenv("OXI_API_KEY")))
	if err != nil {
		log.Fatal(err)
	}

	var cursor *string
	for ctx.Err() == nil {
		reader, err := client.OpenStream(ctx, &oxinsider.GetStreamParams{LastEventID: cursor})
		if err != nil {
			log.Fatal(err) // *oxinsider.StreamHTTPError carries the decoded error body and Retry-After
		}
		for {
			frame, err := reader.Next()
			if errors.Is(err, io.EOF) {
				break // the server closed the stream; resume from the last seq
			}
			if err != nil {
				if closeErr := reader.Close(); closeErr != nil {
					log.Printf("close: %v", closeErr)
				}
				if ctx.Err() != nil {
					return
				}
				log.Fatal(err) // a transport error or *oxinsider.StreamProtocolError: decide where to resume
			}
			if frame.Resync {
				fmt.Printf("resync %s: refetch current state\n", frame.Data)
				continue
			}
			seq := strconv.FormatInt(frame.Seq, 10)
			cursor = &seq
			fmt.Printf("%s %s %s\n", seq, frame.Type, frame.Data)
		}
		if err := reader.Close(); err != nil {
			log.Printf("close: %v", err)
		}
	}
}
