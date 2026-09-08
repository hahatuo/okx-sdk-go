package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"time"

	okx "github.com/hahatuo/okx-sdk-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	client := okx.NewWSClient(okx.Public)
	if err := client.ConnectWithTimeout(ctx, 10*time.Second); err != nil {
		log.Print(err)
		return
	}
	defer client.Close()
	// The callback runs on the read loop. Only inspect borrowed levels here;
	// copying or publishing to another goroutine is the application's responsibility.
	_, err := client.SubscribeLocalOrderBookHandle(ctx, okx.WSChannelBooks, "BTC-USDT", 5, func(msg okx.WSLocalOrderBookMessage) {
		if msg.Err != nil {
			return
		} // The client reconnects and waits for a fresh snapshot.
		if msg.Book.Ready() && len(msg.Book.Bids()) > 0 {
			_ = msg.Book.Bids()[0].Price() // Run a short, nonblocking strategy step here.
		}
	})
	if err != nil {
		log.Print(err)
		return
	}
	<-ctx.Done()
}
