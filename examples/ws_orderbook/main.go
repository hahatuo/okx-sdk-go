package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	okx "github.com/hahatuo/okx-sdk-go"
)

type orderBookData struct {
	Asks [][]string `json:"asks"`
	Bids [][]string `json:"bids"`
	TS   string     `json:"ts"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	instID := envOr("OKX_INST_ID", "BTC-USDT")
	channel := envOr("OKX_WS_CHANNEL", "books5")

	ws := okx.NewWSClient()
	if err := ws.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := ws.Close(); err != nil {
			log.Printf("close websocket: %v", err)
		}
	}()

	ch, err := ws.Subscribe(ctx, okx.Subscription{
		Channel: channel,
		Args:    map[string]string{"instId": instID},
	})
	if err != nil {
		log.Fatal(err)
	}

	msg := waitForMessage(ctx, ch)
	var books []orderBookData
	if err := json.Unmarshal(msg.Data, &books); err != nil {
		log.Fatal(err)
	}
	if len(books) == 0 {
		log.Fatal("empty order book data")
	}

	book := books[0]
	fmt.Printf("channel=%s instId=%s asks=%d bids=%d ts=%s\n",
		msg.Arg["channel"], msg.Arg["instId"], len(book.Asks), len(book.Bids), book.TS)
	if len(book.Bids) > 0 {
		fmt.Printf("bestBid px=%s sz=%s\n", book.Bids[0][0], book.Bids[0][1])
	}
	if len(book.Asks) > 0 {
		fmt.Printf("bestAsk px=%s sz=%s\n", book.Asks[0][0], book.Asks[0][1])
	}
}

func waitForMessage(ctx context.Context, ch <-chan okx.WSMessage) okx.WSMessage {
	select {
	case msg, ok := <-ch:
		if !ok {
			log.Fatal("websocket message channel closed")
		}
		return msg
	case <-ctx.Done():
		log.Fatal(ctx.Err())
		return okx.WSMessage{}
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
