package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	okx "github.com/hahatuo/okx-sdk-go"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := okx.NewRestClient()
	instID := envOr("OKX_INST_ID", "BTC-USDT")
	bar := envOr("OKX_BAR", "1m")
	limit := envOr("OKX_LIMIT", "3")

	tickers, err := client.Market.Ticker(ctx, okx.TickerRequest{InstID: instID})
	if err != nil {
		log.Fatal(err)
	}
	if len(tickers) == 0 {
		log.Fatalf("empty ticker response for %s", instID)
	}
	ticker := tickers[0]
	fmt.Printf("%s last=%s bid=%s ask=%s ts=%s\n", ticker.InstID, ticker.Last, ticker.BidPx, ticker.AskPx, ticker.TS)

	candles, err := client.Market.Candles(ctx, okx.CandlesRequest{
		InstID: instID,
		Bar:    bar,
		Limit:  limit,
	})
	if err != nil {
		log.Fatal(err)
	}
	for i, candle := range candles {
		fmt.Printf("candle[%d] ts=%s open=%s high=%s low=%s close=%s confirm=%s\n",
			i, candle.TS, candle.O, candle.H, candle.L, candle.C, candle.Confirm)
	}
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
