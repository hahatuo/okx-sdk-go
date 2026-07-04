# okx-sdk-go

A small Go SDK for the OKX API v5 REST and WebSocket APIs.

This package is intentionally focused: it covers common account, trade, market, public data, asset, and WebSocket flows while keeping the request and response types close to the official OKX API v5 documentation.

Official API documentation: https://www.okx.com/docs-v5/en/

## Install

```bash
go get github.com/hahatuo/okx-sdk-go
```

## REST Usage

Public market data:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	okx "github.com/hahatuo/okx-sdk-go"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := okx.NewRestClient()

	tickers, err := client.Market.Ticker(ctx, okx.TickerRequest{InstID: "BTC-USDT"})
	if err != nil {
		log.Fatal(err)
	}
	if len(tickers) == 0 {
		log.Fatal("empty ticker response")
	}

	fmt.Println(tickers[0].InstID, tickers[0].Last)
}
```

Private account request:

```go
client := okx.NewRestClient(
	okx.WithCredentials(apiKey, secretKey, passphrase),
	okx.WithTimeout(10*time.Second),
)

balances, err := client.Account.Balance(ctx, okx.BalanceRequest{Ccy: "BTC"})
```

Demo trading:

```go
client := okx.NewRestClient(
	okx.WithCredentials(apiKey, secretKey, passphrase),
	okx.WithDemoTrading(true),
)
```

## WebSocket Usage

Public order book subscription:

```go
ws := okx.NewWSClient()
if err := ws.Connect(ctx); err != nil {
	log.Fatal(err)
}
defer ws.Close()

ch, err := ws.SubscribeOrderBook(ctx, "books5", "BTC-USDT")
if err != nil {
	log.Fatal(err)
}

msg := <-ch
if msg.Err != nil {
	log.Fatal(msg.Err)
}
fmt.Println(msg.Arg["channel"], len(msg.Data[0].Bids), len(msg.Data[0].Asks))
```

Private WebSocket subscriptions use the private URL and login:

```go
ws := okx.NewWSClient(
	okx.WithWSURL(okx.WSPrivateURL),
	okx.WithWSCredentials(apiKey, secretKey, passphrase),
)

if err := ws.Connect(ctx); err != nil {
	log.Fatal(err)
}
if err := ws.Login(ctx); err != nil {
	log.Fatal(err)
}

orders, err := ws.SubscribeOrders(ctx, okx.OrdersSubscriptionRequest{InstType: "ANY"})
```

## API Domains

The default REST base URL is `https://openapi.okx.com`, matching the current OKX API v5 documentation.

Regional accounts must use the correct API domain:

- US and AU accounts registered on `app.okx.com`: use `https://us.okx.com`
- EU accounts registered on `my.okx.com`: use `https://eea.okx.com`

Override the REST base URL with:

```go
client := okx.NewRestClient(okx.WithBaseURL("https://us.okx.com"))
```

Available WebSocket URL constants include:

- `okx.WSPublicURL`
- `okx.WSPrivateURL`
- `okx.WSBusinessURL`
- `okx.WSDemoPublicURL`
- `okx.WSDemoPrivateURL`
- `okx.WSDemoBusinessURL`

## Covered APIs

REST:

- Account: balance, positions, max available size, set leverage
- Trade: place, batch place, cancel, batch cancel, amend, batch amend, get order, pending orders
- Market: tickers, ticker, order book, candles, historical candles, index components, platform 24h volume
- Public data: instruments, system time, underlying, funding rate
- Asset: currencies, funding account balances

WebSocket:

- Connect, login, subscribe, unsubscribe
- Typed helpers for tickers, order books, and orders

## Errors

API errors are returned as `*okx.OKXError` and can also wrap package sentinel errors such as `okx.ErrRateLimited`, `okx.ErrUnauthorized`, and `okx.ErrInvalidParameter`.

```go
if errors.Is(err, okx.ErrRateLimited) {
	// retry later
}

var okxErr *okx.OKXError
if errors.As(err, &okxErr) {
	fmt.Println(okxErr.Code, okxErr.Message)
}
```

## Tests

```bash
go test ./...
go vet ./...
```

Read-only account integration tests run when `OKX_API_KEY`, `OKX_SECRET_KEY`, and `OKX_PASSPHRASE` are set.

Public WebSocket integration tests run when `OKX_WS_INTEGRATION=1`.
By default they validate `bbo-tbt` and `books5`. Set
`OKX_WS_DEPTH_CHANNELS=bbo-tbt,books5,books50-l2-tbt,books-l2-tbt` to include
the deeper order book channels as well.
