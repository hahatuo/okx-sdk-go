package okx

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

type wsDepthChannelCase struct {
	name      string
	channel   string
	maxLevels int
}

var wsDepthChannelCatalog = map[string]wsDepthChannelCase{
	"bbo-tbt":        {name: "best bid offer", channel: "bbo-tbt", maxLevels: 1},
	"books5":         {name: "five levels", channel: "books5", maxLevels: 5},
	"books50-l2-tbt": {name: "fifty levels", channel: "books50-l2-tbt", maxLevels: 50},
	"books-l2-tbt":   {name: "four hundred levels", channel: "books-l2-tbt", maxLevels: 400},
}

func TestIntegrationReadOnlyAccountBalance(t *testing.T) {
	if strings.TrimSpace(os.Getenv("OKX_REST_INTEGRATION")) != "1" {
		t.Skip("set OKX_REST_INTEGRATION=1 to run OKX REST integration tests")
	}

	apiKey, secretKey, passphrase := okxCredentialsFromEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewRestClient(
		WithCredentials(apiKey, secretKey, passphrase),
		WithTimeout(10*time.Second),
	)

	if baseURL := strings.TrimSpace(os.Getenv("OKX_REST_URL")); baseURL != "" {
		WithBaseURL(baseURL)(client.Client)
	}

	if _, err := client.Account.Balance(ctx, BalanceRequest{}); err != nil {
		t.Fatalf("read-only account balance request failed: %v", err)
	}
}

func TestIntegrationDemoTradingPlaceAndCancelOrder(t *testing.T) {
	if strings.TrimSpace(os.Getenv("OKX_DEMO_TRADING_INTEGRATION")) != "1" {
		t.Skip("set OKX_DEMO_TRADING_INTEGRATION=1 to run OKX demo trading integration tests")
	}

	apiKey, secretKey, passphrase := okxDemoCredentialsFromEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := NewRestClient(
		WithCredentials(apiKey, secretKey, passphrase),
		WithDemoTrading(true),
		WithTimeout(10*time.Second),
	)
	if baseURL := strings.TrimSpace(os.Getenv("OKX_DEMO_REST_URL")); baseURL != "" {
		WithBaseURL(baseURL)(client.Client)
	}

	instID := envOr("OKX_DEMO_INST_ID", "BTC-USDT")
	tdMode := envOr("OKX_DEMO_TD_MODE", "cash")
	side := envOr("OKX_DEMO_SIDE", "buy")
	size := envOr("OKX_DEMO_SZ", "0.0001")
	price := strings.TrimSpace(os.Getenv("OKX_DEMO_PX"))
	if price == "" {
		price = demoLimitPrice(ctx, t, client, instID, side)
	}
	clOrdID := "okxsdkgo" + strconv.FormatInt(time.Now().UnixNano(), 10)

	acks, err := client.Trade.PlaceOrder(ctx, PlaceOrderRequest{
		InstID:  instID,
		TdMode:  tdMode,
		Side:    side,
		OrdType: "limit",
		Sz:      size,
		Px:      price,
		ClOrdID: clOrdID,
	})
	if err != nil {
		t.Fatalf("place demo order: %v", err)
	}
	if len(acks) == 0 {
		t.Fatal("place demo order returned no ack rows")
	}
	if acks[0].SCode != "" && acks[0].SCode != "0" {
		t.Fatalf("place demo order rejected: %+v", acks[0])
	}

	cancelReq := CancelOrderRequest{InstID: instID, OrdID: acks[0].OrdID}
	if cancelReq.OrdID == "" {
		cancelReq.ClOrdID = clOrdID
	}
	cancelAcks, err := client.Trade.CancelOrder(ctx, cancelReq)
	if err != nil {
		t.Fatalf("cancel demo order: %v", err)
	}
	if len(cancelAcks) == 0 {
		t.Fatal("cancel demo order returned no ack rows")
	}
	if cancelAcks[0].SCode != "" && cancelAcks[0].SCode != "0" {
		t.Fatalf("cancel demo order rejected: %+v", cancelAcks[0])
	}
}

func TestIntegrationDemoTradingPrivateWebSocketLogin(t *testing.T) {
	if strings.TrimSpace(os.Getenv("OKX_DEMO_TRADING_INTEGRATION")) != "1" {
		t.Skip("set OKX_DEMO_TRADING_INTEGRATION=1 to run OKX demo trading integration tests")
	}

	apiKey, secretKey, passphrase := okxDemoCredentialsFromEnv(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	wsURL := envOr("OKX_DEMO_WS_PRIVATE_URL", WSDemoPrivateURL)
	ws := NewWSClient(
		WithWSURL(wsURL),
		WithWSCredentials(apiKey, secretKey, passphrase),
	)
	if err := ws.Connect(ctx); err != nil {
		t.Fatalf("connect demo private ws: %v", err)
	}
	defer func() {
		if err := ws.Close(); err != nil {
			t.Logf("close demo private ws: %v", err)
		}
	}()

	if err := ws.Login(ctx); err != nil {
		t.Fatalf("login demo private ws: %v", err)
	}
}

func TestIntegrationPublicOrderBookDepthChannels(t *testing.T) {
	if strings.TrimSpace(os.Getenv("OKX_WS_INTEGRATION")) != "1" {
		t.Skip("set OKX_WS_INTEGRATION=1 to run OKX public WebSocket integration tests")
	}

	instID := strings.TrimSpace(os.Getenv("OKX_WS_DEPTH_INST_ID"))
	if instID == "" {
		instID = "BTC-USDT"
	}

	tests := publicWSDepthChannelCases(t)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			ws := NewWSClient()
			if wsURL := strings.TrimSpace(os.Getenv("OKX_WS_PUBLIC_URL")); wsURL != "" {
				WithWSURL(wsURL)(ws)
			}
			if err := ws.Connect(ctx); err != nil {
				t.Fatalf("connect public ws: %v", err)
			}
			defer func() {
				if err := ws.Close(); err != nil {
					t.Logf("close public ws: %v", err)
				}
			}()

			ch, err := ws.Subscribe(ctx, Subscription{
				Channel: tt.channel,
				Args:    map[string]string{"instId": instID},
			})
			if err != nil {
				t.Fatalf("subscribe %s %s: %v", tt.channel, instID, err)
			}

			msg := waitForWSMessage(t, ch, 20*time.Second)
			if msg.Arg["channel"] != tt.channel {
				t.Fatalf("channel = %q, want %q", msg.Arg["channel"], tt.channel)
			}
			if msg.Arg["instId"] != instID {
				t.Fatalf("instId = %q, want %q", msg.Arg["instId"], instID)
			}

			books := decodeOrderBookData(t, msg.Data)
			if len(books) == 0 {
				t.Fatal("empty order book data")
			}
			if len(books[0].Asks) == 0 {
				t.Fatal("empty asks")
			}
			if len(books[0].Bids) == 0 {
				t.Fatal("empty bids")
			}
			if len(books[0].Asks) > tt.maxLevels {
				t.Fatalf("asks length = %d, want <= %d", len(books[0].Asks), tt.maxLevels)
			}
			if len(books[0].Bids) > tt.maxLevels {
				t.Fatalf("bids length = %d, want <= %d", len(books[0].Bids), tt.maxLevels)
			}
		})
	}
}

func publicWSDepthChannelCases(t *testing.T) []wsDepthChannelCase {
	t.Helper()

	configured := strings.TrimSpace(os.Getenv("OKX_WS_DEPTH_CHANNELS"))
	if configured == "" {
		configured = "bbo-tbt,books5"
	}

	var tests []wsDepthChannelCase
	for _, item := range strings.Split(configured, ",") {
		channel := strings.TrimSpace(item)
		if channel == "" {
			continue
		}
		testCase, ok := wsDepthChannelCatalog[channel]
		if !ok {
			t.Fatalf("unsupported OKX_WS_DEPTH_CHANNELS entry %q", channel)
		}
		tests = append(tests, testCase)
	}
	if len(tests) == 0 {
		t.Fatal("OKX_WS_DEPTH_CHANNELS did not contain any valid channels")
	}
	return tests
}

func okxCredentialsFromEnv(t *testing.T) (string, string, string) {
	t.Helper()

	apiKey := strings.TrimSpace(os.Getenv("OKX_API_KEY"))
	secretKey := strings.TrimSpace(os.Getenv("OKX_SECRET_KEY"))
	passphrase := strings.TrimSpace(os.Getenv("OKX_PASSPHRASE"))
	if apiKey == "" || secretKey == "" || passphrase == "" {
		t.Skip("set OKX_API_KEY, OKX_SECRET_KEY, and OKX_PASSPHRASE to run OKX integration tests")
	}
	return apiKey, secretKey, passphrase
}

func okxDemoCredentialsFromEnv(t *testing.T) (string, string, string) {
	t.Helper()

	apiKey := strings.TrimSpace(os.Getenv("OKX_DEMO_API_KEY"))
	secretKey := strings.TrimSpace(os.Getenv("OKX_DEMO_SECRET_KEY"))
	passphrase := strings.TrimSpace(os.Getenv("OKX_DEMO_PASSPHRASE"))
	if apiKey == "" || secretKey == "" || passphrase == "" {
		t.Skip("set OKX_DEMO_API_KEY, OKX_DEMO_SECRET_KEY, and OKX_DEMO_PASSPHRASE to run OKX demo trading integration tests")
	}
	return apiKey, secretKey, passphrase
}

func demoLimitPrice(ctx context.Context, t *testing.T, client *RestClient, instID, side string) string {
	t.Helper()
	if instID != "BTC-USDT" {
		t.Skip("set OKX_DEMO_PX when OKX_DEMO_INST_ID is not BTC-USDT")
	}

	tickers, err := client.Market.Ticker(ctx, TickerRequest{InstID: instID})
	if err != nil {
		t.Fatalf("fetch %s ticker for demo order price: %v", instID, err)
	}
	if len(tickers) == 0 {
		t.Fatalf("empty %s ticker response", instID)
	}
	last, err := strconv.ParseFloat(tickers[0].Last, 64)
	if err != nil {
		t.Fatalf("parse %s last price %q: %v", instID, tickers[0].Last, err)
	}
	if strings.EqualFold(side, "sell") {
		return strconv.FormatFloat(last*2, 'f', 1, 64)
	}
	return strconv.FormatFloat(last*0.5, 'f', 1, 64)
}

func envOr(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func waitForWSMessage(t *testing.T, ch <-chan WSMessage, timeout time.Duration) WSMessage {
	t.Helper()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case msg, ok := <-ch:
		if !ok {
			t.Fatal("websocket message channel closed")
		}
		return msg
	case <-timer.C:
		t.Fatalf("timed out waiting for websocket message after %s", timeout)
		return WSMessage{}
	}
}
