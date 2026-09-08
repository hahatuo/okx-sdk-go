package okx

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	json "github.com/go-json-experiment/json"
)

const (
	testAccountLive = "live"
	testAccountDemo = "demo"

	localTestCredentialsPath = ".okx/credentials.json"
)

type testCredentialAccount struct {
	APIKey     string   `json:"apiKey"`
	SecretKey  string   `json:"secretKey"`
	Passphrase string   `json:"passphrase"`
	APIName    string   `json:"apiName,omitzero"`
	IP         string   `json:"ip,omitzero"`
	Permission []string `json:"permissions,omitzero"`
}

type testCredentials struct {
	Live testCredentialAccount `json:"live,omitzero"`
	Demo testCredentialAccount `json:"demo,omitzero"`
}

func TestIntegrationReadOnlyAccountBalance(t *testing.T) {
	requireOKXIntegration(t)

	acct := testOKXAccount(t, testAccountLive)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client := NewClient(
		WithCredentials(acct.APIKey, acct.SecretKey, acct.Passphrase),
		WithTimeout(10*time.Second),
	)
	if baseURL := strings.TrimSpace(os.Getenv("OKX_REST_URL")); baseURL != "" {
		WithBaseURL(baseURL)(client)
	}

	if _, err := client.Account.Balance(ctx, ""); err != nil {
		t.Fatalf("read-only account balance request failed: %v", err)
	}
}

func TestIntegrationPublicMarketData(t *testing.T) {
	if !integrationEnabled("OKX_PUBLIC_INTEGRATION") {
		t.Skip("set OKX_PUBLIC_INTEGRATION=1 to run OKX public REST integration tests")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient(WithTimeout(10 * time.Second))
	if baseURL := strings.TrimSpace(os.Getenv("OKX_REST_URL")); baseURL != "" {
		WithBaseURL(baseURL)(client)
	}

	instID := envOr("OKX_PUBLIC_INST_ID", "BTC-USDT")
	tickers, err := client.Market.Ticker(ctx, TickerRequest{InstID: instID})
	if err != nil {
		t.Fatalf("ticker %s: %v", instID, err)
	}
	if len(tickers) == 0 || tickers[0].InstID != instID {
		t.Fatalf("unexpected ticker response: %+v", tickers)
	}

	trades, err := client.Market.Trades(ctx, TradesRequest{InstID: instID, Limit: 1})
	if err != nil {
		t.Fatalf("trades %s: %v", instID, err)
	}
	if len(trades) == 0 || trades[0].InstID != instID || trades[0].TradeID == "" {
		t.Fatalf("unexpected trades response: %+v", trades)
	}

	historyTrades, err := client.Market.HistoryTrades(ctx, HistoryTradesRequest{InstID: instID, Limit: 1})
	if err != nil {
		t.Fatalf("history trades %s: %v", instID, err)
	}
	if len(historyTrades) == 0 || historyTrades[0].InstID != instID || historyTrades[0].TradeID == "" {
		t.Fatalf("unexpected history trades response: %+v", historyTrades)
	}

	bar := envOr("OKX_PUBLIC_CANDLE_BAR", "1m")
	candles, err := client.Market.Candles(ctx, instID, bar, 1)
	if err != nil {
		t.Fatalf("candles %s: %v", instID, err)
	}
	requireCandleRows(t, "candles", candles)

	historyCandles, err := client.Market.HistoryCandles(ctx, CandlesRequest{InstID: instID, Bar: bar, Limit: 1})
	if err != nil {
		t.Fatalf("history candles %s: %v", instID, err)
	}
	requireCandleRows(t, "history candles", historyCandles)

	indexID := envOr("OKX_PUBLIC_INDEX_INST_ID", "BTC-USD")
	indexCandles, err := client.Market.IndexCandles(ctx, CandlesRequest{InstID: indexID, Bar: bar, Limit: 1})
	if err != nil {
		t.Fatalf("index candles %s: %v", indexID, err)
	}
	requireCandleRows(t, "index candles", indexCandles)

	historyIndexCandles, err := client.Market.HistoryIndexCandles(ctx, CandlesRequest{InstID: indexID, Bar: bar, Limit: 1})
	if err != nil {
		t.Fatalf("history index candles %s: %v", indexID, err)
	}
	requireCandleRows(t, "history index candles", historyIndexCandles)

	markInstID := envOr("OKX_PUBLIC_MARK_PRICE_INST_ID", "BTC-USD-SWAP")
	markPriceCandles, err := client.Market.MarkPriceCandles(ctx, CandlesRequest{InstID: markInstID, Bar: bar, Limit: 1})
	if err != nil {
		t.Fatalf("mark price candles %s: %v", markInstID, err)
	}
	requireCandleRows(t, "mark price candles", markPriceCandles)

	historyMarkPriceCandles, err := client.Market.HistoryMarkPriceCandles(ctx, CandlesRequest{InstID: markInstID, Bar: bar, Limit: 1})
	if err != nil {
		t.Fatalf("history mark price candles %s: %v", markInstID, err)
	}
	requireCandleRows(t, "history mark price candles", historyMarkPriceCandles)

	instruments, err := client.Public.Instruments(ctx, InstrumentsRequest{InstType: InstSpot, InstID: instID})
	if err != nil {
		t.Fatalf("instrument %s: %v", instID, err)
	}
	if len(instruments) == 0 || instruments[0].InstID != instID {
		t.Fatalf("unexpected instruments response: %+v", instruments)
	}
}

func TestIntegrationDemoTradingPlaceAndCancelOrder(t *testing.T) {
	requireOKXTradingIntegration(t)

	acct := testOKXAccount(t, testAccountDemo)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := NewClient(
		WithCredentials(acct.APIKey, acct.SecretKey, acct.Passphrase),
		WithDemoTrading(true),
		WithTimeout(10*time.Second),
	)
	if baseURL := strings.TrimSpace(os.Getenv("OKX_DEMO_REST_URL")); baseURL != "" {
		WithBaseURL(baseURL)(client)
	}

	instID := envOr("OKX_DEMO_INST_ID", "BTC-USDT")
	side := Side(envOr("OKX_DEMO_SIDE", string(Buy)))
	size := envOr("OKX_DEMO_SZ", "0.0001")
	price := strings.TrimSpace(os.Getenv("OKX_DEMO_PX"))
	if price == "" {
		price = demoLimitPrice(ctx, t, client, instID, side)
	}
	clOrdID := "okxsdkgo" + strconv.FormatInt(time.Now().UnixNano(), 10)

	ack, err := client.Trade.PlaceOrder(ctx, PlaceOrderRequest{
		InstID:  instID,
		TdMode:  TdCash,
		Side:    side,
		OrdType: OrdLimit,
		Sz:      size,
		Px:      price,
		ClOrdID: clOrdID,
	})
	if err != nil {
		t.Fatalf("place demo order: %v", err)
	}
	if ack.SCode != "" && ack.SCode != "0" {
		t.Fatalf("place demo order rejected: %+v", ack)
	}

	cancelReq := CancelOrderRequest{InstID: instID, OrdID: ack.OrdID}
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

func TestIntegrationDemoTradingMarketBuyAndSellMinSize(t *testing.T) {
	if !integrationEnabled("OKX_DEMO_MARKET_ROUNDTRIP_INTEGRATION") {
		t.Skip("set OKX_DEMO_MARKET_ROUNDTRIP_INTEGRATION=1 to run OKX demo market buy/sell integration tests")
	}

	acct := testOKXAccount(t, testAccountDemo)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	client := NewClient(
		WithCredentials(acct.APIKey, acct.SecretKey, acct.Passphrase),
		WithDemoTrading(true),
		WithTimeout(10*time.Second),
	)
	if baseURL := strings.TrimSpace(os.Getenv("OKX_DEMO_REST_URL")); baseURL != "" {
		WithBaseURL(baseURL)(client)
	}

	instID := envOr("OKX_DEMO_MARKET_INST_ID", "BTC-USDT")
	inst := demoSpotInstrument(ctx, t, client, instID)
	minSize := inst.MinSz.String()
	if minSize == "" || minSize == "0" {
		t.Fatalf("%s returned invalid minSz %q", instID, inst.MinSz)
	}

	buyClOrdID := "okxsdkbuy" + strconv.FormatInt(time.Now().UnixNano(), 10)
	buyAck, err := client.Trade.PlaceOrder(ctx, PlaceOrderRequest{
		InstID:  instID,
		TdMode:  TdCash,
		Side:    Buy,
		OrdType: OrdMarket,
		Sz:      minSize,
		TgtCcy:  "base_ccy",
		ClOrdID: buyClOrdID,
	})
	if err != nil {
		t.Fatalf("market buy %s minSz=%s: %v", instID, minSize, err)
	}
	requireDemoOrderAck(t, "market buy", buyAck)
	buyOrder := waitForDemoOrderState(ctx, t, client, orderLookup(instID, buyAck, buyClOrdID), "filled")
	if buyOrder.AccFillSz == "" || buyOrder.AccFillSz == "0" {
		t.Fatalf("market buy filled with empty accFillSz: %+v", buyOrder)
	}

	sellClOrdID := "okxsdksell" + strconv.FormatInt(time.Now().UnixNano(), 10)
	sellAck, err := client.Trade.PlaceOrder(ctx, PlaceOrderRequest{
		InstID:  instID,
		TdMode:  TdCash,
		Side:    Sell,
		OrdType: OrdMarket,
		Sz:      minSize,
		TgtCcy:  "base_ccy",
		ClOrdID: sellClOrdID,
	})
	if err != nil {
		t.Fatalf("market sell %s minSz=%s after buy ordId=%s accFillSz=%s: %v", instID, minSize, buyAck.OrdID, buyOrder.AccFillSz, err)
	}
	requireDemoOrderAck(t, "market sell", sellAck)
	sellOrder := waitForDemoOrderState(ctx, t, client, orderLookup(instID, sellAck, sellClOrdID), "filled")
	if sellOrder.AccFillSz == "" || sellOrder.AccFillSz == "0" {
		t.Fatalf("market sell filled with empty accFillSz: %+v", sellOrder)
	}

	t.Logf("demo market round trip %s minSz=%s buyOrdId=%s buyFill=%s sellOrdId=%s sellFill=%s",
		instID, minSize, buyOrder.OrdID, buyOrder.AccFillSz, sellOrder.OrdID, sellOrder.AccFillSz)
}

func TestIntegrationDemoPrivateWebSocketConnect(t *testing.T) {
	requireOKXWebSocketIntegration(t)

	acct := testOKXAccount(t, testAccountDemo)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	ws := NewWSClient(Private, WithWSDemo(true), WithWSCredentials(acct.APIKey, acct.SecretKey, acct.Passphrase))
	if err := ws.Connect(ctx); err != nil {
		t.Fatalf("connect demo private ws: %v", err)
	}
	defer func() {
		if err := ws.Close(); err != nil {
			t.Logf("close demo private ws: %v", err)
		}
	}()
}

func TestIntegrationPublicOrderBookDepthChannels(t *testing.T) {
	if !integrationEnabled("OKX_WS_INTEGRATION") {
		t.Skip("set OKX_WS_INTEGRATION=1 to run OKX public WebSocket integration tests")
	}

	instID := envOr("OKX_WS_DEPTH_INST_ID", "BTC-USDT")
	channels := strings.Split(envOr("OKX_WS_DEPTH_CHANNELS", "bbo-tbt,books5"), ",")
	for _, channel := range channels {
		channel := strings.TrimSpace(channel)
		if channel == "" {
			continue
		}
		t.Run(channel, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			opts := []WSOption{}
			if wsURL := strings.TrimSpace(os.Getenv("OKX_WS_PUBLIC_URL")); wsURL != "" {
				opts = append(opts, WithWSURL(wsURL))
			}
			ws := NewWSClient(Public, opts...)
			if err := ws.Connect(ctx); err != nil {
				t.Fatalf("connect public ws: %v", err)
			}
			defer func() {
				if err := ws.Close(); err != nil {
					t.Logf("close public ws: %v", err)
				}
			}()

			msgCh := make(chan WSTypedMessage[OrderBook], 1)
			if err := ws.SubscribeOrderBook(ctx, channel, instID, func(msg WSTypedMessage[OrderBook]) {
				select {
				case msgCh <- msg:
				default:
				}
			}); err != nil {
				t.Fatalf("subscribe %s %s: %v", channel, instID, err)
			}

			msg := waitForTypedMessage(t, msgCh, 20*time.Second)
			if msg.Err != nil {
				t.Fatalf("decode %s %s: %v", channel, instID, msg.Err)
			}
			if msg.Arg.Channel != channel || msg.Arg.InstID != instID {
				t.Fatalf("unexpected arg: %+v", msg.Arg)
			}
			if len(msg.Data) == 0 {
				t.Fatal("empty order book data")
			}
			if len(msg.Data[0].Asks) == 0 || len(msg.Data[0].Bids) == 0 {
				t.Fatalf("empty order book side: %+v", msg.Data[0])
			}
			if err := ws.Unsubscribe(ctx, channel, instID); err != nil {
				t.Fatalf("unsubscribe %s %s: %v", channel, instID, err)
			}
		})
	}
}

func TestIntegrationPublicTradesChannel(t *testing.T) {
	if !integrationEnabled("OKX_WS_INTEGRATION") {
		t.Skip("set OKX_WS_INTEGRATION=1 to run OKX public WebSocket integration tests")
	}

	instID := envOr("OKX_WS_TRADES_INST_ID", "BTC-USDT")
	channels := strings.Split(envOr("OKX_WS_TRADES_CHANNELS", WSChannelTrades), ",")
	for _, channel := range channels {
		channel := strings.TrimSpace(channel)
		if channel == "" {
			continue
		}
		t.Run(channel, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			opts := []WSOption{}
			if wsURL := strings.TrimSpace(os.Getenv("OKX_WS_PUBLIC_URL")); wsURL != "" {
				opts = append(opts, WithWSURL(wsURL))
			}
			ws := NewWSClient(Public, opts...)
			if err := ws.Connect(ctx); err != nil {
				t.Fatalf("connect public ws: %v", err)
			}
			defer func() {
				if err := ws.Close(); err != nil {
					t.Logf("close public ws: %v", err)
				}
			}()

			msgCh := make(chan WSTypedMessage[MarketTrade], 1)
			handler := func(msg WSTypedMessage[MarketTrade]) {
				select {
				case msgCh <- msg:
				default:
				}
			}
			var err error
			switch channel {
			case WSChannelTrades:
				err = ws.SubscribeTrades(ctx, instID, handler)
			case WSChannelTradesAll:
				err = ws.SubscribeAllTrades(ctx, instID, handler)
			default:
				t.Fatalf("unsupported trades channel %q", channel)
			}
			if err != nil {
				t.Fatalf("subscribe %s %s: %v", channel, instID, err)
			}

			msg := waitForTypedMessage(t, msgCh, 20*time.Second)
			if msg.Err != nil {
				t.Fatalf("decode %s %s: %v", channel, instID, msg.Err)
			}
			if msg.Arg.Channel != channel || msg.Arg.InstID != instID {
				t.Fatalf("unexpected arg: %+v", msg.Arg)
			}
			if len(msg.Data) == 0 {
				t.Fatal("empty trades data")
			}
			if msg.Data[0].InstID != instID || msg.Data[0].TradeID == "" || msg.Data[0].Px == "" || msg.Data[0].Sz == "" {
				t.Fatalf("unexpected trades data: %+v", msg.Data[0])
			}
			if err := ws.Unsubscribe(ctx, channel, instID); err != nil {
				t.Fatalf("unsubscribe %s %s: %v", channel, instID, err)
			}
		})
	}
}

func TestIntegrationBusinessCandlesChannels(t *testing.T) {
	if !integrationEnabled("OKX_WS_INTEGRATION") {
		t.Skip("set OKX_WS_INTEGRATION=1 to run OKX business WebSocket integration tests")
	}

	bar := envOr("OKX_WS_CANDLE_BAR", "1m")
	tests := []struct {
		name      string
		channel   string
		instID    string
		subscribe func(context.Context, *WSClient, string, string, WSTypedHandler[Candle]) error
	}{
		{
			name:    "regular",
			channel: CandleChannel(bar),
			instID:  envOr("OKX_WS_CANDLE_INST_ID", "BTC-USDT"),
			subscribe: func(ctx context.Context, ws *WSClient, instID, bar string, handler WSTypedHandler[Candle]) error {
				return ws.SubscribeCandles(ctx, instID, bar, handler)
			},
		},
		{
			name:    "index",
			channel: IndexCandleChannel(bar),
			instID:  envOr("OKX_WS_INDEX_CANDLE_INST_ID", "BTC-USD"),
			subscribe: func(ctx context.Context, ws *WSClient, instID, bar string, handler WSTypedHandler[Candle]) error {
				return ws.SubscribeIndexCandles(ctx, instID, bar, handler)
			},
		},
		{
			name:    "mark price",
			channel: MarkPriceCandleChannel(bar),
			instID:  envOr("OKX_WS_MARK_PRICE_CANDLE_INST_ID", "BTC-USD-SWAP"),
			subscribe: func(ctx context.Context, ws *WSClient, instID, bar string, handler WSTypedHandler[Candle]) error {
				return ws.SubscribeMarkPriceCandles(ctx, instID, bar, handler)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			opts := []WSOption{}
			if wsURL := strings.TrimSpace(os.Getenv("OKX_WS_BUSINESS_URL")); wsURL != "" {
				opts = append(opts, WithWSURL(wsURL))
			}
			ws := NewWSClient(Business, opts...)
			if err := ws.Connect(ctx); err != nil {
				t.Fatalf("connect business ws: %v", err)
			}
			defer func() {
				if err := ws.Close(); err != nil {
					t.Logf("close business ws: %v", err)
				}
			}()

			msgCh := make(chan WSTypedMessage[Candle], 1)
			if err := tt.subscribe(ctx, ws, tt.instID, bar, func(msg WSTypedMessage[Candle]) {
				select {
				case msgCh <- msg:
				default:
				}
			}); err != nil {
				t.Fatalf("subscribe %s %s: %v", tt.channel, tt.instID, err)
			}

			msg := waitForTypedMessage(t, msgCh, 20*time.Second)
			if msg.Err != nil {
				t.Fatalf("decode %s %s: %v", tt.channel, tt.instID, msg.Err)
			}
			if msg.Arg.Channel != tt.channel || msg.Arg.InstID != tt.instID {
				t.Fatalf("unexpected arg: %+v", msg.Arg)
			}
			requireCandleRows(t, tt.channel, msg.Data)
			if err := ws.Unsubscribe(ctx, tt.channel, tt.instID); err != nil {
				t.Fatalf("unsubscribe %s %s: %v", tt.channel, tt.instID, err)
			}
		})
	}
}

func loadTestCredentials(t testing.TB) testCredentials {
	t.Helper()

	var creds testCredentials
	if data, err := os.ReadFile(localTestCredentialsPath); err == nil {
		if err := json.Unmarshal(data, &creds); err != nil {
			t.Fatalf("read %s: %v", localTestCredentialsPath, err)
		}
	} else if !os.IsNotExist(err) {
		t.Fatalf("read %s: %v", localTestCredentialsPath, err)
	}

	overlayTestAccountEnv("OKX_LIVE", &creds.Live)
	overlayTestAccountEnv("OKX_PROD", &creds.Live)
	overlayTestAccountEnv("OKX_PRODUCTION", &creds.Live)
	overlayTestAccountEnv("OKX_DEMO", &creds.Demo)
	overlayTestAccountEnv("OKX_SIM", &creds.Demo)
	overlayTestAccountEnv("OKX_PAPER", &creds.Demo)

	if !creds.Demo.valid() {
		overlayTestAccountEnv("OKX", &creds.Demo)
	}
	return creds
}

func testOKXAccount(t testing.TB, name string) testCredentialAccount {
	t.Helper()

	creds := loadTestCredentials(t)
	switch name {
	case testAccountLive:
		if !creds.Live.valid() {
			t.Skip("missing OKX live credentials")
		}
		return creds.Live
	case testAccountDemo:
		if !creds.Demo.valid() {
			t.Skip("missing OKX demo credentials")
		}
		return creds.Demo
	default:
		t.Fatalf("unknown OKX test account %q", name)
		return testCredentialAccount{}
	}
}

func (a testCredentialAccount) valid() bool {
	return a.APIKey != "" && a.SecretKey != "" && a.Passphrase != ""
}

func requireOKXIntegration(t testing.TB) {
	t.Helper()
	if !integrationEnabled("OKX_RUN_INTEGRATION") && !integrationEnabled("OKX_REST_INTEGRATION") {
		t.Skip("set OKX_RUN_INTEGRATION=1 or OKX_REST_INTEGRATION=1 to run OKX REST integration tests")
	}
}

func requireOKXTradingIntegration(t testing.TB) {
	t.Helper()
	if !integrationEnabled("OKX_RUN_TRADING_INTEGRATION") && !integrationEnabled("OKX_DEMO_TRADING_INTEGRATION") {
		t.Skip("set OKX_RUN_TRADING_INTEGRATION=1 or OKX_DEMO_TRADING_INTEGRATION=1 to run OKX demo trading integration tests")
	}
}

func requireOKXWebSocketIntegration(t testing.TB) {
	t.Helper()
	if !integrationEnabled("OKX_RUN_WS_INTEGRATION") && !integrationEnabled("OKX_DEMO_WS_INTEGRATION") {
		t.Skip("set OKX_RUN_WS_INTEGRATION=1 or OKX_DEMO_WS_INTEGRATION=1 to run OKX websocket integration tests")
	}
}

func integrationEnabled(name string) bool {
	return strings.TrimSpace(os.Getenv(name)) == "1"
}

func overlayTestAccountEnv(prefix string, acct *testCredentialAccount) {
	setEnvValue(&acct.APIKey, prefix+"_API_KEY", prefix+"_KEY")
	setEnvValue(&acct.SecretKey, prefix+"_SECRET_KEY", prefix+"_API_SECRET", prefix+"_SECRET")
	setEnvValue(&acct.Passphrase, prefix+"_PASSPHRASE", prefix+"_API_PASSPHRASE")
}

func setEnvValue(dst *string, names ...string) {
	for _, name := range names {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			*dst = value
			return
		}
	}
}

func demoSpotInstrument(ctx context.Context, t *testing.T, client *Client, instID string) Instrument {
	t.Helper()

	instruments, err := client.Public.Instruments(ctx, InstrumentsRequest{InstType: InstSpot, InstID: instID})
	if err != nil {
		t.Fatalf("fetch %s instrument configuration: %v", instID, err)
	}
	if len(instruments) == 0 {
		t.Fatalf("empty %s instrument configuration", instID)
	}
	inst := instruments[0]
	if inst.InstID != instID {
		t.Fatalf("unexpected instrument configuration: %+v", inst)
	}
	return inst
}

func requireDemoOrderAck(t testing.TB, name string, ack OrderAck) {
	t.Helper()

	if ack.SCode != "" && ack.SCode != "0" {
		t.Fatalf("%s rejected: %+v", name, ack)
	}
	if ack.OrdID == "" && ack.ClOrdID == "" {
		t.Fatalf("%s returned no order identifier: %+v", name, ack)
	}
}

func orderLookup(instID string, ack OrderAck, fallbackClOrdID string) OrderRequest {
	req := OrderRequest{InstID: instID, OrdID: ack.OrdID}
	if req.OrdID == "" {
		req.ClOrdID = ack.ClOrdID
	}
	if req.ClOrdID == "" {
		req.ClOrdID = fallbackClOrdID
	}
	return req
}

func waitForDemoOrderState(ctx context.Context, t *testing.T, client *Client, req OrderRequest, want string) Order {
	t.Helper()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(15 * time.Second)
	defer deadline.Stop()

	var last Order
	var lastErr error
	for {
		orders, err := client.Trade.Order(ctx, req)
		if err != nil {
			lastErr = err
		} else if len(orders) > 0 {
			last = orders[0]
			if last.State == want {
				return last
			}
		}

		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("order %s/%s did not reach state %q, last=%+v err=%v", req.OrdID, req.ClOrdID, want, last, lastErr)
		case <-ctx.Done():
			t.Fatalf("context expired waiting for order %s/%s state %q, last=%+v err=%v: %v", req.OrdID, req.ClOrdID, want, last, lastErr, ctx.Err())
		}
	}
}

func demoLimitPrice(ctx context.Context, t *testing.T, client *Client, instID string, side Side) string {
	t.Helper()

	tickers, err := client.Market.Ticker(ctx, TickerRequest{InstID: instID})
	if err != nil {
		t.Fatalf("fetch %s ticker for demo order price: %v", instID, err)
	}
	if len(tickers) == 0 {
		t.Fatalf("empty %s ticker response", instID)
	}
	last, err := strconv.ParseFloat(tickers[0].Last.String(), 64)
	if err != nil {
		t.Fatalf("parse %s last price %q: %v", instID, tickers[0].Last, err)
	}
	if side == Sell {
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

func waitForTypedMessage[T any](t *testing.T, ch <-chan WSTypedMessage[T], timeout time.Duration) WSTypedMessage[T] {
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
		return WSTypedMessage[T]{}
	}
}

func requireCandleRows(t testing.TB, name string, candles []Candle) {
	t.Helper()
	if len(candles) == 0 {
		t.Fatalf("%s returned no rows", name)
	}
	row := candles[0]
	if row.TS == "" || row.Open == "" || row.High == "" || row.Low == "" || row.Close == "" || row.Confirm == "" {
		t.Fatalf("unexpected %s row: %+v", name, row)
	}
}
