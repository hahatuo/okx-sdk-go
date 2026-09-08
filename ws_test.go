package okx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	json "github.com/go-json-experiment/json"
)

func TestOrderBookChannelHelpers(t *testing.T) {
	tests := []struct {
		depth   int
		channel string
		round   int
	}{
		{depth: 1, channel: WSChannelBBO, round: 1},
		{depth: 5, channel: WSChannelBooks5, round: 5},
		{depth: 10, channel: WSChannelBooks50L2TBT, round: 50},
		{depth: 50, channel: WSChannelBooks50L2TBT, round: 50},
		{depth: 400, channel: WSChannelBooks, round: 400},
	}
	for _, tt := range tests {
		if got := OrderBookChannel(tt.depth); got != tt.channel {
			t.Fatalf("OrderBookChannel(%d) = %q, want %q", tt.depth, got, tt.channel)
		}
		if got := OrderBookDepthFromChannel(tt.channel); got != tt.round {
			t.Fatalf("OrderBookDepthFromChannel(%q) = %d, want %d", tt.channel, got, tt.round)
		}
	}
}

func TestPublicOrderBookChannelUsesNonVIPChannels(t *testing.T) {
	tests := []struct {
		depth   int
		channel string
		round   int
	}{
		{depth: 1, channel: WSChannelBooks5, round: 5},
		{depth: 5, channel: WSChannelBooks5, round: 5},
		{depth: 6, channel: WSChannelBooks, round: 400},
		{depth: 50, channel: WSChannelBooks, round: 400},
		{depth: 400, channel: WSChannelBooks, round: 400},
	}
	for _, tt := range tests {
		if got := PublicOrderBookChannel(tt.depth); got != tt.channel {
			t.Fatalf("PublicOrderBookChannel(%d) = %q, want %q", tt.depth, got, tt.channel)
		}
		if got := PublicOrderBookDepth(tt.depth); got != tt.round {
			t.Fatalf("PublicOrderBookDepth(%d) = %d, want %d", tt.depth, got, tt.round)
		}
	}
}

func TestDecodeTypedWSMessage(t *testing.T) {
	msg := Message{
		Channel: WSChannelTickers,
		InstID:  "BTC-USDT",
		Data:    []byte(`[{"instId":"BTC-USDT","last":"1","askPx":"2","bidPx":"0.9","vol24h":"10","ts":"1"}]`),
	}
	got := decodeTypedWSMessage[Ticker](msg)
	if got.Err != nil {
		t.Fatal(got.Err)
	}
	if got.Arg.Channel != WSChannelTickers || got.Arg.InstID != "BTC-USDT" {
		t.Fatalf("unexpected arg: %+v", got.Arg)
	}
	if len(got.Data) != 1 || got.Data[0].Last != "1" {
		t.Fatalf("unexpected data: %+v", got.Data)
	}
}

func TestSubscribeTradesTypedMessages(t *testing.T) {
	tests := []struct {
		name      string
		channel   string
		subscribe func(context.Context, *WSClient, string, WSTypedHandler[MarketTrade]) error
	}{
		{
			name:    "aggregated trades",
			channel: WSChannelTrades,
			subscribe: func(ctx context.Context, c *WSClient, instID string, handler WSTypedHandler[MarketTrade]) error {
				return c.SubscribeTrades(ctx, instID, handler)
			},
		},
		{
			name:    "all trades",
			channel: WSChannelTradesAll,
			subscribe: func(ctx context.Context, c *WSClient, instID string, handler WSTypedHandler[MarketTrade]) error {
				return c.SubscribeAllTrades(ctx, instID, handler)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverErr := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer closeTestWSConn(conn)

				_, raw, err := conn.Read(context.Background())
				if err != nil {
					serverErr <- err
					return
				}
				var req struct {
					ID   string   `json:"id"`
					Op   string   `json:"op"`
					Args []subArg `json:"args"`
				}
				if err := json.Unmarshal(raw, &req); err != nil {
					serverErr <- err
					return
				}
				if req.Op != "subscribe" || len(req.Args) != 1 {
					serverErr <- fmt.Errorf("unexpected subscribe request: %s", raw)
					return
				}
				if req.Args[0].Channel != tt.channel || req.Args[0].InstID != "BTC-USDT" {
					serverErr <- fmt.Errorf("unexpected subscribe arg: %+v", req.Args[0])
					return
				}

				ack, err := json.Marshal(map[string]any{
					"id":    req.ID,
					"event": "subscribe",
					"code":  "0",
					"arg": map[string]string{
						"channel": tt.channel,
						"instId":  "BTC-USDT",
					},
				})
				if err != nil {
					serverErr <- err
					return
				}
				if err := conn.Write(context.Background(), websocket.MessageText, ack); err != nil {
					serverErr <- err
					return
				}

				push, err := json.Marshal(map[string]any{
					"arg": map[string]string{
						"channel": tt.channel,
						"instId":  "BTC-USDT",
					},
					"data": []map[string]string{{
						"instId":  "BTC-USDT",
						"tradeId": "242720720",
						"px":      "29963.2",
						"sz":      "0.001",
						"side":    "sell",
						"count":   "2",
						"ts":      "1597026383085",
					}},
				})
				if err != nil {
					serverErr <- err
					return
				}
				if err := conn.Write(context.Background(), websocket.MessageText, push); err != nil {
					serverErr <- err
					return
				}

				ctx := conn.CloseRead(context.Background())
				<-ctx.Done()
			}))
			defer server.Close()

			client := NewWSClient(Public, WithWSURL(websocketTestURL(server)))
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := client.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := client.Close(); err != nil {
					t.Logf("close websocket: %v", err)
				}
			}()

			msgCh := make(chan WSTypedMessage[MarketTrade], 1)
			if err := tt.subscribe(ctx, client, "BTC-USDT", func(msg WSTypedMessage[MarketTrade]) {
				select {
				case msgCh <- msg:
				default:
				}
			}); err != nil {
				t.Fatal(err)
			}

			select {
			case msg := <-msgCh:
				if msg.Err != nil {
					t.Fatal(msg.Err)
				}
				if msg.Arg.Channel != tt.channel || msg.Arg.InstID != "BTC-USDT" {
					t.Fatalf("unexpected arg: %+v", msg.Arg)
				}
				if len(msg.Data) != 1 {
					t.Fatalf("data length = %d, want 1", len(msg.Data))
				}
				trade := msg.Data[0]
				if trade.TradeID != "242720720" || trade.Px != "29963.2" || trade.Side != Sell || trade.Count.String() != "2" {
					t.Fatalf("unexpected trade: %+v", trade)
				}
			case <-ctx.Done():
				t.Fatal("timed out waiting for typed trades message")
			}

			select {
			case err := <-serverErr:
				t.Fatal(err)
			default:
			}
		})
	}
}

func TestSubscribeTradesValidation(t *testing.T) {
	client := NewWSClient(Public)
	ctx := context.Background()
	if err := client.SubscribeTrades(ctx, "", func(WSTypedMessage[MarketTrade]) {}); err == nil || !strings.Contains(err.Error(), "instId") {
		t.Fatalf("SubscribeTrades empty instID error = %v, want instId", err)
	}
	if err := client.SubscribeAllTrades(ctx, "", func(WSTypedMessage[MarketTrade]) {}); err == nil || !strings.Contains(err.Error(), "instId") {
		t.Fatalf("SubscribeAllTrades empty instID error = %v, want instId", err)
	}
	if err := client.SubscribeTrades(ctx, "BTC-USDT", nil); err == nil || !strings.Contains(err.Error(), "handler") {
		t.Fatalf("SubscribeTrades nil handler error = %v, want handler", err)
	}
}

func TestCandleChannelHelpers(t *testing.T) {
	tests := []struct {
		name string
		fn   func(string) string
		bar  string
		want string
	}{
		{name: "regular", fn: CandleChannel, bar: "1m", want: "candle1m"},
		{name: "mark price", fn: MarkPriceCandleChannel, bar: "1H", want: "mark-price-candle1H"},
		{name: "index", fn: IndexCandleChannel, bar: "1Dutc", want: "index-candle1Dutc"},
	}
	for _, tt := range tests {
		if got := tt.fn(tt.bar); got != tt.want {
			t.Fatalf("%s channel = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestSubscribeCandleTypedMessages(t *testing.T) {
	tests := []struct {
		name      string
		kind      WSKind
		channel   string
		instID    string
		subscribe func(context.Context, *WSClient, string, string, WSTypedHandler[Candle]) error
	}{
		{
			name:    "regular",
			kind:    Business,
			channel: CandleChannel("1m"),
			instID:  "BTC-USDT",
			subscribe: func(ctx context.Context, c *WSClient, instID, bar string, handler WSTypedHandler[Candle]) error {
				return c.SubscribeCandles(ctx, instID, bar, handler)
			},
		},
		{
			name:    "mark price",
			kind:    Business,
			channel: MarkPriceCandleChannel("1m"),
			instID:  "BTC-USD-SWAP",
			subscribe: func(ctx context.Context, c *WSClient, instID, bar string, handler WSTypedHandler[Candle]) error {
				return c.SubscribeMarkPriceCandles(ctx, instID, bar, handler)
			},
		},
		{
			name:    "index",
			kind:    Business,
			channel: IndexCandleChannel("1m"),
			instID:  "BTC-USD",
			subscribe: func(ctx context.Context, c *WSClient, instID, bar string, handler WSTypedHandler[Candle]) error {
				return c.SubscribeIndexCandles(ctx, instID, bar, handler)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			serverErr := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer closeTestWSConn(conn)

				_, raw, err := conn.Read(context.Background())
				if err != nil {
					serverErr <- err
					return
				}
				var req struct {
					ID   string   `json:"id"`
					Op   string   `json:"op"`
					Args []subArg `json:"args"`
				}
				if err := json.Unmarshal(raw, &req); err != nil {
					serverErr <- err
					return
				}
				if req.Op != "subscribe" || len(req.Args) != 1 {
					serverErr <- fmt.Errorf("unexpected subscribe request: %s", raw)
					return
				}
				if req.Args[0].Channel != tt.channel || req.Args[0].InstID != tt.instID {
					serverErr <- fmt.Errorf("unexpected subscribe arg: %+v", req.Args[0])
					return
				}

				ack, err := json.Marshal(map[string]any{
					"id":    req.ID,
					"event": "subscribe",
					"code":  "0",
					"arg": map[string]string{
						"channel": tt.channel,
						"instId":  tt.instID,
					},
				})
				if err != nil {
					serverErr <- err
					return
				}
				if err := conn.Write(context.Background(), websocket.MessageText, ack); err != nil {
					serverErr <- err
					return
				}

				push, err := json.Marshal(map[string]any{
					"arg": map[string]string{
						"channel": tt.channel,
						"instId":  tt.instID,
					},
					"data": []any{[]string{
						"1597026383085",
						"1",
						"2",
						"0.5",
						"1.5",
						"0",
					}},
				})
				if err != nil {
					serverErr <- err
					return
				}
				if err := conn.Write(context.Background(), websocket.MessageText, push); err != nil {
					serverErr <- err
					return
				}

				ctx := conn.CloseRead(context.Background())
				<-ctx.Done()
			}))
			defer server.Close()

			client := NewWSClient(tt.kind, WithWSURL(websocketTestURL(server)))
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if err := client.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := client.Close(); err != nil {
					t.Logf("close websocket: %v", err)
				}
			}()

			msgCh := make(chan WSTypedMessage[Candle], 1)
			if err := tt.subscribe(ctx, client, tt.instID, "1m", func(msg WSTypedMessage[Candle]) {
				select {
				case msgCh <- msg:
				default:
				}
			}); err != nil {
				t.Fatal(err)
			}

			select {
			case msg := <-msgCh:
				if msg.Err != nil {
					t.Fatal(msg.Err)
				}
				if msg.Arg.Channel != tt.channel || msg.Arg.InstID != tt.instID {
					t.Fatalf("unexpected arg: %+v", msg.Arg)
				}
				if len(msg.Data) != 1 || msg.Data[0].TS != "1597026383085" || msg.Data[0].Open != "1" || msg.Data[0].Confirm != "0" {
					t.Fatalf("unexpected candle: %+v", msg.Data)
				}
			case <-ctx.Done():
				t.Fatal("timed out waiting for typed candle message")
			}

			select {
			case err := <-serverErr:
				t.Fatal(err)
			default:
			}
		})
	}
}

func TestSubscribeCandleValidation(t *testing.T) {
	client := NewWSClient(Business)
	ctx := context.Background()
	if err := client.SubscribeCandles(ctx, "", "1m", func(WSTypedMessage[Candle]) {}); err == nil || !strings.Contains(err.Error(), "instId") {
		t.Fatalf("SubscribeCandles empty instID error = %v, want instId", err)
	}
	if err := client.SubscribeCandles(ctx, "BTC-USDT", "", func(WSTypedMessage[Candle]) {}); err == nil || !strings.Contains(err.Error(), "bar") {
		t.Fatalf("SubscribeCandles empty bar error = %v, want bar", err)
	}
	if err := client.SubscribeCandles(ctx, "BTC-USDT", "1m", nil); err == nil || !strings.Contains(err.Error(), "handler") {
		t.Fatalf("SubscribeCandles nil handler error = %v, want handler", err)
	}
	if err := client.SubscribeMarkPriceCandles(ctx, "", "1m", func(WSTypedMessage[Candle]) {}); err == nil || !strings.Contains(err.Error(), "instId") {
		t.Fatalf("SubscribeMarkPriceCandles empty instID error = %v, want instId", err)
	}
	if err := client.SubscribeIndexCandles(ctx, "BTC-USD", "", func(WSTypedMessage[Candle]) {}); err == nil || !strings.Contains(err.Error(), "bar") {
		t.Fatalf("SubscribeIndexCandles empty bar error = %v, want bar", err)
	}
}

func TestWSDispatchesStructuredSubscriptionArgs(t *testing.T) {
	client := NewWSClient(Private)
	got := make(chan WSTypedMessage[AccountUpdate], 1)

	arg := subArg{Channel: WSChannelAccount, Ccy: "BTC"}
	handlers := map[string]*subEntry{
		arg.key(): {
			arg: arg,
			handler: func(msg Message) {
				got <- decodeTypedWSMessage[AccountUpdate](msg)
			},
		},
	}
	client.handlers.Store(&handlers)

	client.dispatch([]byte(`{"arg":{"channel":"account","ccy":"BTC"},"data":[{"uTime":"1","totalEq":"2","details":[{"ccy":"BTC","eq":"1"}]}]}`))

	select {
	case msg := <-got:
		if msg.Err != nil {
			t.Fatal(msg.Err)
		}
		if msg.Arg.Ccy != "BTC" || len(msg.Data) != 1 || msg.Data[0].TotalEq != "2" {
			t.Fatalf("unexpected message: %+v", msg)
		}
	default:
		t.Fatal("message was not dispatched")
	}
}

func TestWSSystemEventsExposeConnectionCount(t *testing.T) {
	client := NewWSClient(Public)
	raw := []byte(`{"event":"channel-conn-count","channel":"orders","connCount":"2","connId":"abc"}`)

	client.dispatch(raw)

	select {
	case event := <-client.SystemEvents():
		if event.Event != "channel-conn-count" || event.Channel != "orders" || event.ConnCount != "2" || event.ConnID != "abc" {
			t.Fatalf("unexpected system event: %+v", event)
		}
		if string(event.Raw) != string(raw) {
			t.Fatalf("raw event = %s, want %s", event.Raw, raw)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for system event")
	}
}

func TestWSNoticeEventIsExposedAsSystemEvent(t *testing.T) {
	client := NewWSClient(Public)
	raw := []byte(`{"event":"notice","code":"64008","msg":"The connection will soon be closed for a service upgrade. Please reconnect.","connId":"abc"}`)

	client.dispatch(raw)

	select {
	case event := <-client.SystemEvents():
		if event.Event != "notice" || event.Code != "64008" || event.ConnID != "abc" {
			t.Fatalf("unexpected notice event: %+v", event)
		}
		if string(event.Raw) != string(raw) {
			t.Fatalf("raw event = %s, want %s", event.Raw, raw)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for notice event")
	}
}

func TestWSUnmatchedErrorEventIsExposedAsSystemEvent(t *testing.T) {
	client := NewWSClient(Public)

	client.dispatch([]byte(`{"event":"error","code":"60018","msg":"Wrong URL or channel","arg":{"channel":"tickers","instId":"BTC-USDT"}}`))

	select {
	case event := <-client.SystemEvents():
		if event.Event != "error" || event.Code != "60018" || event.Msg != "Wrong URL or channel" {
			t.Fatalf("unexpected system event: %+v", event)
		}
		if event.Arg.Channel != WSChannelTickers || event.Arg.InstID != "BTC-USDT" {
			t.Fatalf("unexpected arg: %+v", event.Arg)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for unmatched error event")
	}
}

func TestWSPingLoopSendsApplicationPing(t *testing.T) {
	pinged := make(chan struct{})
	serverErr := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer closeTestWSConn(conn)
		for {
			_, raw, err := conn.Read(context.Background())
			if err != nil {
				serverErr <- err
				return
			}
			if string(raw) == "ping" {
				close(pinged)
				_ = conn.Write(context.Background(), websocket.MessageText, []byte("pong"))
				ctx := conn.CloseRead(context.Background())
				<-ctx.Done()
				return
			}
		}
	}))
	defer server.Close()

	client := NewWSClient(Public, WithWSURL(websocketTestURL(server)))
	client.pingInt = 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	}()

	select {
	case <-pinged:
	case err := <-serverErr:
		t.Fatalf("server read: %v", err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for ping")
	}
}

func TestWSPingLoopTimesOutBeforeFirstRead(t *testing.T) {
	closed := make(chan websocket.StatusCode, 1)
	serverErr := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer closeTestWSConn(conn)
		for {
			_, raw, err := conn.Read(context.Background())
			if err != nil {
				closed <- websocket.CloseStatus(err)
				return
			}
			if string(raw) != "ping" {
				serverErr <- fmt.Errorf("unexpected websocket frame: %s", raw)
				return
			}
		}
	}))
	defer server.Close()

	client := NewWSClient(Public, WithWSURL(websocketTestURL(server)), WithWSReconnect(false))
	client.pingInt = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	}()

	select {
	case status := <-closed:
		if status != websocket.StatusGoingAway {
			t.Fatalf("close status = %v, want %v", status, websocket.StatusGoingAway)
		}
	case err := <-serverErr:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("timed out waiting for read timeout close")
	}
}

func TestWSConnectRejectsDuplicateConnection(t *testing.T) {
	var accepted atomic.Int32
	acceptedCh := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		accepted.Add(1)
		select {
		case acceptedCh <- struct{}{}:
		default:
		}
		ctx := conn.CloseRead(context.Background())
		<-ctx.Done()
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}))
	defer server.Close()

	client := NewWSClient(Public, WithWSURL(websocketTestURL(server)))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-acceptedCh:
	case <-ctx.Done():
		t.Fatal("timed out waiting for first websocket accept")
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	}()

	if err := client.Connect(ctx); !errors.Is(err, ErrWSAlreadyConnected) {
		t.Fatalf("Connect duplicate error = %v, want %v", err, ErrWSAlreadyConnected)
	}
	if got := accepted.Load(); got != 1 {
		t.Fatalf("accepted connections = %d, want 1", got)
	}
}

func TestWSConnectWithTimeoutKeepsSuccessfulConnectionAlive(t *testing.T) {
	serverErr := make(chan error, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer closeTestWSConn(conn)
		_, raw, err := conn.Read(context.Background())
		if err != nil {
			serverErr <- err
			return
		}
		var req struct {
			ID   string   `json:"id"`
			Op   string   `json:"op"`
			Args []subArg `json:"args"`
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			serverErr <- err
			return
		}
		if req.Op != "subscribe" || len(req.Args) != 1 || req.Args[0].InstID != "BTC-USDT" {
			serverErr <- fmt.Errorf("unexpected subscribe request: %s", raw)
			return
		}
		if err := conn.Write(context.Background(), websocket.MessageText, wsTestAck(req.ID, "subscribe", "0", "", subArg{Channel: WSChannelTickers, InstID: "BTC-USDT"})); err != nil {
			serverErr <- err
			return
		}
		ctx := conn.CloseRead(context.Background())
		<-ctx.Done()
	}))
	defer server.Close()

	client := NewWSClient(Public, WithWSURL(websocketTestURL(server)))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.ConnectWithTimeout(ctx, time.Second); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	}()

	if err := client.Subscribe(ctx, WSChannelTickers, "BTC-USDT", func(Message) {}); err != nil {
		t.Fatalf("Subscribe after ConnectWithTimeout = %v", err)
	}
	select {
	case err := <-serverErr:
		t.Fatal(err)
	default:
	}
}

func TestWSConnectWithTimeoutTimesOutInitialHandshake(t *testing.T) {
	client := NewWSClient(Public)
	client.dialFn = func(ctx context.Context, _ string) (*websocket.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	err := client.ConnectWithTimeout(ctx, 10*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ConnectWithTimeout error = %v, want deadline exceeded", err)
	}
}

func TestWSReconnectResubscribes(t *testing.T) {
	var accepted atomic.Int32
	var subscribed atomic.Int32
	resubscribed := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		connID := accepted.Add(1)
		for {
			_, raw, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			var req struct {
				ID   string   `json:"id"`
				Op   string   `json:"op"`
				Args []subArg `json:"args"`
			}
			if err := json.Unmarshal(raw, &req); err != nil {
				return
			}
			if req.Op != "subscribe" || len(req.Args) != 1 {
				continue
			}
			if err := conn.Write(context.Background(), websocket.MessageText, wsTestAck(req.ID, "subscribe", "0", "", subArg{Channel: WSChannelTickers, InstID: "BTC-USDT"})); err != nil {
				return
			}
			count := subscribed.Add(1)
			if connID == 1 {
				_ = conn.Close(websocket.StatusGoingAway, "force reconnect")
				return
			}
			if count == 2 {
				close(resubscribed)
				ctx := conn.CloseRead(context.Background())
				<-ctx.Done()
				_ = conn.Close(websocket.StatusNormalClosure, "")
				return
			}
		}
	}))
	defer server.Close()

	client := NewWSClient(Public,
		WithWSURL(websocketTestURL(server)),
		WithWSReconnectConfig(WSReconnectConfig{Enabled: true, MinDelay: 10 * time.Millisecond, MaxDelay: 10 * time.Millisecond}),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	}()

	if err := client.Subscribe(ctx, WSChannelTickers, "BTC-USDT", func(Message) {}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-resubscribed:
	case <-ctx.Done():
		t.Fatal("timed out waiting for resubscribe")
	}
	for client.Reconnects() == 0 {
		select {
		case event := <-client.SystemEvents():
			if event.Event == "ready" && client.Reconnects() > 0 {
				break
			}
		case <-ctx.Done():
			t.Fatal("expected acknowledged reconnect")
		}
	}
}

func TestWSReconnectCanBeDisabled(t *testing.T) {
	var accepted atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		accepted.Add(1)
		time.Sleep(10 * time.Millisecond)
		_ = conn.Close(websocket.StatusGoingAway, "no reconnect")
	}))
	defer server.Close()

	client := NewWSClient(Public, WithWSURL(websocketTestURL(server)), WithWSReconnect(false))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	}()

	time.Sleep(150 * time.Millisecond)
	if got := accepted.Load(); got != 1 {
		t.Fatalf("accepted connections = %d, want 1", got)
	}
	if got := client.Reconnects(); got != 0 {
		t.Fatalf("reconnect count = %d, want 0", got)
	}
}

func TestWSPrivateConnectLogsInAndReconnectRelogins(t *testing.T) {
	var loggedIn atomic.Int32
	relogged := make(chan struct{})
	firstReady := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		for {
			_, raw, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			var req struct {
				ID   string `json:"id"`
				Op   string `json:"op"`
				Args []struct {
					APIKey     string `json:"apiKey"`
					Passphrase string `json:"passphrase"`
					Timestamp  string `json:"timestamp"`
					Sign       string `json:"sign"`
				} `json:"args"`
			}
			if err := json.Unmarshal(raw, &req); err != nil {
				return
			}
			if req.Op != "login" {
				continue
			}
			if len(req.Args) != 1 || req.Args[0].APIKey != "key" || req.Args[0].Passphrase != "pass" || req.Args[0].Timestamp == "" || req.Args[0].Sign == "" {
				return
			}
			if err := conn.Write(context.Background(), websocket.MessageText, wsTestAck(req.ID, "login", "0", "", subArg{})); err != nil {
				return
			}
			count := loggedIn.Add(1)
			if count == 1 {
				select {
				case <-firstReady:
				case <-t.Context().Done():
					return
				}
				_ = conn.Close(websocket.StatusGoingAway, "force reconnect")
				return
			}
			if count == 2 {
				close(relogged)
				ctx := conn.CloseRead(context.Background())
				<-ctx.Done()
				_ = conn.Close(websocket.StatusNormalClosure, "")
				return
			}
		}
	}))
	defer server.Close()

	client := NewWSClient(Private,
		WithWSURL(websocketTestURL(server)),
		WithWSCredentials("key", "secret", "pass"),
		WithWSReconnectConfig(WSReconnectConfig{Enabled: true, MinDelay: 10 * time.Millisecond, MaxDelay: 10 * time.Millisecond}),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	close(firstReady)
	defer func() {
		if err := client.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	}()

	select {
	case <-relogged:
	case <-ctx.Done():
		t.Fatal("timed out waiting for relogin")
	}
	for client.Reconnects() == 0 {
		select {
		case event := <-client.SystemEvents():
			if event.Event == "ready" && client.Reconnects() > 0 {
				break
			}
		case <-ctx.Done():
			t.Fatal("expected acknowledged reconnect")
		}
	}
}

func TestWSPrivateConnectLoginErrorSupportsAs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer closeTestWSConn(conn)
		_, raw, err := conn.Read(context.Background())
		if err != nil {
			return
		}
		var req struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			return
		}
		_ = conn.Write(context.Background(), websocket.MessageText, wsTestAck(req.ID, "login", "60009", "Login failed", subArg{}))
	}))
	defer server.Close()

	client := NewWSClient(Private, WithWSURL(websocketTestURL(server)), WithWSCredentials("key", "secret", "pass"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := client.Connect(ctx)
	if err == nil {
		t.Fatal("expected login error")
	}
	if !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("errors.Is unauthorized = false, err=%v", err)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "60009" {
		t.Fatalf("unexpected APIError: %#v err=%v", apiErr, err)
	}
}

func TestWSExplicitLoginOnConnectedClient(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		_, raw, err := conn.Read(context.Background())
		if err != nil {
			return
		}
		var req struct {
			ID   string `json:"id"`
			Op   string `json:"op"`
			Args []struct {
				APIKey     string `json:"apiKey"`
				Passphrase string `json:"passphrase"`
				Timestamp  string `json:"timestamp"`
				Sign       string `json:"sign"`
			} `json:"args"`
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			return
		}
		if req.Op != "login" || len(req.Args) != 1 || req.Args[0].APIKey != "key" || req.Args[0].Passphrase != "pass" || req.Args[0].Timestamp == "" || req.Args[0].Sign == "" {
			return
		}
		if err := conn.Write(context.Background(), websocket.MessageText, wsTestAck(req.ID, "login", "0", "", subArg{})); err != nil {
			return
		}
		ctx := conn.CloseRead(context.Background())
		<-ctx.Done()
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}))
	defer server.Close()

	client := NewWSClient(Public, WithWSURL(websocketTestURL(server)), WithWSCredentials("key", "secret", "pass"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	}()

	if err := client.Login(context.TODO()); err != nil {
		t.Fatal(err)
	}
}

func TestWSLoginBeforeConnectReturnsNotConnectedAndCleansPending(t *testing.T) {
	client := NewWSClient(Public, WithWSCredentials("key", "secret", "pass"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := client.Login(ctx); !errors.Is(err, ErrWSNotConnected) {
		t.Fatalf("Login error = %v, want %v", err, ErrWSNotConnected)
	}
	if client.session != nil {
		t.Fatal("unexpected session before Connect")
	}

}

func TestWSDispatchesOperationByIDAndOp(t *testing.T) {
	client := NewWSClient(Private)
	respCh := make(chan WSOperationResponse, 1)
	session := newWSSession(t.Context(), nil)
	defer session.cancel(context.Canceled)
	session.pending[1] = &wsPending{op: "order", done: respCh}
	var in wsIncoming
	if err := client.dispatchSession(session, []byte(`{"id":"1","op":"order","code":"0","msg":"","data":[{"ordId":"10","clOrdId":"c1","sCode":"0","sMsg":""}],"inTime":"1","outTime":"2"}`), &in); err != nil {
		t.Fatal(err)
	}

	select {
	case resp := <-respCh:
		if resp.ID != "1" || resp.Op != "order" || resp.InTime != "1" || resp.OutTime != "2" {
			t.Fatalf("unexpected response: %+v", resp)
		}
		var acks []OrderAck
		if err := resp.DecodeData(&acks); err != nil {
			t.Fatal(err)
		}
		if len(acks) != 1 || acks[0].OrdID != "10" || acks[0].SCode != "0" {
			t.Fatalf("unexpected acks: %+v", acks)
		}
	default:
		t.Fatal("operation response was not dispatched")
	}
}

func TestWSSubscribeNonZeroAckReturnsAPIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer closeTestWSConn(conn)
		_, raw, err := conn.Read(context.Background())
		if err != nil {
			return
		}
		var req struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			return
		}
		_ = conn.Write(context.Background(), websocket.MessageText, wsTestAck(req.ID, "subscribe", "60018", "Wrong URL or channel", subArg{Channel: WSChannelTickers, InstID: "BTC-USDT"}))
		ctx := conn.CloseRead(context.Background())
		<-ctx.Done()
	}))
	defer server.Close()

	client := NewWSClient(Public, WithWSURL(websocketTestURL(server)))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	}()

	err := client.Subscribe(ctx, WSChannelTickers, "BTC-USDT", func(Message) {})
	if err == nil {
		t.Fatal("expected subscribe error")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "60018" {
		t.Fatalf("unexpected APIError: %#v err=%v", apiErr, err)
	}
	handlers := *client.handlers.Load()
	if _, ok := handlers[(subArg{Channel: WSChannelTickers, InstID: "BTC-USDT"}).key()]; ok {
		t.Fatal("subscription handler was not cleaned up after failed subscribe")
	}
}

func TestWSUnsubscribeWaitsForAckAndRemovesHandler(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		for {
			_, raw, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			var req struct {
				ID   string   `json:"id"`
				Op   string   `json:"op"`
				Args []subArg `json:"args"`
			}
			if err := json.Unmarshal(raw, &req); err != nil || len(req.Args) != 1 {
				return
			}
			switch req.Op {
			case "subscribe":
				if err := conn.Write(context.Background(), websocket.MessageText, wsTestAck(req.ID, "subscribe", "0", "", subArg{Channel: WSChannelTickers, InstID: "BTC-USDT"})); err != nil {
					return
				}
			case "unsubscribe":
				if err := conn.Write(context.Background(), websocket.MessageText, wsTestAck(req.ID, "unsubscribe", "0", "", subArg{Channel: WSChannelTickers, InstID: "BTC-USDT"})); err != nil {
					return
				}
				ctx := conn.CloseRead(context.Background())
				<-ctx.Done()
				_ = conn.Close(websocket.StatusNormalClosure, "")
				return
			default:
				return
			}
		}
	}))
	defer server.Close()

	client := NewWSClient(Public, WithWSURL(websocketTestURL(server)))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	}()

	arg := WSChannelArg{Channel: WSChannelTickers, InstID: "BTC-USDT"}
	if err := client.SubscribeArg(ctx, arg, func(Message) {}); err != nil {
		t.Fatal(err)
	}
	if err := client.UnsubscribeArg(ctx, arg); err != nil {
		t.Fatal(err)
	}
	handlers := *client.handlers.Load()
	if _, ok := handlers[arg.subArg().key()]; ok {
		t.Fatal("handler remained after successful unsubscribe")
	}
}

func TestWSUnsubscribeErrorPreservesHandler(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		for {
			_, raw, err := conn.Read(context.Background())
			if err != nil {
				return
			}
			var req struct {
				ID   string   `json:"id"`
				Op   string   `json:"op"`
				Args []subArg `json:"args"`
			}
			if err := json.Unmarshal(raw, &req); err != nil || len(req.Args) != 1 {
				return
			}
			switch req.Op {
			case "subscribe":
				if err := conn.Write(context.Background(), websocket.MessageText, wsTestAck(req.ID, "subscribe", "0", "", subArg{Channel: WSChannelTickers, InstID: "BTC-USDT"})); err != nil {
					return
				}
			case "unsubscribe":
				if err := conn.Write(context.Background(), websocket.MessageText, wsTestAck(req.ID, "unsubscribe", "60018", "Wrong URL or channel", subArg{Channel: WSChannelTickers, InstID: "BTC-USDT"})); err != nil {
					return
				}
				ctx := conn.CloseRead(context.Background())
				<-ctx.Done()
				_ = conn.Close(websocket.StatusNormalClosure, "")
				return
			default:
				return
			}
		}
	}))
	defer server.Close()

	client := NewWSClient(Public, WithWSURL(websocketTestURL(server)))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	}()

	arg := WSChannelArg{Channel: WSChannelTickers, InstID: "BTC-USDT"}
	if err := client.SubscribeArg(ctx, arg, func(Message) {}); err != nil {
		t.Fatal(err)
	}
	err := client.UnsubscribeArg(ctx, arg)
	if err == nil {
		t.Fatal("expected unsubscribe error")
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "60018" {
		t.Fatalf("unexpected APIError: %#v err=%v", apiErr, err)
	}
	handlers := *client.handlers.Load()
	if _, ok := handlers[arg.subArg().key()]; !ok {
		t.Fatal("handler was removed after failed unsubscribe")
	}
}

func TestWSTradeOperationMatchesResponseByIDAndOpRealConn(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		_, raw, err := conn.Read(context.Background())
		if err != nil {
			return
		}
		var req struct {
			ID   string              `json:"id"`
			Op   string              `json:"op"`
			Args []PlaceOrderRequest `json:"args"`
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			return
		}
		if req.ID == "" || req.Op != "order" || len(req.Args) != 1 || req.Args[0].InstID != "" || req.Args[0].InstIDCode != 123456 {
			return
		}
		resp, err := json.Marshal(map[string]any{
			"id":   req.ID,
			"op":   req.Op,
			"code": "0",
			"msg":  "",
			"data": []map[string]string{{"ordId": "1", "clOrdId": "slot-1", "sCode": "0", "sMsg": ""}},
		})
		if err != nil {
			return
		}
		if err := conn.Write(context.Background(), websocket.MessageText, resp); err != nil {
			return
		}
		ctx := conn.CloseRead(context.Background())
		<-ctx.Done()
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}))
	defer server.Close()

	catalog := &InstrumentCatalog{}
	if err := catalog.Update([]Instrument{{InstID: "BTC-USDT", InstIDCode: 123456}}); err != nil {
		t.Fatal(err)
	}
	client := NewWSClient(Public, WithWSURL(websocketTestURL(server)), WithWSInstrumentCatalog(catalog))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	}()

	acks, err := client.PlaceOrder(ctx, PlaceOrderRequest{
		InstID:     "BTC-USDT",
		InstIDCode: 123456,
		TdMode:     "cash",
		Side:       "buy",
		OrdType:    "limit",
		Sz:         "1",
		Px:         "100",
		ClOrdID:    "slot-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(acks) != 1 || acks[0].OrdID != "1" || acks[0].ClOrdID != "slot-1" || acks[0].SCode != "0" {
		t.Fatalf("unexpected acks: %+v", acks)
	}
}

func TestWSBatchTradeOperationPreservesPartialSuccessRowsRealConn(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		_, raw, err := conn.Read(context.Background())
		if err != nil {
			return
		}
		var req struct {
			ID   string              `json:"id"`
			Op   string              `json:"op"`
			Args []PlaceOrderRequest `json:"args"`
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			return
		}
		if req.ID == "" || req.Op != "batch-orders" || len(req.Args) != 2 {
			return
		}
		resp, err := json.Marshal(map[string]any{
			"id":   req.ID,
			"op":   req.Op,
			"code": "2",
			"msg":  "Bulk operation partially successful",
			"data": []map[string]string{
				{"ordId": "1", "clOrdId": "slot-1", "sCode": "0", "sMsg": ""},
				{"ordId": "", "clOrdId": "slot-2", "sCode": "51008", "sMsg": "Insufficient balance"},
			},
		})
		if err != nil {
			return
		}
		if err := conn.Write(context.Background(), websocket.MessageText, resp); err != nil {
			return
		}
		ctx := conn.CloseRead(context.Background())
		<-ctx.Done()
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}))
	defer server.Close()

	catalog := &InstrumentCatalog{}
	if err := catalog.Update([]Instrument{{InstID: "BTC-USDT", InstIDCode: 123456}}); err != nil {
		t.Fatal(err)
	}
	client := NewWSClient(Public, WithWSURL(websocketTestURL(server)), WithWSInstrumentCatalog(catalog))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	}()

	acks, err := client.PlaceMultipleOrders(ctx, []PlaceOrderRequest{
		{InstID: "BTC-USDT", InstIDCode: 123456, TdMode: "cash", Side: "buy", OrdType: "limit", Sz: "1", Px: "100", ClOrdID: "slot-1"},
		{InstID: "BTC-USDT", InstIDCode: 123456, TdMode: "cash", Side: "buy", OrdType: "limit", Sz: "1", Px: "99", ClOrdID: "slot-2"},
	})
	if err == nil {
		t.Fatal("expected partial success error")
	}
	var partial *PartialError
	if !errors.As(err, &partial) {
		t.Fatalf("expected PartialError, got %v", err)
	}
	if len(acks) != 2 || acks[0].SCode != "0" || acks[1].SCode != "51008" || acks[1].ClOrdID != "slot-2" {
		t.Fatalf("unexpected acks: %+v", acks)
	}
}

func TestWSTradeOperationErrorSupportsAsRealConn(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		_, raw, err := conn.Read(context.Background())
		if err != nil {
			return
		}
		var req struct {
			ID string `json:"id"`
			Op string `json:"op"`
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			return
		}
		resp, err := json.Marshal(map[string]string{
			"id":   req.ID,
			"op":   req.Op,
			"code": "50011",
			"msg":  "Rate limit reached",
		})
		if err != nil {
			return
		}
		if err := conn.Write(context.Background(), websocket.MessageText, resp); err != nil {
			return
		}
		ctx := conn.CloseRead(context.Background())
		<-ctx.Done()
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}))
	defer server.Close()

	client := NewWSClient(Public, WithWSURL(websocketTestURL(server)), WithWSInstrumentCatalog(testTradingCatalog(t)))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			t.Logf("close websocket: %v", err)
		}
	}()

	_, err := client.AmendOrder(ctx, AmendOrderRequest{InstID: "BTC-USDT", OrdID: "1", NewPx: "101"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("errors.Is rate limited = false, err=%v", err)
	}
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "50011" {
		t.Fatalf("unexpected APIError: %#v err=%v", apiErr, err)
	}
}

func TestWSOperationValidationBeforeConnection(t *testing.T) {
	client := NewWSClient(Private)
	if _, err := client.PlaceOrder(context.Background(), PlaceOrderRequest{}); err == nil {
		t.Fatal("expected validation error")
	}
	if _, err := client.CancelOrder(context.Background(), CancelOrderRequest{
		InstID: "BTC-USDT",
	}); err == nil {
		t.Fatal("expected missing order id error")
	}
}

func TestWSMethodsBeforeConnectReturnNotConnectedAndCleanUp(t *testing.T) {
	client := NewWSClient(Public)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, err := client.Do(ctx, "order", PlaceOrderRequest{}); !errors.Is(err, ErrWSNotConnected) {
		t.Fatalf("Do error = %v, want %v", err, ErrWSNotConnected)
	}
	if client.session != nil {
		t.Fatal("unexpected session before Connect")
	}

	arg := WSChannelArg{Channel: WSChannelTickers, InstID: "BTC-USDT"}
	if err := client.SubscribeArg(ctx, arg, func(Message) {}); !errors.Is(err, ErrWSNotConnected) {
		t.Fatalf("SubscribeArg error = %v, want %v", err, ErrWSNotConnected)
	}
	handlers := *client.handlers.Load()
	if _, ok := handlers[arg.subArg().key()]; ok {
		t.Fatal("subscription handler was not cleaned up after not connected error")
	}
}

func TestWSConnectionLossFailsPendingControlPlane(t *testing.T) {
	session := newWSSession(t.Context(), nil)
	defer session.cancel(context.Canceled)
	ack := make(chan WSOperationResponse, 1)
	op := make(chan WSOperationResponse, 1)
	session.pending[1] = &wsPending{op: "login", control: true, done: ack}
	session.pending[2] = &wsPending{op: "order", done: op}
	session.fail(ErrWSConnectionLost)
	for _, ch := range []chan WSOperationResponse{ack, op} {
		select {
		case r := <-ch:
			if !errors.Is(r.Err, ErrWSConnectionLost) {
				t.Fatal(r.Err)
			}
		default:
			t.Fatal("pending request not completed")
		}
	}
	if len(session.pending) != 0 {
		t.Fatal("pending entries retained")
	}
}

func TestWSControlAckDispatchesByRequestID(t *testing.T) {
	client := NewWSClient(Public)
	session := newWSSession(t.Context(), nil)
	defer session.cancel(context.Canceled)
	ack1 := make(chan WSOperationResponse, 1)
	ack2 := make(chan WSOperationResponse, 1)
	session.pending[1] = &wsPending{op: "subscribe", control: true, done: ack1}
	session.pending[2] = &wsPending{op: "subscribe", control: true, done: ack2}
	var in wsIncoming
	if err := client.dispatchSession(session, []byte(`{"id":"2","event":"subscribe","arg":{"channel":"tickers","instId":"BTC-USDT"}}`), &in); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-ack2:
		if r.Err != nil {
			t.Fatal(r.Err)
		}
	default:
		t.Fatal("ACK not delivered")
	}
	select {
	case <-ack1:
		t.Fatal("wrong request completed")
	default:
	}
	if err := client.dispatchSession(session, []byte(`{"id":"1","event":"error","code":"60012","msg":"Invalid request"}`), &in); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-ack1:
		var e *APIError
		if !errors.As(r.Err, &e) || e.Code != "60012" {
			t.Fatal(r.Err)
		}
	default:
		t.Fatal("error without arg not delivered")
	}
}

func TestSubscriptionKeyIncludesStructuredFields(t *testing.T) {
	a := subArg{Channel: WSChannelOrders, InstType: string(InstAny)}
	b := subArg{Channel: WSChannelOrders, InstID: "BTC-USDT"}
	if a.key() == b.key() {
		t.Fatalf("subscription keys collided: %q", a.key())
	}
}

func wsTestAck(id, event, code, msg string, arg subArg) []byte {
	m := map[string]any{
		"event": event,
		"code":  code,
	}
	if id != "" {
		m["id"] = id
	}
	if msg != "" {
		m["msg"] = msg
	}
	if arg.Channel != "" {
		m["arg"] = arg
	}
	b, _ := json.Marshal(m)
	return b
}

func websocketTestURL(server *httptest.Server) string {
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func closeTestWSConn(conn *websocket.Conn) {
	_ = conn.Close(websocket.StatusNormalClosure, "")
}
