package okx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestSubscriptionKeyStable(t *testing.T) {
	a := Subscription{Channel: "orders", Args: map[string]string{"instType": "SWAP", "instFamily": "BTC-USDT"}}
	b := Subscription{Channel: "orders", Args: map[string]string{"instFamily": "BTC-USDT", "instType": "SWAP"}}
	if a.key() != b.key() {
		t.Fatalf("keys differ: %q %q", a.key(), b.key())
	}
}

func TestWSDispatchesOrderBookDepthMessages(t *testing.T) {
	tests := []struct {
		name    string
		channel string
		levels  int
	}{
		{name: "best bid offer", channel: "bbo-tbt", levels: 1},
		{name: "five levels", channel: "books5", levels: 5},
		{name: "fifty levels", channel: "books50-l2-tbt", levels: 50},
		{name: "four hundred levels", channel: "books-l2-tbt", levels: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewWSClient()
			sub := Subscription{Channel: tt.channel, Args: map[string]string{"instId": "BTC-USDT"}}
			ch := make(chan WSMessage, 1)
			client.subs[sub.key()] = wsSubscriptionState{sub: sub.clone(), ch: ch}

			raw := marshalOrderBookWSMessage(t, tt.channel, tt.levels)
			client.handleRaw(raw)

			select {
			case msg := <-ch:
				if msg.Arg["channel"] != tt.channel {
					t.Fatalf("channel = %q, want %q", msg.Arg["channel"], tt.channel)
				}
				if string(msg.Raw) != string(raw) {
					t.Fatal("raw message was not forwarded unchanged")
				}
				books := decodeOrderBookData(t, msg.Data)
				if len(books) != 1 {
					t.Fatalf("data length = %d, want 1", len(books))
				}
				if len(books[0].Asks) != tt.levels {
					t.Fatalf("asks length = %d, want %d", len(books[0].Asks), tt.levels)
				}
				if len(books[0].Bids) != tt.levels {
					t.Fatalf("bids length = %d, want %d", len(books[0].Bids), tt.levels)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for dispatched order book message")
			}
		})
	}
}

func TestDecodeTypedWSMessage(t *testing.T) {
	msg := WSMessage{
		Arg:    map[string]string{"channel": "orders", "instType": "SPOT", "instId": "BTC-USDT"},
		Action: "snapshot",
		Data:   json.RawMessage(`[{"instType":"SPOT","instId":"BTC-USDT","ordId":"1","state":"filled","accFillSz":"0.1","avgPx":"100"}]`),
		Raw:    json.RawMessage(`{"data":[]}`),
	}
	got := decodeTypedWSMessage[Order](msg)
	if got.Err != nil {
		t.Fatal(got.Err)
	}
	if got.Action != "snapshot" {
		t.Fatalf("action = %q", got.Action)
	}
	if len(got.Data) != 1 || got.Data[0].OrdID != "1" || got.Data[0].State != "filled" {
		t.Fatalf("unexpected orders: %+v", got.Data)
	}
}

func TestDecodeTypedAccountWSMessage(t *testing.T) {
	msg := WSMessage{
		Arg:  map[string]string{"channel": "account", "ccy": "BTC"},
		Data: json.RawMessage(`[{"uTime":"1597026383085","totalEq":"10","details":[{"ccy":"BTC","availBal":"1","availEq":"2","uTime":"1597026383085"}]}]`),
		Raw:  json.RawMessage(`{"data":[]}`),
	}
	got := decodeTypedWSMessage[AccountUpdate](msg)
	if got.Err != nil {
		t.Fatal(got.Err)
	}
	if len(got.Data) != 1 || got.Data[0].Details[0].Ccy != "BTC" || got.Data[0].Details[0].AvailBal != "1" {
		t.Fatalf("unexpected account update: %+v", got.Data)
	}
}

func TestOrderBookChannelHelpers(t *testing.T) {
	tests := []struct {
		depth       int
		wantDepth   int
		wantChannel string
	}{
		{depth: 1, wantDepth: 1, wantChannel: "bbo-tbt"},
		{depth: 5, wantDepth: 5, wantChannel: "books5"},
		{depth: 20, wantDepth: 50, wantChannel: "books50-l2-tbt"},
		{depth: 400, wantDepth: 400, wantChannel: "books-l2-tbt"},
	}
	for _, tt := range tests {
		if got := OrderBookDepth(tt.depth); got != tt.wantDepth {
			t.Fatalf("OrderBookDepth(%d) = %d, want %d", tt.depth, got, tt.wantDepth)
		}
		if got := OrderBookChannel(tt.depth); got != tt.wantChannel {
			t.Fatalf("OrderBookChannel(%d) = %q, want %q", tt.depth, got, tt.wantChannel)
		}
		if got := OrderBookDepthFromChannel(tt.wantChannel); got != tt.wantDepth {
			t.Fatalf("OrderBookDepthFromChannel(%q) = %d, want %d", tt.wantChannel, got, tt.wantDepth)
		}
	}
	if got := OrderBookDepthFromChannel("books"); got != 400 {
		t.Fatalf("OrderBookDepthFromChannel(books) = %d, want 400", got)
	}
}

func TestWSConnectRejectsDuplicateConnection(t *testing.T) {
	var accepted atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		accepted.Add(1)
		ctx := conn.CloseRead(context.Background())
		<-ctx.Done()
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}))
	defer server.Close()

	client := NewWSClient(WithWSURL(websocketTestURL(server)))
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

	if err := client.Connect(ctx); !errors.Is(err, errWSAlreadyConnected) {
		t.Fatalf("Connect duplicate error = %v, want %v", err, errWSAlreadyConnected)
	}
	if got := accepted.Load(); got != 1 {
		t.Fatalf("accepted connections = %d, want 1", got)
	}
}

func TestWSMethodsBeforeConnectReturnNotConnected(t *testing.T) {
	client := NewWSClient()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if _, err := client.Do(ctx, "order"); !errors.Is(err, errWSNotConnected) {
		t.Fatalf("Do error = %v, want %v", err, errWSNotConnected)
	}
	if len(client.ops) != 0 {
		t.Fatalf("pending operations were not cleaned up: %d", len(client.ops))
	}

	sub := Subscription{Channel: "tickers", Args: map[string]string{"instId": "BTC-USDT"}}
	if _, err := client.Subscribe(ctx, sub); !errors.Is(err, errWSNotConnected) {
		t.Fatalf("Subscribe error = %v, want %v", err, errWSNotConnected)
	}
	if _, ok := client.subs[sub.key()]; ok {
		t.Fatal("subscription was not cleaned up after not connected error")
	}
}

func TestWSLoginNilContextUsesBackgroundContext(t *testing.T) {
	client := NewWSClient(WithWSCredentials("key", "secret", "pass"))
	markWSConnectedForTest(client)

	done := make(chan error, 1)
	go func() {
		item := <-client.writeCh
		item.errCh <- nil
		client.handleRaw([]byte(`{"event":"login","code":"0"}`))
	}()
	go func() {
		done <- client.Login(context.TODO())
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for nil-context login")
	}
}

func TestWSDispatchSubscriptionConcurrentCloseDoesNotPanic(t *testing.T) {
	sub := Subscription{Channel: "books5", Args: map[string]string{"instId": "BTC-USDT"}}
	raw := marshalOrderBookWSMessage(t, sub.Channel, 5)

	for range 100 {
		client := NewWSClient()
		ch := make(chan WSMessage, 1)
		client.subs[sub.key()] = wsSubscriptionState{sub: sub.clone(), ch: ch}

		done := make(chan struct{})
		go func() {
			defer close(done)
			client.handleRaw(raw)
		}()
		client.removeSubscription(sub.key(), true)

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for dispatch")
		}
	}
}

func TestWSTradeOperationMatchesResponseByIDAndOp(t *testing.T) {
	client := NewWSClient()
	markWSConnectedForTest(client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		item := <-client.writeCh
		var req struct {
			ID   string              `json:"id"`
			Op   string              `json:"op"`
			Args []PlaceOrderRequest `json:"args"`
		}
		if err := json.Unmarshal(item.payload, &req); err != nil {
			item.errCh <- err
			return
		}
		if req.ID == "" || req.Op != "order" || len(req.Args) != 1 || req.Args[0].InstID != "BTC-USDT" || req.Args[0].InstIDCode != 123456 {
			item.errCh <- fmt.Errorf("unexpected request: %+v", req)
			return
		}
		item.errCh <- nil
		raw, err := json.Marshal(map[string]any{
			"id":   req.ID,
			"op":   req.Op,
			"code": "0",
			"msg":  "",
			"data": []map[string]string{{"ordId": "1", "sCode": "0"}},
		})
		if err != nil {
			return
		}
		client.handleRaw(raw)
	}()

	acks, err := client.PlaceOrder(ctx, PlaceOrderRequest{
		InstID:     "BTC-USDT",
		InstIDCode: 123456,
		TdMode:     "cash",
		Side:       "buy",
		OrdType:    "limit",
		Sz:         "1",
		Px:         "100",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(acks) != 1 || acks[0].OrdID != "1" || acks[0].SCode != "0" {
		t.Fatalf("unexpected acks: %+v", acks)
	}
	<-done
}

func TestWSBatchTradeOperationPreservesPartialSuccessRows(t *testing.T) {
	client := NewWSClient()
	markWSConnectedForTest(client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		item := <-client.writeCh
		var req struct {
			ID   string              `json:"id"`
			Op   string              `json:"op"`
			Args []PlaceOrderRequest `json:"args"`
		}
		if err := json.Unmarshal(item.payload, &req); err != nil {
			item.errCh <- err
			return
		}
		if req.ID == "" || req.Op != "batch-orders" || len(req.Args) != 2 {
			item.errCh <- fmt.Errorf("unexpected request: %+v", req)
			return
		}
		item.errCh <- nil
		raw, err := json.Marshal(map[string]any{
			"id":   req.ID,
			"op":   req.Op,
			"code": "2",
			"msg":  "Bulk operation partially successful",
			"data": []map[string]string{
				{"ordId": "1", "clOrdId": "slot-1", "sCode": "0"},
				{"ordId": "", "clOrdId": "slot-2", "sCode": "51008", "sMsg": "Insufficient balance"},
			},
		})
		if err != nil {
			return
		}
		client.handleRaw(raw)
	}()

	acks, err := client.PlaceMultipleOrders(ctx, []PlaceOrderRequest{
		{InstID: "BTC-USDT", InstIDCode: 123456, TdMode: "cash", Side: "buy", OrdType: "limit", Sz: "1", Px: "100", ClOrdID: "slot-1"},
		{InstID: "BTC-USDT", InstIDCode: 123456, TdMode: "cash", Side: "buy", OrdType: "limit", Sz: "1", Px: "99", ClOrdID: "slot-2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(acks) != 2 || acks[0].SCode != "0" || acks[1].SCode != "51008" || acks[1].ClOrdID != "slot-2" {
		t.Fatalf("unexpected acks: %+v", acks)
	}
	<-done
}

func TestWSTradeOperationErrorSupportsAs(t *testing.T) {
	client := NewWSClient()
	markWSConnectedForTest(client)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	go func() {
		item := <-client.writeCh
		var req struct {
			ID string `json:"id"`
			Op string `json:"op"`
		}
		if err := json.Unmarshal(item.payload, &req); err != nil {
			item.errCh <- err
			return
		}
		item.errCh <- nil
		raw, _ := json.Marshal(map[string]string{
			"id":   req.ID,
			"op":   req.Op,
			"code": "50011",
			"msg":  "Rate limit reached",
		})
		client.handleRaw(raw)
	}()

	_, err := client.AmendOrder(ctx, AmendOrderRequest{InstID: "BTC-USDT", OrdID: "1", NewPx: "101"})
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("errors.Is rate limited = false, err=%v", err)
	}
	var okxErr *OKXError
	if !errors.As(err, &okxErr) || okxErr.Code != "50011" {
		t.Fatalf("unexpected OKXError: %#v err=%v", okxErr, err)
	}
}

func TestOrderBookUnmarshalNumericSequence(t *testing.T) {
	var books []OrderBook
	if err := json.Unmarshal([]byte(`[{"asks":[],"bids":[],"ts":"1597026383085","checksum":0,"prevSeqId":-1,"seqId":123456}]`), &books); err != nil {
		t.Fatal(err)
	}
	if books[0].Checksum.String() != "0" || books[0].PrevSeqID.String() != "-1" || books[0].SeqID.String() != "123456" {
		t.Fatalf("unexpected sequence fields: %+v", books[0])
	}
}

func TestWSErrorAckDispatchesToSubscriptionPendingKey(t *testing.T) {
	client := NewWSClient()
	sub := Subscription{Channel: "tickers", Args: map[string]string{"instId": "BTC-USDT"}}
	ackCh := make(chan WSAck, 1)
	client.pending["subscribe:"+sub.key()] = ackCh

	client.dispatchAck("error", WSAck{
		Event: "error",
		Code:  "60018",
		Msg:   "Wrong URL or channel",
		Arg:   sub.arg(),
	})

	select {
	case ack := <-ackCh:
		if ack.Code != "60018" {
			t.Fatalf("code = %q, want 60018", ack.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for subscription error ack")
	}
}

type orderBookData struct {
	Asks     [][]string `json:"asks"`
	Bids     [][]string `json:"bids"`
	TS       string     `json:"ts"`
	Checksum string     `json:"checksum,omitempty"`
}

func marshalOrderBookWSMessage(t *testing.T, channel string, levels int) []byte {
	t.Helper()

	raw, err := json.Marshal(map[string]any{
		"arg": map[string]string{
			"channel": channel,
			"instId":  "BTC-USDT",
		},
		"action": "snapshot",
		"data":   []orderBookData{makeOrderBookData(levels)},
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func makeOrderBookData(levels int) orderBookData {
	asks := make([][]string, levels)
	bids := make([][]string, levels)
	for i := range levels {
		asks[i] = []string{fmt.Sprintf("%d.1", 100+i), "1", "0", "1"}
		bids[i] = []string{fmt.Sprintf("%d.9", 99-i), "1", "0", "1"}
	}
	return orderBookData{
		Asks:     asks,
		Bids:     bids,
		TS:       "1597026383085",
		Checksum: "0",
	}
}

func decodeOrderBookData(t *testing.T, data json.RawMessage) []orderBookData {
	t.Helper()

	var books []orderBookData
	if err := json.Unmarshal(data, &books); err != nil {
		t.Fatalf("decode order book data: %v", err)
	}
	return books
}

func websocketTestURL(server *httptest.Server) string {
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func markWSConnectedForTest(client *WSClient) {
	client.conn = new(websocket.Conn)
}
