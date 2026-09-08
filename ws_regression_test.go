package okx

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	json "github.com/go-json-experiment/json"
)

type regressionWSRequest struct {
	ID   string   `json:"id"`
	Op   string   `json:"op"`
	Args []subArg `json:"args"`
}

func regressionWSServer(t *testing.T, serve func(context.Context, *websocket.Conn)) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		serve(t.Context(), conn)
	}))
	t.Cleanup(server.Close)
	return server
}

func readRegressionRequest(ctx context.Context, conn *websocket.Conn) (regressionWSRequest, error) {
	_, raw, err := conn.Read(ctx)
	var req regressionWSRequest
	if err == nil {
		err = json.Unmarshal(raw, &req)
	}
	return req, err
}

func TestRegressionDuplicateSubscribeAndErrorWithoutArg(t *testing.T) {
	received := make(chan regressionWSRequest, 1)
	release := make(chan struct{})
	server := regressionWSServer(t, func(ctx context.Context, conn *websocket.Conn) {
		req, err := readRegressionRequest(ctx, conn)
		if err != nil {
			return
		}
		received <- req
		select {
		case <-release:
		case <-ctx.Done():
			return
		}
		raw, _ := json.Marshal(map[string]string{"id": req.ID, "event": "error", "code": "60012", "msg": "Invalid request"})
		_ = conn.Write(ctx, websocket.MessageText, raw)
		_, _, _ = conn.Read(ctx)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	c := NewWSClient(Public, WithWSURL(websocketTestURL(server)), WithWSRateLimiter(nil))
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	result := make(chan error, 1)
	// This control task is canceled and joined on every exit path.
	opCtx, stop := context.WithCancel(ctx)
	go func() {
		_, err := c.SubscribeArgHandle(opCtx, WSChannelArg{Channel: "books", InstID: "BTC-USDT"}, func(Message) {})
		result <- err
	}()
	joined := false
	defer func() {
		stop()
		if !joined {
			<-result
		}
	}()
	select {
	case <-received:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	old := (*c.handlers.Load())[subArg{Channel: "books", InstID: "BTC-USDT"}.key()]
	_, err := c.SubscribeArgHandle(ctx, WSChannelArg{Channel: "books", InstID: "BTC-USDT"}, func(Message) {})
	if !errors.Is(err, ErrWSConcurrentOperation) {
		t.Fatalf("duplicate: %v", err)
	}
	if (*c.handlers.Load())[old.arg.key()] != old {
		t.Fatal("rejected duplicate removed original handler")
	}
	close(release)
	select {
	case err = <-result:
		joined = true
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !errors.Is(err, ErrInvalidParameter) {
		t.Fatalf("error without arg: %v", err)
	}
	if len(*c.handlers.Load()) != 0 {
		t.Fatal("failed subscription retained handler")
	}
}

func TestRegressionLateACKAndOldSessionCannotCompleteNewRequest(t *testing.T) {
	old := newWSSession(t.Context(), nil)
	defer old.cancel(context.Canceled)
	current := newWSSession(t.Context(), nil)
	defer current.cancel(context.Canceled)
	timed := &wsPending{op: "subscribe", control: true, done: make(chan WSOperationResponse, 1)}
	old.pending[1] = timed
	old.finish(1, WSOperationResponse{Err: context.DeadlineExceeded})
	next := &wsPending{op: "subscribe", control: true, done: make(chan WSOperationResponse, 1)}
	old.pending[2] = next
	if old.deliver(&wsIncoming{ID: "1", Event: "subscribe"}) {
		t.Fatal("late ACK consumed new waiter")
	}
	current.pending[2] = &wsPending{op: "subscribe", control: true, done: make(chan WSOperationResponse, 1)}
	old.fail(ErrWSConnectionLost)
	if len(current.pending) != 1 {
		t.Fatal("old cleanup completed new session waiter")
	}
	if current.deliver(&wsIncoming{ID: "2", Event: "unsubscribe"}) {
		t.Fatal("wrong operation matched")
	}
	if !current.deliver(&wsIncoming{ID: "2", Event: "subscribe"}) {
		t.Fatal("matching ACK missed")
	}
}

func TestRegressionBookGapReconnectsAndCallbackCanRequestClose(t *testing.T) {
	var connections atomic.Int32
	server := regressionWSServer(t, func(ctx context.Context, conn *websocket.Conn) {
		number := connections.Add(1)
		req, err := readRegressionRequest(ctx, conn)
		if err != nil {
			return
		}
		if err = conn.Write(ctx, websocket.MessageText, wsTestAck(req.ID, req.Op, "0", "", req.Args[0])); err != nil {
			return
		}
		frame := `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"seqId":10,"prevSeqId":-1,"bids":[["100","1","0","1"]],"asks":[["101","1","0","1"]]}]}`
		if number > 1 {
			frame = `{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"seqId":20,"prevSeqId":-1,"bids":[["200","1","0","1"]],"asks":[["201","1","0","1"]]}]}`
		}
		if err = conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
			return
		}
		if number == 1 {
			_ = conn.Write(ctx, websocket.MessageText, []byte(`{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"seqId":12,"prevSeqId":11,"bids":[],"asks":[]}]}`))
		}
		_, _, _ = conn.Read(ctx)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	c := NewWSClient(Public, WithWSURL(websocketTestURL(server)), WithWSRateLimiter(nil), WithWSReconnectConfig(WSReconnectConfig{Enabled: true, MinDelay: time.Millisecond, MaxDelay: time.Millisecond, MaxAttempts: 2}))
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var sawGap atomic.Bool
	restored := make(chan string, 1)
	_, err := c.SubscribeLocalOrderBookHandle(ctx, "books", "BTC-USDT", 5, func(msg WSLocalOrderBookMessage) {
		if errors.Is(msg.Err, ErrOrderBookSequenceGap) {
			sawGap.Store(true)
		}
		if msg.Book != nil && msg.Book.Ready() && msg.Book.SeqID() == 20 {
			restored <- msg.Book.Bids()[0].Price()
			c.RequestClose()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case price := <-restored:
		if price != "200" || !sawGap.Load() {
			t.Fatal("invalid recovered snapshot", price)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := c.WaitClosed(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRegressionRestoreRequiresACK(t *testing.T) {
	var connections atomic.Int32
	drop := make(chan struct{})
	restoring := make(chan struct{})
	server := regressionWSServer(t, func(ctx context.Context, conn *websocket.Conn) {
		n := connections.Add(1)
		req, err := readRegressionRequest(ctx, conn)
		if err != nil {
			return
		}
		if n == 1 {
			_ = conn.Write(ctx, websocket.MessageText, wsTestAck(req.ID, req.Op, "0", "", req.Args[0]))
			select {
			case <-drop:
			case <-ctx.Done():
				return
			}
			return
		}
		if n == 2 {
			close(restoring)
		}
		_, _, _ = conn.Read(ctx)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	c := NewWSClient(Public, WithWSURL(websocketTestURL(server)), WithWSRateLimiter(nil), WithWSRequestTimeout(40*time.Millisecond), WithWSReconnectConfig(WSReconnectConfig{Enabled: true, MinDelay: time.Millisecond, MaxDelay: time.Millisecond, MaxAttempts: 1}))
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Subscribe(ctx, "books", "BTC-USDT", func(Message) {}); err != nil {
		t.Fatal(err)
	}
	close(drop)
	select {
	case <-restoring:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if c.Ready() || c.Reconnects() != 0 {
		t.Fatal("unacknowledged restore reported ready")
	}
	if err := c.WaitClosed(ctx); err != nil {
		t.Fatal(err)
	}
	if c.Reconnects() != 0 {
		t.Fatal("failed restore counted as successful reconnect")
	}
}

func TestRegressionWriteAdmissionHonorsCancellation(t *testing.T) {
	server := regressionWSServer(t, func(ctx context.Context, conn *websocket.Conn) { _, _, _ = conn.Read(ctx) })
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	c := NewWSClient(Public, WithWSURL(websocketTestURL(server)), WithWSRateLimiter(nil))
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	session, err := c.getSession(true)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := session.conn.Writer(ctx, websocket.MessageText)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	blocked, stop := context.WithTimeout(ctx, 20*time.Millisecond)
	defer stop()
	err = c.writeSession(blocked, session, opMessage{Op: "subscribe", Args: []any{subArg{Channel: "books", InstID: "BTC-USDT"}}})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("write admission: %v", err)
	}
}

func TestRegressionConnectTimeoutRetainsRunUntilJoined(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	c := NewWSClient(Public, WithWSRateLimiter(nil))
	c.dialFn = func(ctx context.Context, _ string) (*websocket.Conn, error) {
		close(entered)
		<-ctx.Done()
		<-release
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	err := c.ConnectWithTimeout(ctx, 20*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Connect: %v", err)
	}
	<-entered
	if err = c.Connect(ctx); !errors.Is(err, ErrWSAlreadyConnected) {
		t.Fatalf("overlapping run: %v", err)
	}
	close(release)
	if err = c.WaitClosed(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRegressionAccountMetadataAndTypedCurrencyFilter(t *testing.T) {
	raw, err := os.ReadFile("testdata/contracts/trading-account-websocket-account-channel-7.json")
	if err != nil {
		t.Fatal(err)
	}
	server := regressionWSServer(t, func(ctx context.Context, conn *websocket.Conn) {
		req, err := readRegressionRequest(ctx, conn)
		if err != nil {
			return
		}
		if err = conn.Write(ctx, websocket.MessageText, wsTestAck(req.ID, req.Op, "0", "", req.Args[0])); err != nil {
			return
		}
		_ = conn.Write(ctx, websocket.MessageText, raw)
		_, _, _ = conn.Read(ctx)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	c := NewWSClient(Public, WithWSURL(websocketTestURL(server)), WithWSRateLimiter(nil))
	if err = c.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	received := make(chan WSTypedMessage[AccountUpdate], 1)
	_, err = c.SubscribeAccountHandle(ctx, AccountSubscriptionRequest{Ccy: "USDT"}, func(msg WSTypedMessage[AccountUpdate]) { received <- msg; c.RequestClose() })
	if err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-received:
		if msg.Err != nil || msg.Arg.UID == "" || msg.EventType != "snapshot" || msg.CurPage != 1 || !msg.LastPage || len(msg.Data) != 1 || len(msg.Data[0].Details) != 1 || msg.Data[0].Details[0].Ccy != "USDT" {
			t.Fatalf("account metadata/filter: %+v", msg)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err = c.WaitClosed(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRegressionOrderFiltersRespectFamilyAndInstrument(t *testing.T) {
	rows := []Order{{InstID: "BTC-USDT-SWAP", InstType: InstSwap}, {InstID: "ETH-USDT-SWAP", InstType: InstSwap}, {InstID: "BTC-USDT", InstType: InstSpot}}
	got := filterPrivateRows(WSChannelArg{Channel: "orders", InstType: "SWAP", InstFamily: "BTC-USDT"}, rows)
	if len(got) != 1 || got[0].InstID != "BTC-USDT-SWAP" {
		t.Fatalf("family filter: %+v", got)
	}
	got = filterPrivateRows(WSChannelArg{Channel: "orders", InstType: "ANY", InstID: "ETH-USDT-SWAP"}, rows)
	if len(got) != 1 || got[0].InstID != "ETH-USDT-SWAP" {
		t.Fatalf("instrument filter: %+v", got)
	}
}

func TestRegressionOldHandleCannotRemoveNewSubscription(t *testing.T) {
	c := NewWSClient(Public, WithWSRateLimiter(nil))
	arg := WSChannelArg{Channel: "books", InstID: "BTC-USDT"}
	old := &subEntry{arg: arg.subArg()}
	current := &subEntry{arg: arg.subArg()}
	entries := map[string]*subEntry{arg.subArg().key(): current}
	c.handlers.Store(&entries)
	handle := &WSSubscription{c: c, arg: arg, entry: old}
	if err := handle.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if (*c.handlers.Load())[arg.subArg().key()] != current {
		t.Fatal("old handle removed replacement")
	}
}
