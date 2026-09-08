package okx

import (
	"context"
	"errors"
	"fmt"
	"github.com/coder/websocket"
	json "github.com/go-json-experiment/json"
	"github.com/shopspring/decimal"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"
)

func auditSnapshot() []byte {
	var s strings.Builder
	s.WriteString(`{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[`)
	for i := 0; i < 400; i++ {
		if i > 0 {
			s.WriteByte(',')
		}
		fmt.Fprintf(&s, `["%d.12345678","0.12345678","0","12"]`, 100001+i)
	}
	s.WriteString(`],"bids":[`)
	for i := 0; i < 400; i++ {
		if i > 0 {
			s.WriteByte(',')
		}
		fmt.Fprintf(&s, `["%d.12345678","0.12345678","0","12"]`, 99999-i)
	}
	s.WriteString(`],"ts":"1780000000000","seqId":100,"prevSeqId":-1}]}`)
	return []byte(s.String())
}

func TestRegressionDefaultDialReceives400LevelBook(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	data := auditSnapshot()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		if err = c.Write(ctx, websocket.MessageText, data); err != nil {
			return
		}
		_, _, _ = c.Read(ctx)
	}))
	defer server.Close()
	conn, err := defaultDial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	_, got, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("valid 400x2 book (%d bytes) rejected: %v", len(data), err)
	}
	if len(got) != len(data) {
		t.Fatal("truncated book")
	}
}

func TestRegressionBookGapInvalidatesReady(t *testing.T) {
	b := NewLocalOrderBook("BTC-USDT", 5)
	if err := b.Apply("snapshot", OrderBook{SeqID: "10", Bids: []BookLevel{{"100", "1", "0", "1"}}}); err != nil {
		t.Fatal(err)
	}
	err := b.Apply("update", OrderBook{PrevSeqID: "11", SeqID: "12"})
	if !errors.Is(err, ErrOrderBookSequenceGap) {
		t.Fatal(err)
	}
	if b.Ready() {
		t.Fatalf("book remains ready at stale sequence %d after gap", b.SeqID())
	}
}

func TestRegressionRuleRoundingPreservesDirection(t *testing.T) {
	r := InstrumentRules{TickSize: decimal.NewFromInt(1), LotSize: decimal.NewFromInt(1)}
	x := decimal.RequireFromString("0.999999999999999999")
	got := r.RoundSize(x)
	if got.GreaterThan(x) {
		t.Errorf("RoundSize(%s)=%s exceeds requested size", x, got)
	}
	price := decimal.RequireFromString("1.000000000000000001")
	sell, err := r.RoundPriceForSide(Sell, price)
	if err != nil {
		t.Fatal(err)
	}
	if sell.LessThan(price) {
		t.Errorf("sell RoundPriceForSide(%s)=%s is below requested price", price, sell)
	}
}

func TestRegressionRulesRejectDifferentInstrument(t *testing.T) {
	r, err := NewInstrumentRules(Instrument{InstID: "BTC-USDT", TickSz: "0.1", LotSz: "0.01", MinSz: "0.01"})
	if err != nil {
		t.Fatal(err)
	}
	_, err = r.PreparePlaceOrder(PlaceOrderRequest{InstID: "ETH-USDT", TdMode: TdCash, Side: Buy, OrdType: OrdLimit, Px: "100.19", Sz: "0.019"})
	if err == nil {
		t.Fatal("BTC rules silently accepted ETH order")
	}
}

func TestRegressionNumJSONContract(t *testing.T) {
	for _, s := range []string{`true`, `{}`, `[]`} {
		var n Num
		if err := json.Unmarshal([]byte(s), &n); err == nil {
			t.Errorf("Num accepted nonnumeric JSON %s as %q", s, n)
		}
	}
	var n Num
	if err := json.Unmarshal([]byte(`"1\u002e2"`), &n); err != nil {
		t.Fatal(err)
	}
	if n != "1.2" {
		t.Errorf("escaped numeric text decoded as %q, want 1.2", n)
	}
}

func TestRegressionAccountPushWithUIDReachesSubscriber(t *testing.T) {
	c := NewWSClient(Private)
	called := false
	arg := subArg{Channel: "account"}
	handlers := map[string]*subEntry{arg.key(): {arg: arg, handler: func(Message) { called = true }}}
	c.handlers.Store(&handlers)
	c.dispatch([]byte(`{"arg":{"channel":"account","uid":"123456789"},"eventType":"snapshot","data":[{"uTime":"1780000000000","totalEq":"100","details":[]}]}`))
	if !called {
		t.Fatal("account push with server-supplied uid silently misses channel-only subscription")
	}
}

func TestRegressionRESTBodyRemainsOwnedUntilTransportClose(t *testing.T) {
	oldProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldProcs)
	var pending io.ReadCloser
	defer func() {
		if pending != nil {
			_ = pending.Close()
		}
	}()
	var corrupted bool
	rt := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if pending == nil {
			pending = req.Body
			return nil, errors.New("injected transport error before async body close")
		}
		raw, err := io.ReadAll(pending)
		if err != nil {
			return nil, err
		}
		_ = pending.Close()
		pending = nil
		if strings.Contains(string(raw), "BBBB") {
			corrupted = true
		}
		_ = req.Body.Close()
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"code":"0","data":[]}`))}, nil
	})
	c := NewClient(WithHTTPClient(&http.Client{Transport: rt}), WithRateLimiter(nil))
	for i := 0; i < 16 && !corrupted; i++ {
		_ = c.do(t.Context(), requestSpec{method: "POST", path: "/test", body: map[string]string{"value": strings.Repeat("A", 64)}}, nil)
		if err := c.do(t.Context(), requestSpec{method: "POST", path: "/test", body: map[string]string{"value": strings.Repeat("B", 64)}}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if corrupted {
		t.Fatal("first request body overwritten by second request before first transport closed it")
	}
}

func TestRegressionRulesRejectQuoteQuantity(t *testing.T) {
	rules, err := NewInstrumentRules(Instrument{InstID: "BTC-USDT", InstType: InstSpot, TickSz: "0.1", LotSz: "0.01", MinSz: "0.01"})
	if err != nil {
		t.Fatal(err)
	}
	req := PlaceOrderRequest{InstID: "BTC-USDT", TdMode: TdCash, Side: Buy, OrdType: OrdMarket, Sz: "100"}
	if _, err = rules.PreparePlaceOrder(req); !errors.Is(err, ErrInvalidOrder) {
		t.Fatal("default quote quantity accepted", err)
	}
	req.TgtCcy = "base_ccy"
	if _, err = rules.PreparePlaceOrder(req); err != nil {
		t.Fatal("explicit base quantity rejected", err)
	}
}

func TestRegressionMalformedSequenceInvalidatesBook(t *testing.T) {
	for _, seq := range []Num{"", "broken", "1.5", "-1"} {
		book := NewLocalOrderBook("BTC-USDT", 5)
		if err := book.Apply("snapshot", OrderBook{SeqID: "0"}); err != nil {
			t.Fatal(err)
		}
		err := book.Apply("update", OrderBook{PrevSeqID: "0", SeqID: seq})
		if !errors.Is(err, ErrInvalidOrderBook) || book.Ready() {
			t.Fatalf("invalid seq %q: %v ready=%v", seq, err, book.Ready())
		}
	}
}

func TestRegressionBatchLimitsBeforeNetwork(t *testing.T) {
	rest := NewClient(WithRateLimiter(nil))
	ws := NewWSClient(Private, WithWSRateLimiter(nil))
	places := make([]PlaceOrderRequest, MaxBatchOrderRequests+1)
	cancels := make([]CancelOrderRequest, MaxBatchOrderRequests+1)
	amends := make([]AmendOrderRequest, MaxBatchOrderRequests+1)
	checks := []func() error{
		func() error { _, err := rest.Trade.PlaceMultipleOrders(t.Context(), places); return err },
		func() error { _, err := rest.Trade.CancelMultipleOrders(t.Context(), cancels); return err },
		func() error { _, err := rest.Trade.AmendMultipleOrders(t.Context(), amends); return err },
		func() error { _, err := ws.PlaceMultipleOrders(t.Context(), places); return err },
		func() error { _, err := ws.CancelMultipleOrders(t.Context(), cancels); return err },
		func() error { _, err := ws.AmendMultipleOrders(t.Context(), amends); return err },
	}
	for i, check := range checks {
		if err := check(); !errors.Is(err, ErrInvalidParameter) {
			t.Fatalf("batch API %d: %v", i, err)
		}
	}
}
