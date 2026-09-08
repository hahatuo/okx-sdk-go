package okx

import (
	json "github.com/go-json-experiment/json"
	"testing"
)

func benchmarkBookConsumer(b *testing.B, frame []byte) {
	c := NewWSClient(Public)
	arg := subArg{Channel: "books", InstID: "BTC-USDT"}
	consume, _ := localBookConsumer("books", "BTC-USDT", 400, 400, func(msg WSLocalOrderBookMessage) {
		if msg.Err != nil {
			b.Fatal(msg.Err)
		}
	}, func(error) {})
	entries := map[string]*subEntry{arg.key(): {arg: arg, handler: consume}}
	c.handlers.Store(&entries)
	var env wsIncoming
	if err := c.dispatchSession(nil, auditSnapshot(), &env); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.SetBytes(int64(len(frame)))
	b.ResetTimer()
	for b.Loop() {
		if err := c.dispatchSession(nil, frame, &env); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBookDispatch400(b *testing.B) { benchmarkBookConsumer(b, auditSnapshot()) }
func BenchmarkBookDispatchDelta(b *testing.B) {
	benchmarkBookConsumer(b, []byte(`{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"asks":[["100001.12345678","0.23","0","1"]],"bids":[["99999.12345678","0.45","0","1"]],"seqId":100,"prevSeqId":100,"ts":"1780000000000"}]}`))
}

func TestOrderBookApplyAllocationBudget(t *testing.T) {
	book := NewLocalOrderBook("BTC-USDT", 400)
	snapshot := OrderBook{SeqID: "1", Bids: []BookLevel{{"100", "1", "0", "1"}}, Asks: []BookLevel{{"101", "1", "0", "1"}}}
	if err := book.Apply("snapshot", snapshot); err != nil {
		t.Fatal(err)
	}
	delta := OrderBook{SeqID: "1", PrevSeqID: "1", Bids: []BookLevel{{"100", "2", "0", "2"}}, Asks: []BookLevel{{"101", "2", "0", "2"}}}
	var err error
	allocs := testing.AllocsPerRun(1000, func() { err = book.Apply("update", delta) })
	if err != nil || allocs != 0 {
		t.Fatalf("Apply: %g allocs, %v", allocs, err)
	}
}

func BenchmarkBookApplyDelta(b *testing.B) {
	book := NewLocalOrderBook("BTC-USDT", 400)
	if err := book.Apply("snapshot", OrderBook{SeqID: "1"}); err != nil {
		b.Fatal(err)
	}
	delta := OrderBook{SeqID: "1", PrevSeqID: "1", Bids: []BookLevel{{"100", "2", "0", "2"}}, Asks: []BookLevel{{"101", "2", "0", "2"}}}
	b.ReportAllocs()
	for b.Loop() {
		if err := book.Apply("update", delta); err != nil {
			b.Fatal(err)
		}
	}
}

func TestLocalBookDecodeAllocationBudgetAndReset(t *testing.T) {
	consume, _ := localBookConsumer("books", "BTC-USDT", 400, 400, func(msg WSLocalOrderBookMessage) {
		if msg.Err != nil {
			t.Fatal(msg.Err)
		}
	}, func(error) {})
	snapshot := Message{Action: "snapshot", Data: []byte(`[{"seqId":1,"prevSeqId":-1,"bids":[["100","1","0","1"]],"asks":[["101","1","0","1"]]}]`)}
	consume(snapshot)
	delta := Message{Action: "update", Data: []byte(`[{"seqId":1,"prevSeqId":1,"bids":[["100","2","0","2"]],"asks":[["101","2","0","2"]]}]`)}
	allocs := testing.AllocsPerRun(1000, func() { consume(delta) })
	if allocs > 6 {
		t.Fatalf("local book decode budget exceeded: %g allocs", allocs)
	}
	rows := make(reusableOrderBooks, 1)
	if err := snapshot.Into(&rows); err != nil {
		t.Fatal(err)
	}
	bidCap := cap(rows[0].Bids)
	if err := (Message{Data: []byte(`[{"seqId":2,"prevSeqId":1}]`)}).Into(&rows); err != nil {
		t.Fatal(err)
	}
	if len(rows[0].Bids) != 0 || len(rows[0].Asks) != 0 || cap(rows[0].Bids) != bidCap {
		t.Fatal("decode retained stale fields or lost side capacity")
	}
}

func BenchmarkPlaceOrderEncode(b *testing.B) {
	req := PlaceOrderRequest{InstID: "BTC-USDT", TdMode: TdCash, Side: Buy, OrdType: OrdLimit, Px: "100", Sz: "0.01", ClOrdID: "benchmark1"}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := json.Marshal(req); err != nil {
			b.Fatal(err)
		}
	}
}
