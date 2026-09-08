package okx

import (
	"errors"
	"testing"
)

func TestLocalOrderBookApplySnapshotAndUpdate(t *testing.T) {
	book := NewLocalOrderBook("BTC-USDT", 3)
	err := book.Apply("snapshot", OrderBook{
		Asks:  []BookLevel{{"101", "1", "0", "1"}, {"102", "1", "0", "1"}},
		Bids:  []BookLevel{{"99", "1", "0", "1"}, {"98", "1", "0", "1"}},
		TS:    "1",
		SeqID: "10",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !book.Ready() || book.SeqID() != 10 || book.Timestamp() != "1" {
		t.Fatalf("unexpected book state: ready=%v seq=%d ts=%s", book.Ready(), book.SeqID(), book.Timestamp())
	}

	err = book.Apply("update", OrderBook{
		Asks:      []BookLevel{{"100.5", "2", "0", "1"}, {"102", "0", "0", "1"}},
		Bids:      []BookLevel{{"99.5", "3", "0", "1"}, {"98", "0", "0", "1"}},
		TS:        "2",
		SeqID:     "11",
		PrevSeqID: "10",
	})
	if err != nil {
		t.Fatal(err)
	}
	if book.SeqID() != 11 || len(book.Asks()) != 2 || len(book.Bids()) != 2 {
		t.Fatalf("unexpected book after update: asks=%+v bids=%+v seq=%d", book.Asks(), book.Bids(), book.SeqID())
	}
	if book.Asks()[0].Price() != "100.5" || book.Asks()[1].Price() != "101" {
		t.Fatalf("asks not sorted/applied: %+v", book.Asks())
	}
	if book.Bids()[0].Price() != "99.5" || book.Bids()[1].Price() != "99" {
		t.Fatalf("bids not sorted/applied: %+v", book.Bids())
	}
}

func TestLocalOrderBookRetainsHiddenSnapshotLevels(t *testing.T) {
	book := NewLocalOrderBook("BTC-USDT", 2)
	if err := book.Apply("snapshot", OrderBook{
		Asks: []BookLevel{
			{"101", "1", "0", "1"},
			{"102", "1", "0", "1"},
			{"103", "1", "0", "1"},
			{"104", "1", "0", "1"},
		},
		Bids: []BookLevel{
			{"99", "1", "0", "1"},
			{"98", "1", "0", "1"},
			{"97", "1", "0", "1"},
			{"96", "1", "0", "1"},
		},
		SeqID: "10",
	}); err != nil {
		t.Fatal(err)
	}

	if err := book.Apply("update", OrderBook{
		Asks: []BookLevel{
			{"103", "2", "0", "1"},
			{"101", "0", "0", "0"},
		},
		Bids: []BookLevel{
			{"97", "2", "0", "1"},
			{"99", "0", "0", "0"},
		},
		PrevSeqID: "10",
		SeqID:     "11",
	}); err != nil {
		t.Fatal(err)
	}

	if got := book.Asks(); len(got) != 2 || got[0].Price() != "102" || got[1].Price() != "103" {
		t.Fatalf("asks = %+v, want prices [102 103]", got)
	}
	if got := book.Bids(); len(got) != 2 || got[0].Price() != "98" || got[1].Price() != "97" {
		t.Fatalf("bids = %+v, want prices [98 97]", got)
	}
	if book.Asks()[1].Size() != "2" || book.Bids()[1].Size() != "2" {
		t.Fatalf("hidden updates were not retained: asks=%+v bids=%+v", book.Asks(), book.Bids())
	}
	if cap(book.Asks()) != 2 || cap(book.Bids()) != 2 {
		t.Fatalf("visible slices expose hidden capacity: asks cap=%d bids cap=%d", cap(book.Asks()), cap(book.Bids()))
	}
}

func TestLocalOrderBookSequenceGap(t *testing.T) {
	book := NewLocalOrderBook("BTC-USDT", 0)
	if err := book.Apply("snapshot", OrderBook{SeqID: "10"}); err != nil {
		t.Fatal(err)
	}
	err := book.Apply("update", OrderBook{PrevSeqID: "9", SeqID: "11"})
	if !errors.Is(err, ErrOrderBookSequenceGap) {
		t.Fatalf("expected sequence gap, got %v", err)
	}
	var gap *OrderBookSequenceGapError
	if !errors.As(err, &gap) || gap.ExpectedPrevSeqID != 10 || gap.PrevSeqID != 9 || gap.SeqID != 11 {
		t.Fatalf("unexpected gap detail: %#v err=%v", gap, err)
	}
}

func TestLocalOrderBookNoOpAndSequenceReset(t *testing.T) {
	book := NewLocalOrderBook("BTC-USDT", 0)
	if err := book.Apply("snapshot", OrderBook{
		Asks:      []BookLevel{{"101", "1", "0", "1"}},
		Bids:      []BookLevel{{"99", "1", "0", "1"}},
		SeqID:     "10",
		PrevSeqID: "-1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := book.Apply("update", OrderBook{
		Asks:      []BookLevel{{"100.5", "2", "0", "1"}},
		Bids:      []BookLevel{{"99.5", "3", "0", "1"}},
		PrevSeqID: "10",
		SeqID:     "15",
	}); err != nil {
		t.Fatal(err)
	}

	if err := book.Apply("update", OrderBook{PrevSeqID: "15", SeqID: "15"}); err != nil {
		t.Fatalf("no-op update should keep the book alive: %v", err)
	}
	if book.SeqID() != 15 || len(book.Asks()) != 2 || len(book.Bids()) != 2 {
		t.Fatalf("unexpected no-op state: seq=%d asks=%+v bids=%+v", book.SeqID(), book.Asks(), book.Bids())
	}

	if err := book.Apply("update", OrderBook{PrevSeqID: "15", SeqID: "3"}); err != nil {
		t.Fatalf("sequence reset should be accepted when prevSeqId matches current seqId: %v", err)
	}
	if book.SeqID() != 3 {
		t.Fatalf("seq after reset = %d, want 3", book.SeqID())
	}
	if err := book.Apply("update", OrderBook{
		Bids:      []BookLevel{{"100", "4", "0", "1"}},
		PrevSeqID: "3",
		SeqID:     "5",
	}); err != nil {
		t.Fatal(err)
	}
	if book.SeqID() != 5 || book.Bids()[0].Price() != "100" {
		t.Fatalf("unexpected state after reset continuation: seq=%d bids=%+v", book.SeqID(), book.Bids())
	}
}

func TestLocalOrderBookIgnoresDeprecatedChecksum(t *testing.T) {
	book := NewLocalOrderBook("BTC-USDT", 0)
	if err := book.Apply("snapshot", OrderBook{
		Asks:      []BookLevel{{"101", "1", "0", "1"}},
		Bids:      []BookLevel{{"99", "1", "0", "1"}},
		SeqID:     "10",
		PrevSeqID: "-1",
		Checksum:  "123456",
	}); err != nil {
		t.Fatal(err)
	}
	if err := book.Apply("update", OrderBook{
		Asks:      []BookLevel{{"101", "2", "0", "1"}},
		PrevSeqID: "10",
		SeqID:     "11",
		Checksum:  "-999",
	}); err != nil {
		t.Fatal(err)
	}
	if book.Asks()[0].Size() != "2" {
		t.Fatalf("update was not applied: asks=%+v", book.Asks())
	}
}

func TestLocalOrderBookRejectsUpdateBeforeSnapshot(t *testing.T) {
	book := NewLocalOrderBook("BTC-USDT", 0)
	err := book.Apply("update", OrderBook{PrevSeqID: "0", SeqID: "1"})
	if !errors.Is(err, ErrOrderBookNotReady) {
		t.Fatalf("expected not ready, got %v", err)
	}
}

func TestLocalOrderBookApplyTypedMessage(t *testing.T) {
	book := NewLocalOrderBook("BTC-USDT", 1)
	msg := WSTypedMessage[OrderBook]{
		Action: "snapshot",
		Data: []OrderBook{{
			Asks:  []BookLevel{{"101", "1", "0", "1"}, {"102", "1", "0", "1"}},
			Bids:  []BookLevel{{"99", "1", "0", "1"}, {"98", "1", "0", "1"}},
			SeqID: "1",
		}},
	}
	if err := book.ApplyMessage(msg); err != nil {
		t.Fatal(err)
	}
	if len(book.Asks()) != 1 || len(book.Bids()) != 1 {
		t.Fatalf("max depth not applied: asks=%+v bids=%+v", book.Asks(), book.Bids())
	}
}

func TestLocalBookConsumerEmitsMergedBook(t *testing.T) {
	var bestAsk, bestBid string
	calls := 0
	consume, _ := localBookConsumer("books", "DATA-USDT", 100, MaxOrderBookDepth, func(msg WSLocalOrderBookMessage) {
		if msg.Err != nil || msg.Book == nil {
			t.Fatalf("unexpected book message: %+v", msg)
		}
		calls++
		bestAsk, bestBid = msg.Book.Asks()[0].Price(), msg.Book.Bids()[0].Price()
	}, func(err error) { t.Fatalf("unexpected recovery: %v", err) })
	consume(Message{Action: "snapshot", Data: []byte(`[{"asks":[["0.2945","10","0","1"],["0.2950","20","0","1"]],"bids":[["0.2938","10","0","1"],["0.2937","20","0","1"]],"ts":"1","seqId":10}]`)})
	consume(Message{Action: "update", Data: []byte(`[{"asks":[["0.3160","5","0","1"]],"bids":[["0.2062","5","0","1"]],"ts":"2","seqId":11,"prevSeqId":10}]`)})
	if calls != 2 || bestAsk != "0.2945" || bestBid != "0.2938" {
		t.Fatalf("calls=%d bestAsk=%s bestBid=%s", calls, bestAsk, bestBid)
	}
}

func TestLocalBookConsumerInvalidatesAndRequestsRecovery(t *testing.T) {
	var failures []error
	var recoveries []error
	consume, disconnected := localBookConsumer("books", "BTC-USDT", 5, MaxOrderBookDepth, func(msg WSLocalOrderBookMessage) {
		if msg.Err != nil {
			if msg.Book != nil {
				t.Fatal("invalid book was exposed")
			}
			failures = append(failures, msg.Err)
		}
	}, func(err error) { recoveries = append(recoveries, err) })
	snapshot := Message{Action: "snapshot", Data: []byte(`[{"asks":[["101","1","0","1"]],"bids":[["99","1","0","1"]],"seqId":10}]`)}
	consume(snapshot)
	consume(Message{Action: "update", Data: []byte(`[{"asks":[["102","1","0","1"]],"prevSeqId":9,"seqId":11}]`)})
	consume(Message{Action: "update", Data: []byte(`[{"asks":[["103","1","0","1"]],"prevSeqId":11,"seqId":12}]`)})
	if len(failures) != 2 || len(recoveries) != 2 || !errors.Is(failures[0], ErrOrderBookSequenceGap) || !errors.Is(failures[1], ErrOrderBookNotReady) {
		t.Fatalf("failures=%v recoveries=%v", failures, recoveries)
	}
	consume(snapshot)
	disconnected(ErrWSConnectionLost)
	consume(Message{Action: "update", Data: []byte(`[{"prevSeqId":10,"seqId":11}]`)})
	if len(failures) != 4 || !errors.Is(failures[3], ErrOrderBookNotReady) {
		t.Fatal("disconnect did not invalidate the book", failures)
	}
}

func TestCmpDecimal(t *testing.T) {
	if cmpDecimal("001.20", "1.2") != 0 {
		t.Fatal("expected equal decimal values")
	}
	if cmpDecimal("1.21", "1.2") <= 0 {
		t.Fatal("expected 1.21 > 1.2")
	}
	if cmpDecimal("0.9", "1") >= 0 {
		t.Fatal("expected 0.9 < 1")
	}
}
