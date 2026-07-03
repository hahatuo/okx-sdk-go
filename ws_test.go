package okx

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
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
