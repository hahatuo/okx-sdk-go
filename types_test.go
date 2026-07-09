package okx

import (
	"testing"

	json "github.com/go-json-experiment/json"
)

func TestNormalizeOrderState(t *testing.T) {
	tests := []struct {
		state string
		want  OrderState
	}{
		{state: "live", want: OrderStateOpen},
		{state: "partially_filled", want: OrderStatePartiallyFilled},
		{state: "filled", want: OrderStateFilled},
		{state: "canceled", want: OrderStateCanceled},
		{state: "mmp_canceled", want: OrderStateCanceled},
		{state: "rejected", want: OrderStateRejected},
		{state: "failed", want: OrderStateRejected},
		{state: "other", want: OrderStateUnknown},
	}
	for _, tt := range tests {
		if got := NormalizeOrderState(tt.state); got != tt.want {
			t.Fatalf("NormalizeOrderState(%q) = %q, want %q", tt.state, got, tt.want)
		}
		if got := (Order{State: tt.state}).NormalizedState(); got != tt.want {
			t.Fatalf("Order.NormalizedState(%q) = %q, want %q", tt.state, got, tt.want)
		}
	}
}

func TestNumPreservesOKXDecimalText(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want Num
	}{
		{name: "decimal string", raw: `"0.00000001"`, want: "0.00000001"},
		{name: "leading zeros string", raw: `"001.20"`, want: "001.20"},
		{name: "scientific string", raw: `"1e-8"`, want: "1e-8"},
		{name: "large integer string", raw: `"123456789012345678901234567890"`, want: "123456789012345678901234567890"},
		{name: "bare decimal", raw: `0.00000001`, want: "0.00000001"},
		{name: "bare scientific", raw: `1e-8`, want: "1e-8"},
		{name: "negative", raw: `"-100.000000000000000001"`, want: "-100.000000000000000001"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got Num
			if err := json.Unmarshal([]byte(tt.raw), &got); err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("Num = %q, want %q", got, tt.want)
			}
			encoded, err := json.Marshal(got)
			if err != nil {
				t.Fatal(err)
			}
			if string(encoded) != `"`+string(tt.want)+`"` {
				t.Fatalf("Marshal = %s, want quoted %q", encoded, tt.want)
			}
		})
	}
}

func TestNumPreservesSequenceValues(t *testing.T) {
	var got struct {
		PrevSeqID Num `json:"prevSeqId"`
		SeqID     Num `json:"seqId"`
		Checksum  Num `json:"checksum"`
		Count     Num `json:"count"`
	}
	if err := json.Unmarshal([]byte(`{"prevSeqId":-1,"seqId":0,"checksum":0,"count":"00042"}`), &got); err != nil {
		t.Fatal(err)
	}
	if got.PrevSeqID.String() != "-1" || got.SeqID.String() != "0" || got.Checksum.String() != "0" || got.Count.String() != "00042" {
		t.Fatalf("unexpected Num values: %+v", got)
	}

	if err := json.Unmarshal([]byte(`{"prevSeqId":null,"seqId":"12345678901234567890"}`), &got); err != nil {
		t.Fatal(err)
	}
	if got.PrevSeqID.String() != "" || got.SeqID.String() != "12345678901234567890" {
		t.Fatalf("unexpected nullable/string values: %+v", got)
	}
}
