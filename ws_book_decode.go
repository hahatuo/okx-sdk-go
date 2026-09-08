package okx

import (
	json "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
)

// JSON v2 resets slice elements before unmarshaling. Keep each book's side
// capacities explicitly instead of losing them when the outer array is reset.
// Storage belongs to one local-book consumer on the read loop.
type reusableOrderBooks []OrderBook

func (books *reusableOrderBooks) UnmarshalJSONFrom(dec *jsontext.Decoder) error {
	token, err := dec.ReadToken()
	if err != nil {
		return err
	}
	if token.Kind() != '[' {
		return ErrInvalidOrderBook
	}
	rows := *books
	count := 0
	for dec.PeekKind() != ']' {
		if count == len(rows) {
			rows = append(rows, OrderBook{})
		}
		row := &rows[count]
		*row = OrderBook{Bids: row.Bids[:0], Asks: row.Asks[:0]}
		if err := json.UnmarshalDecode(dec, row); err != nil {
			*books = rows
			return err
		}
		count++
	}
	if _, err := dec.ReadToken(); err != nil {
		return err
	}
	*books = rows[:count]
	return nil
}
