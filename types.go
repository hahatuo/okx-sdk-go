package okx

import (
	"bytes"
	"fmt"
	"strings"

	json "github.com/go-json-experiment/json"
)

// Enumerated string domains from the OKX v5 API. Kept as named string types so
// callers get documentation and light type-safety without boxing.
type (
	InstType    string // instrument type
	TdMode      string // trade mode
	Side        string // order side
	OrdType     string // order type
	PosSide     string // position side
	MgnMode     string // margin mode
	OrderState  string // normalized order state
	AccountType string // OKX account type code used by asset transfers
)

const (
	InstSpot    InstType = "SPOT"
	InstMargin  InstType = "MARGIN"
	InstSwap    InstType = "SWAP"
	InstFutures InstType = "FUTURES"
	InstOption  InstType = "OPTION"
	InstEvents  InstType = "EVENTS"
	InstAny     InstType = "ANY"

	TdCash     TdMode = "cash"
	TdCross    TdMode = "cross"
	TdIsolated TdMode = "isolated"
	TdSpotIso  TdMode = "spot_isolated"

	Buy  Side = "buy"
	Sell Side = "sell"

	SideBuy  Side = Buy
	SideSell Side = Sell

	PosNet   PosSide = "net"
	PosLong  PosSide = "long"
	PosShort PosSide = "short"

	MgnCross    MgnMode = "cross"
	MgnIsolated MgnMode = "isolated"

	OrdMarket          OrdType = "market"
	OrdLimit           OrdType = "limit"
	OrdPostOnly        OrdType = "post_only"
	OrdFOK             OrdType = "fok"
	OrdIOC             OrdType = "ioc"
	OrdOptimalLimitIOC OrdType = "optimal_limit_ioc"

	OrderStateUnknown         OrderState = "unknown"
	OrderStateOpen            OrderState = "open"
	OrderStatePartiallyFilled OrderState = "partially_filled"
	OrderStateFilled          OrderState = "filled"
	OrderStateCanceled        OrderState = "canceled"
	OrderStateRejected        OrderState = "rejected"

	AccountFunding AccountType = "6"
	AccountTrading AccountType = "18"
)

// NormalizeOrderState maps OKX order states to the SDK's compact normalized
// state domain.
func NormalizeOrderState(state string) OrderState {
	switch strings.ToLower(strings.TrimSpace(state)) {
	case "live":
		return OrderStateOpen
	case "partially_filled":
		return OrderStatePartiallyFilled
	case "filled":
		return OrderStateFilled
	case "canceled", "mmp_canceled":
		return OrderStateCanceled
	case "rejected", "failed":
		return OrderStateRejected
	default:
		return OrderStateUnknown
	}
}

// Num is an OKX numeric field. OKX returns all quantities and prices as JSON
// strings to preserve decimal precision, but a few endpoints send bare numbers.
// Num accepts either form and stores the raw textual value; conversion to a
// fixed-point / decimal type is left to the caller so the SDK never silently
// loses precision.
type Num string

// UnmarshalJSON accepts both "123.4" (string) and 123.4 (number). The v1-style
// signature is honored by github.com/go-json-experiment/json.
func (n *Num) UnmarshalJSON(b []byte) error {
	if len(b) == 0 || bytes.Equal(b, []byte("null")) {
		*n = ""
		return nil
	}
	if b[0] == '"' {
		if len(b) < 2 || b[len(b)-1] != '"' {
			return fmt.Errorf("okx: malformed Num %q", b)
		}
		*n = Num(b[1 : len(b)-1])
		return nil
	}
	*n = Num(b) // bare number: keep raw bytes verbatim
	return nil
}

// MarshalJSON always emits the OKX-canonical string form.
func (n Num) MarshalJSON() ([]byte, error) {
	return json.Marshal(string(n))
}

// String returns the raw textual value.
func (n Num) String() string { return string(n) }
