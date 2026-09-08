package okx

import (
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
)

// InstrumentRules contains the local price and size constraints needed to
// prepare an order before it is submitted to OKX.
type InstrumentRules struct {
	InstID     string
	InstIDCode int64
	InstType   InstType
	TickSize   decimal.Decimal
	LotSize    decimal.Decimal
	MinSize    decimal.Decimal
}

// Rules parses an instrument's OKX numeric rule fields.
func (inst Instrument) Rules() (InstrumentRules, error) {
	return NewInstrumentRules(inst)
}

// NewInstrumentRules parses an instrument's OKX numeric rule fields.
func NewInstrumentRules(inst Instrument) (InstrumentRules, error) {
	tickSize, err := parseRuleDecimal("tickSz", inst.TickSz)
	if err != nil {
		return InstrumentRules{}, err
	}
	lotSize, err := parseRuleDecimal("lotSz", inst.LotSz)
	if err != nil {
		return InstrumentRules{}, err
	}
	minSize, err := parseRuleDecimal("minSz", inst.MinSz)
	if err != nil {
		return InstrumentRules{}, err
	}
	return InstrumentRules{
		InstID:     inst.InstID,
		InstIDCode: inst.InstIDCode,
		InstType:   inst.InstType,
		TickSize:   tickSize,
		LotSize:    lotSize,
		MinSize:    minSize,
	}, nil
}

// NormalizeSide maps common caller input onto OKX's order side domain.
func NormalizeSide(side Side) (Side, error) {
	switch Side(strings.ToLower(strings.TrimSpace(string(side)))) {
	case Buy:
		return Buy, nil
	case Sell:
		return Sell, nil
	default:
		return "", fmt.Errorf("%w: %w: unsupported order side %q", ErrInvalidParameter, ErrInvalidOrder, side)
	}
}

// RoundPrice rounds a price down to the instrument tick size.
func (r InstrumentRules) RoundPrice(price decimal.Decimal) decimal.Decimal {
	if r.TickSize.LessThanOrEqual(decimal.Zero) {
		return price
	}
	return roundStep(price, r.TickSize, false)
}

// RoundPriceForSide rounds buy prices down and sell prices up to the instrument
// tick size. This preserves the passive side of caller-selected book prices.
func (r InstrumentRules) RoundPriceForSide(side Side, price decimal.Decimal) (decimal.Decimal, error) {
	side, err := NormalizeSide(side)
	if err != nil {
		return decimal.Zero, err
	}
	if r.TickSize.LessThanOrEqual(decimal.Zero) {
		return price, nil
	}
	if side == Buy {
		return roundStep(price, r.TickSize, false), nil
	}
	return roundStep(price, r.TickSize, true), nil
}

// RoundSize rounds a size down to the instrument lot size.
func (r InstrumentRules) RoundSize(size decimal.Decimal) decimal.Decimal {
	if r.LotSize.LessThanOrEqual(decimal.Zero) {
		return size
	}
	return roundStep(size, r.LotSize, false)
}

// QuoRem is exact: a rounded intermediate quotient can cross a tick boundary.
func roundStep(value, step decimal.Decimal, up bool) decimal.Decimal {
	q, rem := value.QuoRem(step, 0)
	if up && rem.Sign() > 0 {
		q = q.Add(decimal.NewFromInt(1))
	} else if !up && rem.Sign() < 0 {
		q = q.Sub(decimal.NewFromInt(1))
	}
	return q.Mul(step)
}

// IsPriceAligned reports whether a positive price is already aligned to tick
// size. When no tick size is known, positive prices are accepted.
func (r InstrumentRules) IsPriceAligned(price decimal.Decimal) bool {
	if price.LessThanOrEqual(decimal.Zero) {
		return false
	}
	if r.TickSize.LessThanOrEqual(decimal.Zero) {
		return true
	}
	return price.Mod(r.TickSize).Equal(decimal.Zero)
}

// PreparePlaceOrder validates and rounds an order request using instrument
// rules. It returns a copy and never mutates the input request.
func (r InstrumentRules) PreparePlaceOrder(req PlaceOrderRequest) (PlaceOrderRequest, error) {
	if err := validatePlaceOrder(req); err != nil {
		return req, fmt.Errorf("%w: %w", ErrInvalidOrder, err)
	}
	if (req.InstID != "" && (r.InstID == "" || req.InstID != r.InstID)) ||
		(req.InstIDCode != 0 && (r.InstIDCode == 0 || req.InstIDCode != r.InstIDCode)) {
		return req, fmt.Errorf("%w: %w: instrument rules do not match request", ErrInvalidParameter, ErrInvalidOrder)
	}

	side, err := NormalizeSide(req.Side)
	if err != nil {
		return req, err
	}
	req.Side = side
	if req.OrdType == OrdMarket && r.InstType == InstMargin && req.Side == Buy {
		return req, fmt.Errorf("%w: %w: margin market-buy size is quote currency", ErrInvalidParameter, ErrInvalidOrder)
	}
	// Spot market-buy sizes default to quote currency. lotSz/minSz are base
	// currency constraints and must not be applied to a quote amount.
	if req.OrdType == OrdMarket && (r.InstType == InstSpot || (r.InstType == "" && req.TdMode == TdCash)) {
		if req.TgtCcy != "base_ccy" && (req.TgtCcy == "quote_ccy" || req.Side == Buy) {
			return req, fmt.Errorf("%w: %w: quote-currency market size cannot be normalized with base-currency rules; specify base_ccy", ErrInvalidParameter, ErrInvalidOrder)
		}
		if req.TgtCcy != "" && req.TgtCcy != "base_ccy" && req.TgtCcy != "quote_ccy" {
			return req, fmt.Errorf("%w: invalid tgtCcy %q", ErrInvalidParameter, req.TgtCcy)
		}
	}

	if strings.TrimSpace(req.Px) != "" {
		price, err := decimal.NewFromString(strings.TrimSpace(req.Px))
		if err != nil {
			return req, fmt.Errorf("%w: %w: invalid order price %q", ErrInvalidParameter, ErrInvalidOrder, req.Px)
		}
		if price.LessThan(decimal.Zero) {
			return req, fmt.Errorf("%w: %w: order price %s is negative", ErrInvalidParameter, ErrInvalidOrder, price)
		}
		if price.GreaterThan(decimal.Zero) {
			price, err = r.RoundPriceForSide(req.Side, price)
			if err != nil {
				return req, err
			}
			if price.LessThanOrEqual(decimal.Zero) {
				return req, fmt.Errorf("%w: %w: rounded price %s must be > 0", ErrInvalidParameter, ErrInvalidOrder, price)
			}
			req.Px = price.String()
		}
	}

	size, err := decimal.NewFromString(strings.TrimSpace(req.Sz))
	if err != nil {
		return req, fmt.Errorf("%w: %w: invalid order size %q", ErrInvalidParameter, ErrInvalidOrder, req.Sz)
	}
	if size.LessThanOrEqual(decimal.Zero) {
		return req, fmt.Errorf("%w: %w: order size %s must be > 0", ErrInvalidParameter, ErrInvalidOrder, size)
	}
	size = r.RoundSize(size)
	if r.MinSize.GreaterThan(decimal.Zero) && size.LessThan(r.MinSize) {
		return req, fmt.Errorf("%w: %w: %w: rounded size %s below minSz %s", ErrInvalidParameter, ErrInvalidOrder, ErrBelowMinSize, size, r.MinSize)
	}
	if size.LessThanOrEqual(decimal.Zero) {
		return req, fmt.Errorf("%w: %w: rounded size %s must be > 0", ErrInvalidParameter, ErrInvalidOrder, size)
	}
	req.Sz = size.String()
	return req, nil
}

func parseRuleDecimal(name string, value Num) (decimal.Decimal, error) {
	text := strings.TrimSpace(value.String())
	if text == "" {
		return decimal.Zero, nil
	}
	out, err := decimal.NewFromString(text)
	if err != nil {
		return decimal.Zero, fmt.Errorf("%w: invalid %s %q", ErrInvalidParameter, name, text)
	}
	if out.LessThan(decimal.Zero) {
		return decimal.Zero, fmt.Errorf("%w: invalid %s %s", ErrInvalidParameter, name, out)
	}
	return out, nil
}
