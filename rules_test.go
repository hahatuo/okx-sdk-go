package okx

import (
	"errors"
	"testing"

	"github.com/shopspring/decimal"
)

func TestInstrumentRulesPreparePlaceOrderRoundsBySideAndSize(t *testing.T) {
	rules := mustRules(t, Instrument{
		InstID: "BTC-USDT",
		TickSz: Num("0.1"),
		LotSz:  Num("0.01"),
		MinSz:  Num("0.01"),
	})

	buy, err := rules.PreparePlaceOrder(PlaceOrderRequest{
		InstID:  "BTC-USDT",
		TdMode:  TdCash,
		Side:    Side("BUY"),
		OrdType: OrdPostOnly,
		Sz:      "0.019",
		Px:      "100.19",
	})
	if err != nil {
		t.Fatal(err)
	}
	if buy.Side != Buy || buy.Px != "100.1" || buy.Sz != "0.01" {
		t.Fatalf("buy prepared = %+v", buy)
	}

	sell, err := rules.PreparePlaceOrder(PlaceOrderRequest{
		InstID:  "BTC-USDT",
		TdMode:  TdCash,
		Side:    Sell,
		OrdType: OrdPostOnly,
		Sz:      "0.019",
		Px:      "100.11",
	})
	if err != nil {
		t.Fatal(err)
	}
	if sell.Px != "100.2" || sell.Sz != "0.01" {
		t.Fatalf("sell prepared = %+v", sell)
	}
}

func TestInstrumentRulesPreparePlaceOrderRejectsBelowMinAfterRounding(t *testing.T) {
	rules := mustRules(t, Instrument{
		InstID: "BTC-USDT",
		TickSz: Num("0.1"),
		LotSz:  Num("0.01"),
		MinSz:  Num("0.01"),
	})

	_, err := rules.PreparePlaceOrder(PlaceOrderRequest{
		InstID:  "BTC-USDT",
		TdMode:  TdCash,
		Side:    Buy,
		OrdType: OrdPostOnly,
		Sz:      "0.009",
		Px:      "100.1",
	})
	if !errors.Is(err, ErrBelowMinSize) || !errors.Is(err, ErrInvalidOrder) || !errors.Is(err, ErrInvalidParameter) {
		t.Fatalf("err = %v, want ErrBelowMinSize, ErrInvalidOrder, and ErrInvalidParameter", err)
	}
}

func TestInstrumentRulesPrepareMarketOrderAllowsEmptyPrice(t *testing.T) {
	rules := mustRules(t, Instrument{
		InstID: "BTC-USDT",
		TickSz: Num("0.1"),
		LotSz:  Num("0.001"),
		MinSz:  Num("0.01"),
	})

	req, err := rules.PreparePlaceOrder(PlaceOrderRequest{
		InstID:  "BTC-USDT",
		TdMode:  TdCash,
		Side:    Sell,
		OrdType: OrdMarket,
		Sz:      "0.0109",
	})
	if err != nil {
		t.Fatal(err)
	}
	if req.Px != "" || req.Sz != "0.01" {
		t.Fatalf("market prepared = %+v", req)
	}
}

func TestInstrumentRulesAlignmentAndParsing(t *testing.T) {
	rules := mustRules(t, Instrument{
		InstID:     "ETH-USDT",
		InstIDCode: 123,
		InstType:   InstSpot,
		TickSz:     Num("0.01"),
		LotSz:      Num("0.001"),
		MinSz:      Num("0.01"),
	})

	if rules.InstID != "ETH-USDT" || rules.InstIDCode != 123 || rules.InstType != InstSpot {
		t.Fatalf("rules metadata = %+v", rules)
	}
	if !rules.IsPriceAligned(decimal.RequireFromString("100.12")) {
		t.Fatal("aligned price reported invalid")
	}
	if rules.IsPriceAligned(decimal.RequireFromString("100.121")) {
		t.Fatal("misaligned price reported valid")
	}
	if rules.RoundSize(decimal.RequireFromString("1.2345")).String() != "1.234" {
		t.Fatalf("rounded size = %s, want 1.234", rules.RoundSize(decimal.RequireFromString("1.2345")))
	}
}

func mustRules(t *testing.T, inst Instrument) InstrumentRules {
	t.Helper()
	rules, err := inst.Rules()
	if err != nil {
		t.Fatal(err)
	}
	return rules
}
