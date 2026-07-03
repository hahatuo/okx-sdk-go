package okx

import (
	"bytes"
	"encoding/json"
	"fmt"
)

type Balance struct {
	TotalEq string          `json:"totalEq"`
	UTime   string          `json:"uTime"`
	Details []BalanceDetail `json:"details"`
}

type BalanceDetail struct {
	Ccy      string `json:"ccy"`
	Eq       string `json:"eq"`
	AvailBal string `json:"availBal"`
	CashBal  string `json:"cashBal"`
	UTime    string `json:"uTime"`
}

type Position struct {
	InstType string `json:"instType"`
	InstID   string `json:"instId"`
	MgnMode  string `json:"mgnMode"`
	PosSide  string `json:"posSide"`
	Pos      string `json:"pos"`
	AvgPx    string `json:"avgPx"`
	Upl      string `json:"upl"`
	UTime    string `json:"uTime"`
}

type PlaceOrderRequest struct {
	InstID         string            `json:"instId"`
	TdMode         string            `json:"tdMode"`
	Side           string            `json:"side"`
	OrdType        string            `json:"ordType"`
	Sz             string            `json:"sz"`
	Ccy            string            `json:"ccy,omitempty"`
	ClOrdID        string            `json:"clOrdId,omitempty"`
	Tag            string            `json:"tag,omitempty"`
	PosSide        string            `json:"posSide,omitempty"`
	Px             string            `json:"px,omitempty"`
	ReduceOnly     *bool             `json:"reduceOnly,omitempty"`
	TgtCcy         string            `json:"tgtCcy,omitempty"`
	AttachAlgoOrds []AttachAlgoOrder `json:"attachAlgoOrds,omitempty"`
}

type AttachAlgoOrder struct {
	AttachAlgoClOrdID string `json:"attachAlgoClOrdId,omitempty"`
	TpTriggerPx       string `json:"tpTriggerPx,omitempty"`
	TpOrdPx           string `json:"tpOrdPx,omitempty"`
	TpTriggerPxType   string `json:"tpTriggerPxType,omitempty"`
	SlTriggerPx       string `json:"slTriggerPx,omitempty"`
	SlOrdPx           string `json:"slOrdPx,omitempty"`
	SlTriggerPxType   string `json:"slTriggerPxType,omitempty"`
	Sz                string `json:"sz,omitempty"`
}

type OrderAck struct {
	ClOrdID string `json:"clOrdId"`
	OrdID   string `json:"ordId"`
	Tag     string `json:"tag"`
	SCode   string `json:"sCode"`
	SMsg    string `json:"sMsg"`
	ReqID   string `json:"reqId"`
	TS      string `json:"ts"`
}

type CancelOrderRequest struct {
	InstID  string `json:"instId"`
	OrdID   string `json:"ordId,omitempty"`
	ClOrdID string `json:"clOrdId,omitempty"`
}

type AmendOrderRequest struct {
	InstID    string `json:"instId"`
	CxlOnFail *bool  `json:"cxlOnFail,omitempty"`
	OrdID     string `json:"ordId,omitempty"`
	ClOrdID   string `json:"clOrdId,omitempty"`
	ReqID     string `json:"reqId,omitempty"`
	NewSz     string `json:"newSz,omitempty"`
	NewPx     string `json:"newPx,omitempty"`
}

type Order struct {
	InstType        string `json:"instType"`
	InstID          string `json:"instId"`
	TgtCcy          string `json:"tgtCcy"`
	Ccy             string `json:"ccy"`
	OrdID           string `json:"ordId"`
	ClOrdID         string `json:"clOrdId"`
	Tag             string `json:"tag"`
	Px              string `json:"px"`
	Sz              string `json:"sz"`
	NotionalUsd     string `json:"notionalUsd"`
	OrdType         string `json:"ordType"`
	Side            string `json:"side"`
	PosSide         string `json:"posSide"`
	TdMode          string `json:"tdMode"`
	FillPx          string `json:"fillPx"`
	TradeID         string `json:"tradeId"`
	FillSz          string `json:"fillSz"`
	FillPnl         string `json:"fillPnl"`
	FillTime        string `json:"fillTime"`
	FillFee         string `json:"fillFee"`
	FillFeeCcy      string `json:"fillFeeCcy"`
	ExecType        string `json:"execType"`
	AccFillSz       string `json:"accFillSz"`
	FillNotionalUsd string `json:"fillNotionalUsd"`
	AvgPx           string `json:"avgPx"`
	State           string `json:"state"`
	Lever           string `json:"lever"`
	ReduceOnly      string `json:"reduceOnly"`
	Pnl             string `json:"pnl"`
	Source          string `json:"source"`
	Category        string `json:"category"`
	Fee             string `json:"fee"`
	FeeCcy          string `json:"feeCcy"`
	Rebate          string `json:"rebate"`
	RebateCcy       string `json:"rebateCcy"`
	UTime           string `json:"uTime"`
	CTime           string `json:"cTime"`
	Code            string `json:"code"`
	Msg             string `json:"msg"`
}

type Ticker struct {
	InstType  string `json:"instType"`
	InstID    string `json:"instId"`
	Last      string `json:"last"`
	LastSz    string `json:"lastSz"`
	AskPx     string `json:"askPx"`
	AskSz     string `json:"askSz"`
	BidPx     string `json:"bidPx"`
	BidSz     string `json:"bidSz"`
	Open24h   string `json:"open24h"`
	High24h   string `json:"high24h"`
	Low24h    string `json:"low24h"`
	VolCcy24h string `json:"volCcy24h"`
	Vol24h    string `json:"vol24h"`
	TS        string `json:"ts"`
}

type OrderBook struct {
	Asks      [][]string     `json:"asks"`
	Bids      [][]string     `json:"bids"`
	TS        string         `json:"ts"`
	Checksum  StringOrNumber `json:"checksum,omitempty"`
	SeqID     StringOrNumber `json:"seqId,omitempty"`
	PrevSeqID StringOrNumber `json:"prevSeqId,omitempty"`
}

type StringOrNumber string

func (v *StringOrNumber) UnmarshalJSON(data []byte) error {
	data = bytes.TrimSpace(data)
	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		*v = ""
		return nil
	}
	if data[0] == '"' {
		var s string
		if err := json.Unmarshal(data, &s); err != nil {
			return err
		}
		*v = StringOrNumber(s)
		return nil
	}
	*v = StringOrNumber(string(data))
	return nil
}

func (v StringOrNumber) String() string {
	return string(v)
}

type Candle struct {
	TS          string `json:"ts"`
	O           string `json:"o"`
	H           string `json:"h"`
	L           string `json:"l"`
	C           string `json:"c"`
	Vol         string `json:"vol"`
	VolCcy      string `json:"volCcy"`
	VolCcyQuote string `json:"volCcyQuote"`
	Confirm     string `json:"confirm"`
}

func (c *Candle) UnmarshalJSON(data []byte) error {
	var row []string
	if err := json.Unmarshal(data, &row); err != nil {
		return err
	}
	if len(row) != 6 && len(row) != 9 {
		return fmt.Errorf("okx: unexpected candle row length %d", len(row))
	}
	c.TS, c.O, c.H, c.L, c.C = row[0], row[1], row[2], row[3], row[4]
	if len(row) == 6 {
		c.Confirm = row[5]
		return nil
	}
	c.Vol, c.VolCcy, c.VolCcyQuote, c.Confirm = row[5], row[6], row[7], row[8]
	return nil
}

type IndexComponents struct {
	Index      string           `json:"index"`
	Last       string           `json:"last"`
	TS         string           `json:"ts"`
	Components []IndexComponent `json:"components"`
}

type IndexComponent struct {
	Exch   string `json:"exch"`
	Symbol string `json:"symbol"`
	SymPx  string `json:"symPx"`
	CnvPx  string `json:"cnvPx"`
	Wgt    string `json:"wgt"`
}

type Platform24Volume struct {
	VolCny string `json:"volCny"`
	VolUsd string `json:"volUsd"`
	TS     string `json:"ts"`
}

type Instrument struct {
	InstType   string `json:"instType"`
	InstID     string `json:"instId"`
	InstFamily string `json:"instFamily"`
	BaseCcy    string `json:"baseCcy"`
	QuoteCcy   string `json:"quoteCcy"`
	SettleCcy  string `json:"settleCcy"`
	CtVal      string `json:"ctVal"`
	CtMult     string `json:"ctMult"`
	LotSz      string `json:"lotSz"`
	MinSz      string `json:"minSz"`
	TickSz     string `json:"tickSz"`
	State      string `json:"state"`
	ListTime   string `json:"listTime"`
	ExpTime    string `json:"expTime"`
}

type SystemTime struct {
	TS string `json:"ts"`
}

type FundingRate struct {
	InstType        string `json:"instType"`
	InstID          string `json:"instId"`
	FundingRate     string `json:"fundingRate"`
	FundingTime     string `json:"fundingTime"`
	NextFundingRate string `json:"nextFundingRate"`
	NextFundingTime string `json:"nextFundingTime"`
}

type Underlying struct {
	Uly string `json:"uly"`
}

type Currency struct {
	Ccy         string `json:"ccy"`
	Chain       string `json:"chain"`
	CanDep      bool   `json:"canDep"`
	CanWd       bool   `json:"canWd"`
	CanInternal bool   `json:"canInternal"`
	MinDep      string `json:"minDep"`
	MinWd       string `json:"minWd"`
	MinFee      string `json:"minFee"`
	MaxFee      string `json:"maxFee"`
}

type AssetBalance struct {
	Ccy       string `json:"ccy"`
	Bal       string `json:"bal"`
	FrozenBal string `json:"frozenBal"`
	AvailBal  string `json:"availBal"`
}

type MaxAvailSizeRequest struct {
	InstID        string
	Ccy           string
	TdMode        string
	ReduceOnly    *bool
	Px            string
	TradeQuoteCcy string
}

type MaxAvailSize struct {
	InstID  string `json:"instId"`
	Ccy     string `json:"ccy"`
	MaxBuy  string `json:"maxBuy"`
	MaxSell string `json:"maxSell"`
}

type SetLeverageRequest struct {
	InstID  string `json:"instId,omitempty"`
	Ccy     string `json:"ccy,omitempty"`
	Lever   string `json:"lever"`
	MgnMode string `json:"mgnMode"`
	PosSide string `json:"posSide,omitempty"`
}

type Leverage struct {
	Lever   string `json:"lever"`
	MgnMode string `json:"mgnMode"`
	InstID  string `json:"instId"`
	PosSide string `json:"posSide"`
}
