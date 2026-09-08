package okx

import (
	"fmt"

	json "github.com/go-json-experiment/json"
)

// Request structs keep query-only fields untagged where possible; body structs
// use omitzero so optional OKX fields disappear from JSON v2 output.

// ---- Account ----

type BalanceRequest struct {
	Ccy string
}

type Balance struct {
	TotalEq              Num    `json:"totalEq"`
	UTime                string `json:"uTime"`
	AvailEq              Num    `json:"availEq,omitzero"`
	NotionalUSDForBorrow Num    `json:"notionalUsdForBorrow,omitzero"`
	CommonBalanceInfo
	Details []BalanceDetail `json:"details"`
}

type CommonBalanceInfo struct {
	IsoEq       Num `json:"isoEq,omitzero"`
	AdjEq       Num `json:"adjEq,omitzero"`
	OrdFroz     Num `json:"ordFroz,omitzero"`
	IMR         Num `json:"imr,omitzero"`
	MMR         Num `json:"mmr,omitzero"`
	BorrowFroz  Num `json:"borrowFroz,omitzero"`
	MgnRatio    Num `json:"mgnRatio,omitzero"`
	NotionalUSD Num `json:"notionalUsd,omitzero"`
}

type BalanceDetail struct {
	CommonBalanceDetailInfo
	EqUSD             Num  `json:"eqUsd,omitzero"`
	IMR               Num  `json:"imr,omitzero"`
	MMR               Num  `json:"mmr,omitzero"`
	NotionalLever     Num  `json:"notionalLever,omitzero"`
	MaxLoan           Num  `json:"maxLoan,omitzero"`
	SpotInUseAmt      Num  `json:"spotInUseAmt,omitzero"`
	ClSpotInUseAmt    Num  `json:"clSpotInUseAmt,omitzero"`
	MaxSpotInUse      Num  `json:"maxSpotInUse,omitzero"`
	SpotIsoBal        Num  `json:"spotIsoBal,omitzero"`
	SpotBal           Num  `json:"spotBal,omitzero"`
	OpenAvgPx         Num  `json:"openAvgPx,omitzero"`
	AccAvgPx          Num  `json:"accAvgPx,omitzero"`
	SpotUPLRatio      Num  `json:"spotUplRatio,omitzero"`
	TotalPnL          Num  `json:"totalPnl,omitzero"`
	TotalPnLRatio     Num  `json:"totalPnlRatio,omitzero"`
	CollateralEnabled bool `json:"collateralEnabled,omitzero"`
	// Deprecated: use ColRes for the full collateral restriction status.
	CollateralRestrict bool `json:"collateralRestrict,omitzero"`
}

type CommonBalanceDetailInfo struct {
	ColRes    string `json:"colRes,omitzero"`
	Ccy       string `json:"ccy"`
	Eq        Num    `json:"eq"`
	CashBal   Num    `json:"cashBal,omitzero"`
	UTime     string `json:"uTime,omitzero"`
	IsoEq     Num    `json:"isoEq,omitzero"`
	AvailEq   Num    `json:"availEq,omitzero"`
	AvailBal  Num    `json:"availBal,omitzero"`
	FrozenBal Num    `json:"frozenBal,omitzero"`
	OrdFrozen Num    `json:"ordFrozen,omitzero"`
	DisEq     Num    `json:"disEq,omitzero"`
	FixedBal  Num    `json:"fixedBal,omitzero"`
	Liab      Num    `json:"liab,omitzero"`
	UPL       Num    `json:"upl,omitzero"`
	UPLLiab   Num    `json:"uplLiab,omitzero"`
	CrossLiab Num    `json:"crossLiab,omitzero"`
	IsoLiab   Num    `json:"isoLiab,omitzero"`
	MgnRatio  Num    `json:"mgnRatio,omitzero"`
	Interest  Num    `json:"interest,omitzero"`
	SpotUPL   Num    `json:"spotUpl,omitzero"`
}

type AccountUpdate struct {
	UTime   string `json:"uTime"`
	TotalEq Num    `json:"totalEq"`
	CommonBalanceInfo
	Details []AccountUpdateDetail `json:"details"`
}

type AccountUpdateDetail struct {
	CommonBalanceDetailInfo
	TWAP          Num `json:"twap,omitzero"`
	MaxLoan       Num `json:"maxLoan,omitzero"`
	EqUSD         Num `json:"eqUsd,omitzero"`
	NotionalLever Num `json:"notionalLever,omitzero"`
}

type PositionsRequest struct {
	InstType InstType
	InstID   string
	PosID    string
}

type Position struct {
	InstType    InstType `json:"instType"`
	InstID      string   `json:"instId"`
	PosID       string   `json:"posId,omitzero"`
	PosSide     PosSide  `json:"posSide"`
	Pos         Num      `json:"pos"`
	BaseBal     Num      `json:"baseBal,omitzero"`
	QuoteBal    Num      `json:"quoteBal,omitzero"`
	PosCcy      string   `json:"posCcy,omitzero"`
	AvailPos    Num      `json:"availPos,omitzero"`
	AvgPx       Num      `json:"avgPx,omitzero"`
	UPL         Num      `json:"upl,omitzero"`
	UPLRatio    Num      `json:"uplRatio,omitzero"`
	Lever       Num      `json:"lever,omitzero"`
	LiqPx       Num      `json:"liqPx,omitzero"`
	MarkPx      Num      `json:"markPx,omitzero"`
	IMR         Num      `json:"imr,omitzero"`
	MMR         Num      `json:"mmr,omitzero"`
	Margin      Num      `json:"margin,omitzero"`
	MgnMode     MgnMode  `json:"mgnMode,omitzero"`
	MgnRatio    Num      `json:"mgnRatio,omitzero"`
	MgnCcy      string   `json:"mgnCcy,omitzero"`
	NotionalUSD Num      `json:"notionalUsd,omitzero"`
	ADL         string   `json:"adl,omitzero"`
	Ccy         string   `json:"ccy,omitzero"`
	CTime       string   `json:"cTime,omitzero"`
	UTime       string   `json:"uTime,omitzero"`
}

type MaxAvailSizeRequest struct {
	InstID        string
	Ccy           string
	TdMode        TdMode
	ReduceOnly    *bool
	Px            string
	TradeQuoteCcy string
}

type MaxAvailSize struct {
	InstID  string `json:"instId"`
	Ccy     string `json:"ccy"`
	MaxBuy  Num    `json:"maxBuy"`
	MaxSell Num    `json:"maxSell"`
}

type SetLeverageRequest struct {
	InstID  string  `json:"instId,omitzero"`
	Ccy     string  `json:"ccy,omitzero"`
	Lever   string  `json:"lever"`
	MgnMode MgnMode `json:"mgnMode"`
	PosSide PosSide `json:"posSide,omitzero"`
}

type Leverage struct {
	Lever   Num     `json:"lever"`
	MgnMode MgnMode `json:"mgnMode"`
	InstID  string  `json:"instId"`
	PosSide PosSide `json:"posSide"`
}

type SetFeeTypeRequest struct {
	FeeType string `json:"feeType"`
}

type FeeType struct {
	FeeType string `json:"feeType"`
}

// ---- Market ----

type TickersRequest struct {
	InstType   InstType
	InstFamily string
	// Deprecated: use InstFamily.
	Uly string
}

type TickerRequest struct {
	InstID string
}

type Ticker struct {
	InstType  InstType `json:"instType"`
	InstID    string   `json:"instId"`
	Last      Num      `json:"last"`
	LastSz    Num      `json:"lastSz,omitzero"`
	AskPx     Num      `json:"askPx"`
	AskSz     Num      `json:"askSz,omitzero"`
	BidPx     Num      `json:"bidPx"`
	BidSz     Num      `json:"bidSz,omitzero"`
	Open24h   Num      `json:"open24h,omitzero"`
	High24h   Num      `json:"high24h,omitzero"`
	Low24h    Num      `json:"low24h,omitzero"`
	VolCcy24h Num      `json:"volCcy24h,omitzero"`
	Vol24h    Num      `json:"vol24h"`
	TS        string   `json:"ts"`
}

type OrderBookRequest struct {
	InstID string
	Size   int
}

type TradesRequest struct {
	InstID string
	Limit  int
}

type HistoryTradesRequest struct {
	InstID string
	Type   string
	After  string
	Before string
	Limit  int
}

type MarketTrade struct {
	InstID  string `json:"instId"`
	TradeID string `json:"tradeId"`
	Px      Num    `json:"px"`
	Sz      Num    `json:"sz"`
	Side    Side   `json:"side"`
	Source  string `json:"source,omitzero"`
	Count   Num    `json:"count,omitzero"`
	TS      string `json:"ts"`
}

type BookLevel [4]string

func (l BookLevel) Price() string { return l[0] }
func (l BookLevel) Size() string  { return l[1] }

func (l BookLevel) ValidPrice() bool {
	return l[0] != ""
}

func (l BookLevel) IsDelete() bool {
	return isZeroDecimal(l[1])
}

type OrderBook struct {
	Asks []BookLevel `json:"asks"`
	Bids []BookLevel `json:"bids"`
	TS   string      `json:"ts"`
	// Deprecated: OKX fixes this field to zero; use SeqID/PrevSeqID for continuity.
	Checksum  Num `json:"checksum,omitzero"`
	SeqID     Num `json:"seqId,omitzero"`
	PrevSeqID Num `json:"prevSeqId,omitzero"`
}

type CandlesRequest struct {
	InstID  string
	Bar     string
	After   string
	Before  string
	Limit   int
	History bool
}

// Candle is a kline row. OKX sends candles as JSON arrays, not objects.
type Candle struct {
	TS          string
	Open        Num
	High        Num
	Low         Num
	Close       Num
	Vol         Num
	VolCcy      Num
	VolCcyQuote Num
	Confirm     string
}

func (c *Candle) UnmarshalJSON(b []byte) error {
	var row []string
	if err := json.Unmarshal(b, &row); err != nil {
		return err
	}
	if len(row) != 6 && len(row) != 9 {
		return fmt.Errorf("okx: unexpected candle row length %d", len(row))
	}
	c.TS, c.Open, c.High, c.Low, c.Close = row[0], Num(row[1]), Num(row[2]), Num(row[3]), Num(row[4])
	if len(row) == 6 {
		c.Confirm = row[5]
		c.Vol, c.VolCcy, c.VolCcyQuote = "", "", ""
		return nil
	}
	c.Vol, c.VolCcy, c.VolCcyQuote, c.Confirm = Num(row[5]), Num(row[6]), Num(row[7]), row[8]
	return nil
}

type IndexComponents struct {
	Index      string           `json:"index"`
	Last       Num              `json:"last"`
	TS         string           `json:"ts"`
	Components []IndexComponent `json:"components"`
}

type IndexComponent struct {
	Exch   string `json:"exch"`
	Symbol string `json:"symbol"`
	SymPx  Num    `json:"symPx"`
	CnvPx  Num    `json:"cnvPx"`
	Wgt    Num    `json:"wgt"`
}

type Platform24Volume struct {
	VolCny Num    `json:"volCny"`
	VolUsd Num    `json:"volUsd"`
	TS     string `json:"ts"`
}

// ---- Public ----

type InstrumentsRequest struct {
	InstType InstType
	SeriesID string
	// Deprecated: use InstFamily.
	Uly        string
	InstFamily string
	InstID     string
}

type Instrument struct {
	InstID     string   `json:"instId"`
	InstIDCode int64    `json:"instIdCode,omitzero"`
	InstType   InstType `json:"instType"`
	InstFamily string   `json:"instFamily,omitzero"`
	BaseCcy    string   `json:"baseCcy,omitzero"`
	QuoteCcy   string   `json:"quoteCcy,omitzero"`
	SettleCcy  string   `json:"settleCcy,omitzero"`
	CtVal      Num      `json:"ctVal,omitzero"`
	CtMult     Num      `json:"ctMult,omitzero"`
	CtValCcy   string   `json:"ctValCcy,omitzero"`
	OptType    string   `json:"optType,omitzero"`
	Stk        Num      `json:"stk,omitzero"`
	ListTime   string   `json:"listTime,omitzero"`
	ExpTime    string   `json:"expTime,omitzero"`
	Lever      Num      `json:"lever,omitzero"`
	TickSz     Num      `json:"tickSz"`
	LotSz      Num      `json:"lotSz"`
	MinSz      Num      `json:"minSz"`
	CtType     string   `json:"ctType,omitzero"`
	// Deprecated: use ExpTime.
	Alias string `json:"alias,omitzero"`
	State string `json:"state"`
}

type SystemTime struct {
	TS string `json:"ts"`
}

type Underlying struct {
	Uly string `json:"uly"`
}

type FundingRate struct {
	InstType    InstType `json:"instType"`
	InstID      string   `json:"instId"`
	FundingRate Num      `json:"fundingRate"`
	FundingTime string   `json:"fundingTime"`
	// Deprecated: OKX no longer supports the next-period funding rate.
	NextFundingRate Num    `json:"nextFundingRate"`
	NextFundingTime string `json:"nextFundingTime"`
}

// ---- Trade ----

type AttachAlgoOrder struct {
	AttachAlgoID         string `json:"attachAlgoId,omitzero"`
	AttachAlgoClOrdID    string `json:"attachAlgoClOrdId,omitzero"`
	TpTriggerPx          string `json:"tpTriggerPx,omitzero"`
	TpTriggerRatio       string `json:"tpTriggerRatio,omitzero"`
	TpOrdPx              string `json:"tpOrdPx,omitzero"`
	TpOrdKind            string `json:"tpOrdKind,omitzero"`
	SlTriggerPx          string `json:"slTriggerPx,omitzero"`
	SlTriggerRatio       string `json:"slTriggerRatio,omitzero"`
	SlOrdPx              string `json:"slOrdPx,omitzero"`
	TpTriggerPxType      string `json:"tpTriggerPxType,omitzero"`
	SlTriggerPxType      string `json:"slTriggerPxType,omitzero"`
	Sz                   string `json:"sz,omitzero"`
	AmendPxOnTriggerType string `json:"amendPxOnTriggerType,omitzero"`
	CallbackRatio        string `json:"callbackRatio,omitzero"`
	CallbackSpread       string `json:"callbackSpread,omitzero"`
	ActivePx             string `json:"activePx,omitzero"`
}

type PlaceOrderRequest struct {
	InstID     string  `json:"instId,omitzero"`
	InstIDCode int64   `json:"instIdCode,omitzero"`
	TdMode     TdMode  `json:"tdMode"`
	Ccy        string  `json:"ccy,omitzero"`
	ClOrdID    string  `json:"clOrdId,omitzero"`
	Tag        string  `json:"tag,omitzero"`
	Side       Side    `json:"side"`
	PosSide    PosSide `json:"posSide,omitzero"`
	OrdType    OrdType `json:"ordType"`
	Sz         string  `json:"sz"`
	Px         string  `json:"px,omitzero"`
	// Ignored by REST Place order since 2026-07-24. Other endpoint documentation
	// still lists this parameter; retained for those shared request contracts.
	SpeedBump      string            `json:"speedBump,omitzero"`
	ReduceOnly     *bool             `json:"reduceOnly,omitzero"`
	TgtCcy         string            `json:"tgtCcy,omitzero"`
	BanAmend       *bool             `json:"banAmend,omitzero"`
	PxAmendType    string            `json:"pxAmendType,omitzero"`
	TradeQuoteCcy  string            `json:"tradeQuoteCcy,omitzero"`
	SlippagePct    string            `json:"slippagePct,omitzero"`
	STPMode        string            `json:"stpMode,omitzero"`
	AttachAlgoOrds []AttachAlgoOrder `json:"attachAlgoOrds,omitzero"`
}

type OrderAck struct {
	OrdID   string `json:"ordId"`
	ClOrdID string `json:"clOrdId"`
	Tag     string `json:"tag,omitzero"`
	TS      string `json:"ts,omitzero"`
	ReqID   string `json:"reqId,omitzero"`
	SCode   string `json:"sCode"`
	SMsg    string `json:"sMsg"`
	SubCode string `json:"subCode,omitzero"`
}

// OK reports whether OKX accepted the individual order operation.
func (a OrderAck) OK() bool {
	return a.SCode == "" || a.SCode == "0"
}

// APIError returns the sentinel-wrapped OKX error represented by a rejected
// per-order ack. Accepted acks return nil.
func (a OrderAck) APIError() error {
	if a.OK() {
		return nil
	}
	return wrapAPIError(&APIError{Code: a.SCode, Msg: a.SMsg})
}

type CancelOrderRequest struct {
	InstID     string `json:"instId,omitzero"`
	InstIDCode int64  `json:"instIdCode,omitzero"`
	OrdID      string `json:"ordId,omitzero"`
	ClOrdID    string `json:"clOrdId,omitzero"`
}

type AmendOrderRequest struct {
	InstID         string                 `json:"instId,omitzero"`
	InstIDCode     int64                  `json:"instIdCode,omitzero"`
	CxlOnFail      *bool                  `json:"cxlOnFail,omitzero"`
	OrdID          string                 `json:"ordId,omitzero"`
	ClOrdID        string                 `json:"clOrdId,omitzero"`
	ReqID          string                 `json:"reqId,omitzero"`
	NewSz          string                 `json:"newSz,omitzero"`
	NewPx          string                 `json:"newPx,omitzero"`
	SpeedBump      string                 `json:"speedBump,omitzero"`
	NewPxUSD       string                 `json:"newPxUsd,omitzero"`
	NewPxVol       string                 `json:"newPxVol,omitzero"`
	PxAmendType    string                 `json:"pxAmendType,omitzero"`
	AttachAlgoOrds []AmendAttachAlgoOrder `json:"attachAlgoOrds,omitzero"`
}

type AmendAttachAlgoOrder struct {
	AttachAlgoID         string `json:"attachAlgoId,omitzero"`
	AttachAlgoClOrdID    string `json:"attachAlgoClOrdId,omitzero"`
	NewTpTriggerPx       string `json:"newTpTriggerPx,omitzero"`
	NewTpTriggerRatio    string `json:"newTpTriggerRatio,omitzero"`
	NewTpOrdPx           string `json:"newTpOrdPx,omitzero"`
	NewTpOrdKind         string `json:"newTpOrdKind,omitzero"`
	NewSlTriggerPx       string `json:"newSlTriggerPx,omitzero"`
	NewSlTriggerRatio    string `json:"newSlTriggerRatio,omitzero"`
	NewSlOrdPx           string `json:"newSlOrdPx,omitzero"`
	NewTpTriggerPxType   string `json:"newTpTriggerPxType,omitzero"`
	NewSlTriggerPxType   string `json:"newSlTriggerPxType,omitzero"`
	Sz                   string `json:"sz,omitzero"`
	AmendPxOnTriggerType string `json:"amendPxOnTriggerType,omitzero"`
	NewCallbackRatio     string `json:"newCallbackRatio,omitzero"`
	NewCallbackSpread    string `json:"newCallbackSpread,omitzero"`
	NewActivePx          string `json:"newActivePx,omitzero"`
}

type OrderRequest struct {
	InstID  string
	OrdID   string
	ClOrdID string
}

type OrdersPendingRequest struct {
	InstType   InstType
	InstFamily string
	InstID     string
	OrdType    OrdType
	State      string
	After      string
	Before     string
	Limit      int
}

type FillsHistoryRequest struct {
	InstType InstType
	// Deprecated: use InstFamily.
	Uly        string
	InstFamily string
	InstID     string
	OrdID      string
	After      string
	Before     string
	Begin      string
	End        string
	Limit      int
}

type Order struct {
	InstType       InstType          `json:"instType"`
	InstID         string            `json:"instId"`
	TgtCcy         string            `json:"tgtCcy,omitzero"`
	Ccy            string            `json:"ccy,omitzero"`
	OrdID          string            `json:"ordId"`
	ClOrdID        string            `json:"clOrdId"`
	Tag            string            `json:"tag,omitzero"`
	Px             Num               `json:"px,omitzero"`
	PxUSD          Num               `json:"pxUsd,omitzero"`
	PxVol          Num               `json:"pxVol,omitzero"`
	Sz             Num               `json:"sz"`
	NotionalUSD    Num               `json:"notionalUsd,omitzero"`
	PnL            Num               `json:"pnl,omitzero"`
	OrdType        OrdType           `json:"ordType"`
	Side           Side              `json:"side"`
	PosSide        PosSide           `json:"posSide,omitzero"`
	TdMode         TdMode            `json:"tdMode"`
	AccFillSz      Num               `json:"accFillSz,omitzero"`
	FillPx         Num               `json:"fillPx,omitzero"`
	TradeID        string            `json:"tradeId,omitzero"`
	FillSz         Num               `json:"fillSz,omitzero"`
	FillTime       string            `json:"fillTime,omitzero"`
	AvgPx          Num               `json:"avgPx,omitzero"`
	State          string            `json:"state"`
	Lever          Num               `json:"lever,omitzero"`
	AttachAlgoOrds []AttachAlgoOrder `json:"attachAlgoOrds,omitzero"`
	Fee            Num               `json:"fee,omitzero"`
	FeeCcy         string            `json:"feeCcy,omitzero"`
	Rebate         Num               `json:"rebate,omitzero"`
	RebateCcy      string            `json:"rebateCcy,omitzero"`
	ReduceOnly     string            `json:"reduceOnly,omitzero"`
	ReqID          string            `json:"reqId,omitzero"`
	AmendResult    string            `json:"amendResult,omitzero"`
	CancelSource   string            `json:"cancelSource,omitzero"`
	Source         string            `json:"source,omitzero"`
	Category       string            `json:"category,omitzero"`
	LastPx         Num               `json:"lastPx,omitzero"`
	CTime          string            `json:"cTime,omitzero"`
	UTime          string            `json:"uTime,omitzero"`
}

func (o Order) NormalizedState() OrderState {
	return NormalizeOrderState(o.State)
}

type Fill struct {
	InstType        InstType `json:"instType"`
	InstID          string   `json:"instId"`
	TradeID         string   `json:"tradeId"`
	OrdID           string   `json:"ordId"`
	ClOrdID         string   `json:"clOrdId,omitzero"`
	BillID          string   `json:"billId,omitzero"`
	Tag             string   `json:"tag,omitzero"`
	FillPx          Num      `json:"fillPx"`
	FillSz          Num      `json:"fillSz"`
	FillIdxPx       Num      `json:"fillIdxPx,omitzero"`
	FillPnl         Num      `json:"fillPnl,omitzero"`
	Side            Side     `json:"side"`
	PosSide         PosSide  `json:"posSide,omitzero"`
	ExecType        string   `json:"execType"`
	FeeCcy          string   `json:"feeCcy"`
	Fee             Num      `json:"fee"`
	TS              string   `json:"ts"`
	FillTime        string   `json:"fillTime,omitzero"`
	FillNotionalUSD Num      `json:"fillNotionalUsd,omitzero"`
}

// ---- Asset ----

type Currency struct {
	Ccy         string `json:"ccy"`
	Chain       string `json:"chain,omitzero"`
	CanDep      bool   `json:"canDep,omitzero"`
	CanWd       bool   `json:"canWd,omitzero"`
	CanInternal bool   `json:"canInternal,omitzero"`
	MinDep      Num    `json:"minDep,omitzero"`
	MinWd       Num    `json:"minWd,omitzero"`
	Fee         Num    `json:"fee,omitzero"`
	// Deprecated: use Fee, the fixed withdrawal fee.
	MinFee Num `json:"minFee,omitzero"`
	// Deprecated: use Fee, the fixed withdrawal fee.
	MaxFee Num `json:"maxFee,omitzero"`
}

type AssetBalance struct {
	Ccy       string `json:"ccy"`
	Bal       Num    `json:"bal"`
	AvailBal  Num    `json:"availBal"`
	FrozenBal Num    `json:"frozenBal"`
}

type TransferRequest struct {
	Ccy         string      `json:"ccy"`
	Amt         string      `json:"amt"`
	From        AccountType `json:"from"`
	To          AccountType `json:"to"`
	Type        string      `json:"type,omitzero"`
	SubAcct     string      `json:"subAcct,omitzero"`
	InstID      string      `json:"instId,omitzero"`
	ToInstID    string      `json:"toInstId,omitzero"`
	ClientID    string      `json:"clientId,omitzero"`
	LoanTrans   *bool       `json:"loanTrans,omitzero"`
	OmitPosRisk string      `json:"omitPosRisk,omitzero"`
}

type Transfer struct {
	TransID  string      `json:"transId"`
	Ccy      string      `json:"ccy,omitzero"`
	Amt      Num         `json:"amt,omitzero"`
	From     AccountType `json:"from,omitzero"`
	To       AccountType `json:"to,omitzero"`
	ClientID string      `json:"clientId,omitzero"`
}

// ---- Shared wire helpers ----

func isZeroDecimal(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '+', '.', '0':
		default:
			return false
		}
	}
	return true
}
