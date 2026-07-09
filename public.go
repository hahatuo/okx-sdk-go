package okx

import (
	"context"
	"net/url"
)

// PublicService covers /api/v5/public/* (public reference data).
type PublicService struct{ c *Client }

// Instruments returns tradable instruments for a type. See the OKX "instrument
// configuration" best-practice note: cache these and refresh periodically
// rather than fetching per request.
//
// GET /api/v5/public/instruments
func (s *PublicService) Instruments(ctx context.Context, req InstrumentsRequest) ([]Instrument, error) {
	if req.InstType == "" {
		return nil, required("instType")
	}
	q := url.Values{"instType": {string(req.InstType)}}
	setIfNotEmpty(q, "seriesId", req.SeriesID)
	setIfNotEmpty(q, "uly", req.Uly)
	setIfNotEmpty(q, "instFamily", req.InstFamily)
	setIfNotEmpty(q, "instId", req.InstID)
	return executeGet[Instrument](ctx, s.c, "/api/v5/public/instruments", q, "public:public:instruments", false)
}

// SystemTime returns OKX system time.
//
// GET /api/v5/public/time
func (s *PublicService) SystemTime(ctx context.Context) ([]SystemTime, error) {
	return executeGet[SystemTime](ctx, s.c, "/api/v5/public/time", nil, "public:public:time", false)
}

// Underlying returns instrument family values for derivatives.
//
// GET /api/v5/public/underlying
func (s *PublicService) Underlying(ctx context.Context, instType InstType) ([]Underlying, error) {
	if instType == "" {
		return nil, required("instType")
	}
	q := url.Values{"instType": {string(instType)}}
	return executeGet[Underlying](ctx, s.c, "/api/v5/public/underlying", q, "public:public:underlying:"+string(instType), false)
}

// FundingRate returns the current funding-rate state for a contract.
//
// GET /api/v5/public/funding-rate
func (s *PublicService) FundingRate(ctx context.Context, instID string) ([]FundingRate, error) {
	if instID == "" {
		return nil, required("instId")
	}
	q := url.Values{"instId": {instID}}
	return executeGet[FundingRate](ctx, s.c, "/api/v5/public/funding-rate", q, "public:public:funding-rate:"+instID, false)
}
