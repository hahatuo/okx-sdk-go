package okx

import (
	"context"
	"fmt"
	"net/url"
)

const MaxBatchOrderRequests = 20

// TradeService covers /api/v5/trade/* (private).
type TradeService struct{ c *Client }

// PlaceOrder submits a single order. OKX returns a one-element data array; the
// per-order sCode must be checked ("0" == accepted) in addition to the error.
//
// POST /api/v5/trade/order
func (s *TradeService) PlaceOrder(ctx context.Context, req PlaceOrderRequest) (OrderAck, error) {
	if err := validatePlaceOrder(req); err != nil {
		return OrderAck{}, err
	}
	out, err := execute[[]OrderAck](ctx, s.c, requestSpec{
		method:      "POST",
		path:        "/api/v5/trade/order",
		body:        req,
		auth:        true,
		rateLimited: true,
	})
	if err != nil {
		if len(out) > 0 {
			return out[0], err
		}
		return OrderAck{}, err
	}
	if len(out) == 0 {
		return OrderAck{}, ErrNotFound
	}
	return out[0], nil
}

// PlaceMultipleOrders submits a batch of orders.
//
// POST /api/v5/trade/batch-orders
func (s *TradeService) PlaceMultipleOrders(ctx context.Context, req []PlaceOrderRequest) ([]OrderAck, error) {
	return executeMultipleTradeOrders(ctx, s, req, "/api/v5/trade/batch-orders", validatePlaceOrder)
}

// CancelOrder cancels one incomplete order.
//
// POST /api/v5/trade/cancel-order
func (s *TradeService) CancelOrder(ctx context.Context, req CancelOrderRequest) ([]OrderAck, error) {
	return executeTradeOrder(ctx, s, req, "/api/v5/trade/cancel-order", validateCancelOrder)
}

// CancelMultipleOrders cancels a batch of incomplete orders.
//
// POST /api/v5/trade/cancel-batch-orders
func (s *TradeService) CancelMultipleOrders(ctx context.Context, req []CancelOrderRequest) ([]OrderAck, error) {
	return executeMultipleTradeOrders(ctx, s, req, "/api/v5/trade/cancel-batch-orders", validateCancelOrder)
}

// AmendOrder amends one incomplete order.
//
// POST /api/v5/trade/amend-order
func (s *TradeService) AmendOrder(ctx context.Context, req AmendOrderRequest) ([]OrderAck, error) {
	return executeTradeOrder(ctx, s, req, "/api/v5/trade/amend-order", validateAmendOrder)
}

// AmendMultipleOrders amends a batch of incomplete orders.
//
// POST /api/v5/trade/amend-batch-orders
func (s *TradeService) AmendMultipleOrders(ctx context.Context, req []AmendOrderRequest) ([]OrderAck, error) {
	return executeMultipleTradeOrders(ctx, s, req, "/api/v5/trade/amend-batch-orders", validateAmendOrder)
}

func executeMultipleTradeOrders[T any](ctx context.Context, s *TradeService, req []T, path string, validate func(T) error) ([]OrderAck, error) {
	if len(req) == 0 {
		return nil, required("orders")
	}
	if len(req) > MaxBatchOrderRequests {
		return nil, fmt.Errorf("%w: batch exceeds %d orders", ErrInvalidParameter, MaxBatchOrderRequests)
	}
	for i := range req {
		if err := validate(req[i]); err != nil {
			return nil, err
		}
	}
	return execute[[]OrderAck](ctx, s.c, requestSpec{
		method:      "POST",
		path:        path,
		body:        req,
		auth:        true,
		rateLimited: true,
	})
}

func executeTradeOrder[T any](ctx context.Context, s *TradeService, req T, path string, validate func(T) error) ([]OrderAck, error) {
	if err := validate(req); err != nil {
		return nil, err
	}
	return execute[[]OrderAck](ctx, s.c, requestSpec{
		method:      "POST",
		path:        path,
		body:        req,
		auth:        true,
		rateLimited: true,
	})
}

// Order retrieves one order by ordId or clOrdId.
//
// GET /api/v5/trade/order
func (s *TradeService) Order(ctx context.Context, req OrderRequest) ([]Order, error) {
	if req.InstID == "" {
		return nil, required("instId")
	}
	if req.OrdID == "" && req.ClOrdID == "" {
		return nil, required("ordId or clOrdId")
	}
	q := url.Values{"instId": {req.InstID}}
	setIfNotEmpty(q, "ordId", req.OrdID)
	setIfNotEmpty(q, "clOrdId", req.ClOrdID)
	return executeGet[Order](ctx, s.c, "/api/v5/trade/order", q, true)
}

// OrdersPending retrieves live and partially-filled orders.
//
// GET /api/v5/trade/orders-pending
func (s *TradeService) OrdersPending(ctx context.Context, req OrdersPendingRequest) ([]Order, error) {
	q := url.Values{}
	setIfNotEmpty(q, "instType", string(req.InstType))
	setIfNotEmpty(q, "instFamily", req.InstFamily)
	setIfNotEmpty(q, "instId", req.InstID)
	setIfNotEmpty(q, "ordType", string(req.OrdType))
	setIfNotEmpty(q, "state", req.State)
	setIfNotEmpty(q, "after", req.After)
	setIfNotEmpty(q, "before", req.Before)
	if req.Limit > 0 {
		q.Set("limit", itoa(req.Limit))
	}
	return executeGet[Order](ctx, s.c, "/api/v5/trade/orders-pending", q, true)
}

// FillsHistory retrieves recent filled order history.
//
// GET /api/v5/trade/fills-history
func (s *TradeService) FillsHistory(ctx context.Context, req FillsHistoryRequest) ([]Fill, error) {
	q := url.Values{}
	setIfNotEmpty(q, "instType", string(req.InstType))
	setIfNotEmpty(q, "uly", req.Uly)
	setIfNotEmpty(q, "instFamily", req.InstFamily)
	setIfNotEmpty(q, "instId", req.InstID)
	setIfNotEmpty(q, "ordId", req.OrdID)
	setIfNotEmpty(q, "after", req.After)
	setIfNotEmpty(q, "before", req.Before)
	setIfNotEmpty(q, "begin", req.Begin)
	setIfNotEmpty(q, "end", req.End)
	if req.Limit > 0 {
		q.Set("limit", itoa(req.Limit))
	}
	return executeGet[Fill](ctx, s.c, "/api/v5/trade/fills-history", q, true)
}

func validatePlaceOrder(req PlaceOrderRequest) error {
	if req.InstIDCode == 0 && req.InstID == "" {
		return required("instId")
	}
	if req.TdMode == "" {
		return required("tdMode")
	}
	if req.Side == "" {
		return required("side")
	}
	if req.OrdType == "" {
		return required("ordType")
	}
	if req.Sz == "" {
		return required("sz")
	}
	return nil
}

func validateCancelOrder(req CancelOrderRequest) error {
	if req.InstIDCode == 0 && req.InstID == "" {
		return required("instId")
	}
	if req.OrdID == "" && req.ClOrdID == "" {
		return required("ordId or clOrdId")
	}
	return nil
}

func validateAmendOrder(req AmendOrderRequest) error {
	if req.InstIDCode == 0 && req.InstID == "" {
		return required("instId")
	}
	if req.OrdID == "" && req.ClOrdID == "" {
		return required("ordId or clOrdId")
	}
	if req.NewSz == "" && req.NewPx == "" && req.NewPxUSD == "" && req.NewPxVol == "" && len(req.AttachAlgoOrds) == 0 {
		return required("newSz or newPx")
	}
	return nil
}
