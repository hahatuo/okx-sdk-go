package okx

import (
	"context"
	"encoding/json"
)

type WSTypedMessage[T any] struct {
	Arg    map[string]string
	Action string
	Data   []T
	Raw    json.RawMessage
	Err    error
}

type OrdersSubscriptionRequest struct {
	InstType   string
	InstFamily string
	InstID     string
}

func (c *WSClient) SubscribeTickers(ctx context.Context, instID string) (<-chan WSTypedMessage[Ticker], error) {
	return subscribeTyped(ctx, c, Subscription{
		Channel: "tickers",
		Args:    map[string]string{"instId": instID},
	}, decodeTypedWSMessage[Ticker])
}

func (c *WSClient) SubscribeOrderBook(ctx context.Context, channel, instID string) (<-chan WSTypedMessage[OrderBook], error) {
	return subscribeTyped(ctx, c, Subscription{
		Channel: channel,
		Args:    map[string]string{"instId": instID},
	}, decodeTypedWSMessage[OrderBook])
}

func (c *WSClient) SubscribeOrders(ctx context.Context, req OrdersSubscriptionRequest) (<-chan WSTypedMessage[Order], error) {
	instType := req.InstType
	if instType == "" {
		instType = "ANY"
	}
	args := map[string]string{"instType": instType}
	if req.InstFamily != "" {
		args["instFamily"] = req.InstFamily
	}
	if req.InstID != "" {
		args["instId"] = req.InstID
	}
	return subscribeTyped(ctx, c, Subscription{
		Channel: "orders",
		Args:    args,
	}, decodeTypedWSMessage[Order])
}

func subscribeTyped[T any](
	ctx context.Context,
	c *WSClient,
	sub Subscription,
	decode func(WSMessage) WSTypedMessage[T],
) (<-chan WSTypedMessage[T], error) {
	rawCh, err := c.Subscribe(ctx, sub)
	if err != nil {
		return nil, err
	}
	out := make(chan WSTypedMessage[T], capHint(rawCh))
	go func() {
		defer close(out)
		for msg := range rawCh {
			select {
			case out <- decode(msg):
			case <-c.done:
				return
			}
		}
	}()
	return out, nil
}

func decodeTypedWSMessage[T any](msg WSMessage) WSTypedMessage[T] {
	out := WSTypedMessage[T]{
		Arg:    msg.Arg,
		Action: msg.Action,
		Raw:    msg.Raw,
	}
	if len(msg.Data) == 0 {
		return out
	}
	if err := json.Unmarshal(msg.Data, &out.Data); err != nil {
		out.Err = err
	}
	return out
}

func capHint[T any](ch <-chan T) int {
	if cap(ch) <= 0 {
		return 1
	}
	return cap(ch)
}
