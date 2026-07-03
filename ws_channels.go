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

type AccountSubscriptionRequest struct {
	Ccy string
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

func (c *WSClient) SubscribeOrderBookDepth(ctx context.Context, instID string, depth int) (<-chan WSTypedMessage[OrderBook], error) {
	return c.SubscribeOrderBook(ctx, OrderBookChannel(depth), instID)
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

func (c *WSClient) SubscribeAccount(ctx context.Context, req AccountSubscriptionRequest) (<-chan WSTypedMessage[AccountUpdate], error) {
	args := map[string]string{}
	if req.Ccy != "" {
		args["ccy"] = req.Ccy
	}
	return subscribeTyped(ctx, c, Subscription{
		Channel: "account",
		Args:    args,
	}, decodeTypedWSMessage[AccountUpdate])
}

func OrderBookDepth(depth int) int {
	switch {
	case depth <= 1:
		return 1
	case depth <= 5:
		return 5
	case depth <= 50:
		return 50
	default:
		return 400
	}
}

func OrderBookChannel(depth int) string {
	switch OrderBookDepth(depth) {
	case 1:
		return "bbo-tbt"
	case 5:
		return "books5"
	case 50:
		return "books50-l2-tbt"
	default:
		return "books-l2-tbt"
	}
}

func OrderBookDepthFromChannel(channel string) int {
	switch channel {
	case "bbo-tbt":
		return 1
	case "books5":
		return 5
	case "books50", "books50-l2-tbt":
		return 50
	case "books", "books400", "books-l2-tbt":
		return 400
	default:
		return 0
	}
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
			default:
				if c != nil && c.logger != nil {
					c.logger.Warn("okx ws typed subscription channel full, dropping message", "channel", sub.Channel)
				}
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
