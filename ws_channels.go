package okx

import (
	"context"
	"strings"
)

const (
	WSChannelTickers      = "tickers"
	WSChannelTrades       = "trades"
	WSChannelTradesAll    = "trades-all"
	WSChannelCandle       = "candle"
	WSChannelMarkCandle   = "mark-price-candle"
	WSChannelIndexCandle  = "index-candle"
	WSChannelAccount      = "account"
	WSChannelOrders       = "orders"
	WSChannelBooks        = "books"
	WSChannelBooks5       = "books5"
	WSChannelBBO          = "bbo-tbt"
	WSChannelBooksL2TBT   = "books-l2-tbt"
	WSChannelBooks50L2TBT = "books50-l2-tbt"

	MaxOrderBookDepth = 400
)

// WSTypedMessage owns decoded Data; callers may retain it.
type WSTypedMessage[T any] struct {
	EventType string
	CurPage   int
	LastPage  bool
	Arg       WSChannelArg
	Action    string
	Data      []T
	Err       error
}

type WSTypedHandler[T any] func(WSTypedMessage[T])

// WSLocalOrderBookMessage is delivered by SubscribeLocalOrderBook*. Book is a
// mutable view owned by the WebSocket read loop and is only valid during the
// handler call. Copy the levels if they need to outlive the callback.
type WSLocalOrderBookMessage struct {
	Arg    WSChannelArg
	Action string
	Book   *LocalOrderBook
	Err    error
}

type WSLocalOrderBookHandler func(WSLocalOrderBookMessage)

type OrdersSubscriptionRequest struct {
	InstType   InstType
	InstFamily string
	InstID     string
}

type AccountSubscriptionRequest struct {
	Ccy string
}

func (c *WSClient) SubscribeTickers(ctx context.Context, instID string, handler WSTypedHandler[Ticker]) error {
	_, err := c.SubscribeTickersHandle(ctx, instID, handler)
	return err
}

func (c *WSClient) SubscribeTickersHandle(ctx context.Context, instID string, handler WSTypedHandler[Ticker]) (*WSSubscription, error) {
	return subscribeInstIDChannel(ctx, c, WSChannelTickers, instID, handler)
}

func (c *WSClient) SubscribeTrades(ctx context.Context, instID string, handler WSTypedHandler[MarketTrade]) error {
	_, err := c.SubscribeTradesHandle(ctx, instID, handler)
	return err
}

func (c *WSClient) SubscribeTradesHandle(ctx context.Context, instID string, handler WSTypedHandler[MarketTrade]) (*WSSubscription, error) {
	return subscribeInstIDChannel(ctx, c, WSChannelTrades, instID, handler)
}

func (c *WSClient) SubscribeAllTrades(ctx context.Context, instID string, handler WSTypedHandler[MarketTrade]) error {
	_, err := c.SubscribeAllTradesHandle(ctx, instID, handler)
	return err
}

func (c *WSClient) SubscribeAllTradesHandle(ctx context.Context, instID string, handler WSTypedHandler[MarketTrade]) (*WSSubscription, error) {
	return subscribeInstIDChannel(ctx, c, WSChannelTradesAll, instID, handler)
}

func (c *WSClient) SubscribeCandles(ctx context.Context, instID, bar string, handler WSTypedHandler[Candle]) error {
	_, err := c.SubscribeCandlesHandle(ctx, instID, bar, handler)
	return err
}

func (c *WSClient) SubscribeCandlesHandle(ctx context.Context, instID, bar string, handler WSTypedHandler[Candle]) (*WSSubscription, error) {
	return c.subscribeCandleChannel(ctx, CandleChannel(bar), instID, bar, handler)
}

func (c *WSClient) SubscribeMarkPriceCandles(ctx context.Context, instID, bar string, handler WSTypedHandler[Candle]) error {
	_, err := c.SubscribeMarkPriceCandlesHandle(ctx, instID, bar, handler)
	return err
}

func (c *WSClient) SubscribeMarkPriceCandlesHandle(ctx context.Context, instID, bar string, handler WSTypedHandler[Candle]) (*WSSubscription, error) {
	return c.subscribeCandleChannel(ctx, MarkPriceCandleChannel(bar), instID, bar, handler)
}

func (c *WSClient) SubscribeIndexCandles(ctx context.Context, instID, bar string, handler WSTypedHandler[Candle]) error {
	_, err := c.SubscribeIndexCandlesHandle(ctx, instID, bar, handler)
	return err
}

func (c *WSClient) SubscribeIndexCandlesHandle(ctx context.Context, instID, bar string, handler WSTypedHandler[Candle]) (*WSSubscription, error) {
	return c.subscribeCandleChannel(ctx, IndexCandleChannel(bar), instID, bar, handler)
}

func (c *WSClient) subscribeCandleChannel(ctx context.Context, channel, instID, bar string, handler WSTypedHandler[Candle]) (*WSSubscription, error) {
	if bar == "" {
		return nil, required("bar")
	}
	return subscribeInstIDChannel(ctx, c, channel, instID, handler)
}

func (c *WSClient) SubscribeOrderBook(ctx context.Context, channel, instID string, handler WSTypedHandler[OrderBook]) error {
	_, err := c.SubscribeOrderBookHandle(ctx, channel, instID, handler)
	return err
}

func (c *WSClient) SubscribeOrderBookHandle(ctx context.Context, channel, instID string, handler WSTypedHandler[OrderBook]) (*WSSubscription, error) {
	if channel == "" {
		channel = WSChannelBooks
	}
	return subscribeInstIDChannel(ctx, c, channel, instID, handler)
}

func (c *WSClient) SubscribeLocalOrderBook(ctx context.Context, channel, instID string, depth int, handler WSLocalOrderBookHandler) error {
	_, err := c.SubscribeLocalOrderBookHandle(ctx, channel, instID, depth, handler)
	return err
}

func (c *WSClient) SubscribeLocalOrderBookHandle(ctx context.Context, channel, instID string, depth int, handler WSLocalOrderBookHandler) (*WSSubscription, error) {
	if channel == "" {
		channel = WSChannelBooks
	}
	if depth <= 0 {
		depth = OrderBookDepthFromChannel(channel)
	}
	if handler == nil {
		return nil, required("handler")
	}
	if instID == "" {
		return nil, required("instId")
	}
	if OrderBookDepthFromChannel(channel) == 0 {
		return nil, ErrInvalidParameter
	}
	consume, disconnected := localBookConsumer(channel, instID, depth, OrderBookDepthFromChannel(channel), handler, c.requestRecovery)
	return c.subscribeArgHandle(ctx, WSChannelArg{Channel: channel, InstID: instID}, consume, disconnected)
}

func localBookConsumer(channel, instID string, depth, retainedDepth int, handler WSLocalOrderBookHandler, onError func(error)) (Handler, func(error)) {
	book := newLocalOrderBook(instID, depth, retainedDepth)
	// Reuse the decoded array and each side's capacity on this read loop only.
	rows := make(reusableOrderBooks, 1)
	rows[0].Bids = make([]BookLevel, 0, retainedDepth)
	rows[0].Asks = make([]BookLevel, 0, retainedDepth)
	return func(msg Message) {
			err := msg.Into(&rows)
			if err == nil {
				err = book.ApplyMessage(WSTypedMessage[OrderBook]{Action: msg.Action, Data: rows})
			}
			out := WSLocalOrderBookMessage{Arg: WSChannelArg{Channel: msg.Channel, InstID: msg.InstID}, Action: msg.Action, Book: book, Err: err}
			if err != nil {
				book.Reset()
				out.Book = nil
				onError(err)
			}
			handler(out)
		}, func(err error) {
			book.Reset()
			handler(WSLocalOrderBookMessage{Arg: WSChannelArg{Channel: channel, InstID: instID}, Err: err})
		}
}

func subscribeInstIDChannel[T any](ctx context.Context, c *WSClient, channel string, instID string, handler WSTypedHandler[T]) (*WSSubscription, error) {
	if instID == "" {
		return nil, required("instId")
	}
	return subscribeTypedHandle(ctx, c, WSChannelArg{
		Channel: channel,
		InstID:  instID,
	}, handler)
}

func (c *WSClient) SubscribeOrderBookDepth(ctx context.Context, instID string, depth int, handler WSTypedHandler[OrderBook]) error {
	_, err := c.SubscribeOrderBookDepthHandle(ctx, instID, depth, handler)
	return err
}

func (c *WSClient) SubscribeOrderBookDepthHandle(ctx context.Context, instID string, depth int, handler WSTypedHandler[OrderBook]) (*WSSubscription, error) {
	return c.SubscribeOrderBookHandle(ctx, OrderBookChannel(depth), instID, handler)
}

func (c *WSClient) SubscribeLocalOrderBookDepth(ctx context.Context, instID string, depth int, handler WSLocalOrderBookHandler) error {
	_, err := c.SubscribeLocalOrderBookDepthHandle(ctx, instID, depth, handler)
	return err
}

func (c *WSClient) SubscribeLocalOrderBookDepthHandle(ctx context.Context, instID string, depth int, handler WSLocalOrderBookHandler) (*WSSubscription, error) {
	return c.SubscribeLocalOrderBookHandle(ctx, OrderBookChannel(depth), instID, depth, handler)
}

func (c *WSClient) SubscribeOrders(ctx context.Context, req OrdersSubscriptionRequest, handler WSTypedHandler[Order]) error {
	_, err := c.SubscribeOrdersHandle(ctx, req, handler)
	return err
}

func (c *WSClient) SubscribeOrdersHandle(ctx context.Context, req OrdersSubscriptionRequest, handler WSTypedHandler[Order]) (*WSSubscription, error) {
	instType := req.InstType
	if instType == "" {
		instType = InstAny
	}
	return subscribeTypedHandle(ctx, c, WSChannelArg{
		Channel:    WSChannelOrders,
		InstType:   string(instType),
		InstFamily: req.InstFamily,
		InstID:     req.InstID,
	}, handler)
}

func (c *WSClient) SubscribeAccount(ctx context.Context, req AccountSubscriptionRequest, handler WSTypedHandler[AccountUpdate]) error {
	_, err := c.SubscribeAccountHandle(ctx, req, handler)
	return err
}

func (c *WSClient) SubscribeAccountHandle(ctx context.Context, req AccountSubscriptionRequest, handler WSTypedHandler[AccountUpdate]) (*WSSubscription, error) {
	return subscribeTypedHandle(ctx, c, WSChannelArg{
		Channel: WSChannelAccount,
		Ccy:     req.Ccy,
	}, handler)
}

func CandleChannel(bar string) string {
	return WSChannelCandle + bar
}

func MarkPriceCandleChannel(bar string) string {
	return WSChannelMarkCandle + bar
}

func IndexCandleChannel(bar string) string {
	return WSChannelIndexCandle + bar
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
		return MaxOrderBookDepth
	}
}

// OrderBookChannel returns the lowest-depth OKX order-book channel that can
// satisfy the requested depth. books50-l2-tbt and books-l2-tbt are high-frequency
// incremental channels; OKX requires login and the documented VIP tier before
// subscribing.
func OrderBookChannel(depth int) string {
	switch OrderBookDepth(depth) {
	case 1:
		return WSChannelBBO
	case 5:
		return WSChannelBooks5
	case 50:
		return WSChannelBooks50L2TBT
	default:
		return WSChannelBooks
	}
}

// PublicOrderBookDepth rounds depth to a public, non-VIP snapshot channel.
func PublicOrderBookDepth(depth int) int {
	if depth <= 5 {
		return 5
	}
	return MaxOrderBookDepth
}

// PublicOrderBookChannel returns a public, non-VIP order-book channel.
func PublicOrderBookChannel(depth int) string {
	switch PublicOrderBookDepth(depth) {
	case 5:
		return WSChannelBooks5
	default:
		return WSChannelBooks
	}
}

func OrderBookDepthFromChannel(channel string) int {
	switch channel {
	case WSChannelBBO:
		return 1
	case WSChannelBooks5:
		return 5
	case WSChannelBooks50L2TBT:
		return 50
	case WSChannelBooks, WSChannelBooksL2TBT:
		return 400
	default:
		return 0
	}
}

func subscribeTypedHandle[T any](ctx context.Context, c *WSClient, arg WSChannelArg, handler WSTypedHandler[T]) (*WSSubscription, error) {
	if handler == nil {
		return nil, required("handler")
	}
	return c.SubscribeArgHandle(ctx, arg, func(msg Message) {
		out := decodeTypedWSMessage[T](msg)
		if out.Err == nil && (arg.Channel == WSChannelAccount || arg.Channel == WSChannelOrders) {
			out.Data = filterPrivateRows(arg, out.Data)
			if len(out.Data) == 0 && !out.LastPage {
				return
			}
		}
		handler(out)
	})
}

func decodeTypedWSMessage[T any](msg Message) WSTypedMessage[T] {
	out := WSTypedMessage[T]{
		Arg: WSChannelArg{
			Channel:    msg.Channel,
			InstID:     msg.InstID,
			InstType:   msg.InstType,
			InstFamily: msg.InstFamily,
			Ccy:        msg.Ccy,
			UID:        msg.UID,
		},
		Action:    msg.Action,
		EventType: msg.EventType, CurPage: msg.CurPage, LastPage: msg.LastPage,
	}
	if len(msg.Data) == 0 {
		return out
	}
	if err := msg.Into(&out.Data); err != nil {
		out.Err = err
	}
	return out
}

func filterPrivateRows[T any](arg WSChannelArg, rows []T) []T {
	out := rows[:0]
	for _, value := range rows {
		switch row := any(value).(type) {
		case Order:
			if arg.InstType != "" && arg.InstType != "ANY" && string(row.InstType) != arg.InstType {
				continue
			}
			if arg.InstID != "" && row.InstID != arg.InstID {
				continue
			}
			if arg.InstFamily != "" && !strings.HasPrefix(row.InstID, arg.InstFamily+"-") {
				continue
			}
		case AccountUpdate:
			if arg.Ccy != "" {
				details := row.Details[:0]
				for _, d := range row.Details {
					if d.Ccy == arg.Ccy {
						details = append(details, d)
					}
				}
				row.Details = details
				if len(details) == 0 {
					continue
				}
				value = any(row).(T)
			}
		}
		out = append(out, value)
	}
	return out
}
