package okx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

const (
	defaultWSWriteTimeout = 10 * time.Second
	defaultWSPingInterval = 25 * time.Second
)

type WSOption func(*WSClient)

func WithWSURL(url string) WSOption {
	return func(c *WSClient) {
		c.url = url
	}
}

func WithWSCredentials(apiKey, secretKey, passphrase string) WSOption {
	return func(c *WSClient) {
		c.credentials = Credentials{APIKey: apiKey, SecretKey: secretKey, Passphrase: passphrase}
	}
}

func WithWSLogger(logger Logger) WSOption {
	return func(c *WSClient) {
		if logger != nil {
			c.logger = logger
		}
	}
}

type WSClient struct {
	url         string
	credentials Credentials
	logger      Logger
	now         func() time.Time

	mu         sync.RWMutex
	conn       *websocket.Conn
	loopCancel context.CancelFunc
	closed     bool
	closeOnce  sync.Once

	writeCh chan wsWrite
	done    chan struct{}

	pending map[string]chan WSAck
	ops     map[string]chan WSOperationResponse
	subs    map[string]wsSubscriptionState
	opSeq   int64
}

type wsWrite struct {
	ctx     context.Context
	payload []byte
	errCh   chan error
}

type WSAck struct {
	Event  string            `json:"event,omitempty"`
	Code   string            `json:"code,omitempty"`
	Msg    string            `json:"msg,omitempty"`
	ConnID string            `json:"connId,omitempty"`
	Arg    map[string]string `json:"arg,omitempty"`
}

type WSMessage struct {
	Arg    map[string]string
	Action string
	Data   json.RawMessage
	Raw    json.RawMessage
}

type WSOperationResponse struct {
	ID      string          `json:"id,omitempty"`
	Op      string          `json:"op,omitempty"`
	Event   string          `json:"event,omitempty"`
	Code    string          `json:"code,omitempty"`
	Msg     string          `json:"msg,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
	InTime  string          `json:"inTime,omitempty"`
	OutTime string          `json:"outTime,omitempty"`
	Raw     json.RawMessage `json:"-"`
}

func (r WSOperationResponse) DecodeData(out any) error {
	if len(r.Data) == 0 || out == nil {
		return nil
	}
	return json.Unmarshal(r.Data, out)
}

type Subscription struct {
	Channel string
	Args    map[string]string
}

type wsSubscriptionState struct {
	sub Subscription
	ch  chan WSMessage
}

func NewWSClient(opts ...WSOption) *WSClient {
	c := &WSClient{
		url:     WSPublicURL,
		logger:  noopLogger{},
		now:     time.Now,
		writeCh: make(chan wsWrite, 128),
		done:    make(chan struct{}),
		pending: make(map[string]chan WSAck),
		ops:     make(map[string]chan WSOperationResponse),
		subs:    make(map[string]wsSubscriptionState),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *WSClient) Connect(ctx context.Context) error {
	conn, _, err := websocket.Dial(ctx, c.url, nil)
	if err != nil {
		return fmt.Errorf("okx ws: dial: %w", err)
	}
	conn.SetReadLimit(8 << 20)
	loopCtx, loopCancel := context.WithCancel(context.WithoutCancel(ctx))

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		loopCancel()
		_ = conn.Close(websocket.StatusNormalClosure, "")
		return errors.New("okx ws: client closed")
	}
	c.conn = conn
	c.loopCancel = loopCancel
	c.mu.Unlock()

	go c.writeLoop(loopCtx, conn)
	go c.readLoop(loopCtx, conn)
	go c.pingLoop(loopCtx)
	return nil
}

func (c *WSClient) Login(ctx context.Context) error {
	timestamp := wsTimestamp(c.now())
	signature := Sign(c.credentials.SecretKey, timestamp, http.MethodGet, "/users/self/verify", "")
	req := map[string]any{
		"op": "login",
		"args": []map[string]string{{
			"apiKey":     c.credentials.APIKey,
			"passphrase": c.credentials.Passphrase,
			"timestamp":  timestamp,
			"sign":       signature,
		}},
	}
	ack, err := c.sendAndWait(ctx, "login", req)
	if err != nil {
		return err
	}
	if ack.Code != "" && ack.Code != "0" {
		return wrapOKXError(&OKXError{Code: ack.Code, Message: ack.Msg})
	}
	return nil
}

func (c *WSClient) Do(ctx context.Context, op string, args ...any) (WSOperationResponse, error) {
	op = strings.TrimSpace(op)
	if op == "" {
		return WSOperationResponse{}, errors.New("okx ws: operation is required")
	}
	id := strconvFormatUnix(atomic.AddInt64(&c.opSeq, 1))
	req := map[string]any{
		"id":   id,
		"op":   op,
		"args": args,
	}
	return c.sendOperationAndWait(ctx, operationPendingKey(id, op), req)
}

func (c *WSClient) PlaceOrder(ctx context.Context, req PlaceOrderRequest) ([]OrderAck, error) {
	return c.tradeOperation(ctx, "order", req)
}

func (c *WSClient) PlaceMultipleOrders(ctx context.Context, req []PlaceOrderRequest) ([]OrderAck, error) {
	args := make([]any, 0, len(req))
	for i := range req {
		args = append(args, req[i])
	}
	return c.tradeOperationArgs(ctx, "batch-orders", args...)
}

func (c *WSClient) AmendOrder(ctx context.Context, req AmendOrderRequest) ([]OrderAck, error) {
	return c.tradeOperation(ctx, "amend-order", req)
}

func (c *WSClient) AmendMultipleOrders(ctx context.Context, req []AmendOrderRequest) ([]OrderAck, error) {
	args := make([]any, 0, len(req))
	for i := range req {
		args = append(args, req[i])
	}
	return c.tradeOperationArgs(ctx, "batch-amend-orders", args...)
}

func (c *WSClient) CancelOrder(ctx context.Context, req CancelOrderRequest) ([]OrderAck, error) {
	return c.tradeOperation(ctx, "cancel-order", req)
}

func (c *WSClient) CancelMultipleOrders(ctx context.Context, req []CancelOrderRequest) ([]OrderAck, error) {
	args := make([]any, 0, len(req))
	for i := range req {
		args = append(args, req[i])
	}
	return c.tradeOperationArgs(ctx, "batch-cancel-orders", args...)
}

func (c *WSClient) tradeOperation(ctx context.Context, op string, arg any) ([]OrderAck, error) {
	return c.tradeOperationArgs(ctx, op, arg)
}

func (c *WSClient) tradeOperationArgs(ctx context.Context, op string, args ...any) ([]OrderAck, error) {
	resp, err := c.Do(ctx, op, args...)
	if err != nil {
		return nil, err
	}
	if resp.Code != "" && resp.Code != "0" && len(resp.Data) == 0 {
		return nil, wrapOKXError(&OKXError{Code: resp.Code, Message: resp.Msg, Raw: resp.Raw})
	}
	var out []OrderAck
	if err := resp.DecodeData(&out); err != nil {
		return nil, fmt.Errorf("okx ws: decode %s response: %w", op, err)
	}
	return out, nil
}

func (c *WSClient) Subscribe(ctx context.Context, sub Subscription) (<-chan WSMessage, error) {
	if strings.TrimSpace(sub.Channel) == "" {
		return nil, errors.New("okx ws: subscription channel is required")
	}
	key := sub.key()
	ch := make(chan WSMessage, 256)

	c.mu.Lock()
	if _, exists := c.subs[key]; exists {
		c.mu.Unlock()
		return nil, fmt.Errorf("okx ws: already subscribed to %s", key)
	}
	c.subs[key] = wsSubscriptionState{sub: sub.clone(), ch: ch}
	c.mu.Unlock()

	arg := sub.arg()
	ack, err := c.sendAndWait(ctx, "subscribe:"+key, map[string]any{
		"op":   "subscribe",
		"args": []map[string]string{arg},
	})
	if err != nil {
		c.removeSubscription(key, true)
		return nil, err
	}
	if ack.Code != "" && ack.Code != "0" {
		c.removeSubscription(key, true)
		return nil, wrapOKXError(&OKXError{Code: ack.Code, Message: ack.Msg})
	}
	return ch, nil
}

func (c *WSClient) Unsubscribe(ctx context.Context, sub Subscription) error {
	key := sub.key()
	ack, err := c.sendAndWait(ctx, "unsubscribe:"+key, map[string]any{
		"op":   "unsubscribe",
		"args": []map[string]string{sub.arg()},
	})
	if err != nil {
		return err
	}
	if ack.Code != "" && ack.Code != "0" {
		return wrapOKXError(&OKXError{Code: ack.Code, Message: ack.Msg})
	}
	c.removeSubscription(key, true)
	return nil
}

func (c *WSClient) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		conn := c.conn
		loopCancel := c.loopCancel
		c.conn = nil
		c.loopCancel = nil
		for key := range c.subs {
			c.removeSubscriptionLocked(key, true)
		}
		c.mu.Unlock()
		if loopCancel != nil {
			loopCancel()
		}
		close(c.done)
		if conn != nil {
			err = conn.Close(websocket.StatusNormalClosure, "")
		}
	})
	return err
}

func (c *WSClient) sendAndWait(ctx context.Context, pendingKey string, req any) (WSAck, error) {
	ackCh := make(chan WSAck, 1)
	c.mu.Lock()
	c.pending[pendingKey] = ackCh
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, pendingKey)
		c.mu.Unlock()
	}()

	if err := c.sendJSON(ctx, req); err != nil {
		return WSAck{}, err
	}
	select {
	case ack := <-ackCh:
		return ack, nil
	case <-ctx.Done():
		return WSAck{}, ctx.Err()
	case <-c.done:
		return WSAck{}, errors.New("okx ws: client closed")
	}
}

func (c *WSClient) sendOperationAndWait(ctx context.Context, pendingKey string, req any) (WSOperationResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	respCh := make(chan WSOperationResponse, 1)
	c.mu.Lock()
	c.ops[pendingKey] = respCh
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.ops, pendingKey)
		c.mu.Unlock()
	}()

	if err := c.sendJSON(ctx, req); err != nil {
		return WSOperationResponse{}, err
	}
	select {
	case resp := <-respCh:
		return resp, nil
	case <-ctx.Done():
		return WSOperationResponse{}, ctx.Err()
	case <-c.done:
		return WSOperationResponse{}, errors.New("okx ws: client closed")
	}
}

func (c *WSClient) sendJSON(ctx context.Context, v any) error {
	if ctx == nil {
		ctx = context.Background()
	}
	payload, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("okx ws: encode message: %w", err)
	}
	errCh := make(chan error, 1)
	select {
	case c.writeCh <- wsWrite{ctx: ctx, payload: payload, errCh: errCh}:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errors.New("okx ws: client closed")
	}
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errors.New("okx ws: client closed")
	}
}

func (c *WSClient) writeLoop(ctx context.Context, conn *websocket.Conn) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case item := <-c.writeCh:
			writeBaseCtx := item.ctx
			if writeBaseCtx == nil {
				writeBaseCtx = ctx
			}
			writeCtx, cancel := context.WithTimeout(writeBaseCtx, defaultWSWriteTimeout)
			err := conn.Write(writeCtx, websocket.MessageText, item.payload)
			cancel()
			item.errCh <- err
		}
	}
}

func (c *WSClient) readLoop(ctx context.Context, conn *websocket.Conn) {
	for {
		msgType, raw, err := conn.Read(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				c.logger.Warn("okx ws read stopped", "error", err)
			}
			return
		}
		if msgType != websocket.MessageText {
			continue
		}
		if string(raw) == "pong" {
			continue
		}
		c.handleRaw(raw)
	}
}

func (c *WSClient) pingLoop(ctx context.Context) {
	ticker := time.NewTicker(defaultWSPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, defaultWSWriteTimeout)
			err := c.sendRaw(pingCtx, []byte("ping"))
			cancel()
			if err != nil && !errors.Is(err, context.Canceled) {
				c.logger.Warn("okx ws ping failed", "error", err)
			}
		case <-ctx.Done():
			return
		case <-c.done:
			return
		}
	}
}

func (c *WSClient) sendRaw(ctx context.Context, payload []byte) error {
	errCh := make(chan error, 1)
	select {
	case c.writeCh <- wsWrite{ctx: ctx, payload: payload, errCh: errCh}:
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errors.New("okx ws: client closed")
	}
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errors.New("okx ws: client closed")
	}
}

func (c *WSClient) handleRaw(raw []byte) {
	var msg struct {
		Event   string            `json:"event,omitempty"`
		Code    string            `json:"code,omitempty"`
		Msg     string            `json:"msg,omitempty"`
		ConnID  string            `json:"connId,omitempty"`
		ID      string            `json:"id,omitempty"`
		Op      string            `json:"op,omitempty"`
		Arg     map[string]string `json:"arg,omitempty"`
		Action  string            `json:"action,omitempty"`
		Data    json.RawMessage   `json:"data,omitempty"`
		InTime  string            `json:"inTime,omitempty"`
		OutTime string            `json:"outTime,omitempty"`
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		c.logger.Warn("okx ws decode failed", "error", err)
		return
	}

	if msg.ID != "" && msg.Op != "" {
		c.dispatchOperation(WSOperationResponse{
			ID:      msg.ID,
			Op:      msg.Op,
			Event:   msg.Event,
			Code:    msg.Code,
			Msg:     msg.Msg,
			Data:    msg.Data,
			InTime:  msg.InTime,
			OutTime: msg.OutTime,
			Raw:     append(json.RawMessage(nil), raw...),
		})
		return
	}
	if msg.Event != "" {
		c.dispatchAck(msg.Event, WSAck{
			Event:  msg.Event,
			Code:   msg.Code,
			Msg:    msg.Msg,
			ConnID: msg.ConnID,
			Arg:    msg.Arg,
		})
		return
	}
	if msg.Arg == nil || len(msg.Data) == 0 {
		return
	}
	sub := Subscription{Channel: msg.Arg["channel"], Args: argsWithoutChannel(msg.Arg)}
	key := sub.key()
	c.mu.RLock()
	state, ok := c.subs[key]
	c.mu.RUnlock()
	if !ok {
		return
	}
	select {
	case state.ch <- WSMessage{Arg: msg.Arg, Action: msg.Action, Data: msg.Data, Raw: append(json.RawMessage(nil), raw...)}:
	default:
		c.logger.Warn("okx ws subscription channel full", "key", key)
	}
}

func (c *WSClient) dispatchOperation(resp WSOperationResponse) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if ch := c.ops[operationPendingKey(resp.ID, resp.Op)]; ch != nil {
		select {
		case ch <- resp:
		default:
		}
	}
}

func (c *WSClient) dispatchAck(event string, ack WSAck) {
	keys := []string{event}
	if ack.Arg != nil && ack.Arg["channel"] != "" {
		sub := Subscription{Channel: ack.Arg["channel"], Args: argsWithoutChannel(ack.Arg)}
		subKey := sub.key()
		keys = append(keys, event+":"+subKey)
		if event == "error" {
			keys = append(keys, "subscribe:"+subKey, "unsubscribe:"+subKey)
		}
	}
	if event == "error" {
		keys = append(keys, "login")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, key := range keys {
		if ch := c.pending[key]; ch != nil {
			select {
			case ch <- ack:
			default:
			}
		}
	}
}

func operationPendingKey(id, op string) string {
	return "operation:" + op + ":" + id
}

func (c *WSClient) removeSubscription(key string, closeCh bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeSubscriptionLocked(key, closeCh)
}

func (c *WSClient) removeSubscriptionLocked(key string, closeCh bool) {
	state, ok := c.subs[key]
	if !ok {
		return
	}
	delete(c.subs, key)
	if closeCh {
		close(state.ch)
	}
}

func (s Subscription) arg() map[string]string {
	arg := map[string]string{"channel": s.Channel}
	for k, v := range s.Args {
		if k != "channel" && v != "" {
			arg[k] = v
		}
	}
	return arg
}

func (s Subscription) clone() Subscription {
	args := make(map[string]string, len(s.Args))
	for k, v := range s.Args {
		args[k] = v
	}
	return Subscription{Channel: s.Channel, Args: args}
}

func (s Subscription) key() string {
	arg := s.arg()
	keys := make([]string, 0, len(arg))
	for k := range arg {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for i, k := range keys {
		if i > 0 {
			b.WriteByte('|')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(arg[k])
	}
	return b.String()
}

func argsWithoutChannel(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		if k != "channel" {
			out[k] = v
		}
	}
	return out
}
