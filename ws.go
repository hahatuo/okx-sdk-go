package okx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	json "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
	"github.com/hahatuo/okx-sdk-go/internal/sign"
)

// WSKind selects which OKX WebSocket endpoint to connect to.
type WSKind int

const (
	Public   WSKind = iota // market data, no login
	Private                // account/order data, requires login
	Business               // candles/algo, may require login
)

const (
	defaultWSSystemEventBuffer = 128
	defaultWSReconnectMinDelay = time.Second
	defaultWSReconnectMaxDelay = 30 * time.Second
)

var (
	// ErrWSNotConnected is returned when a WebSocket operation is attempted
	// before Connect succeeds or after the connection has closed.
	ErrWSNotConnected = errors.New("okx: websocket not connected")
	// ErrWSConnectionLost is delivered to pending control-plane operations when
	// an established WebSocket connection drops.
	ErrWSConnectionLost = errors.New("okx: websocket connection lost")
	// ErrWSAlreadyConnected is returned when Connect is called on a running
	// WebSocket client.
	ErrWSAlreadyConnected = errors.New("okx: websocket already connected")
	// ErrWSConcurrentOperation is returned when a second login/subscribe/unsubscribe
	// for the same semantic key is attempted while the first is still pending.
	ErrWSConcurrentOperation = errors.New("okx: concurrent websocket operation in progress")
)

// Message is a single channel push delivered to a Handler. Data is the raw
// `data` array; decode it into a typed slice with Into. The Message and its
// Data are only valid for the duration of the Handler call — copy anything you
// retain.
type Message struct {
	Channel    string
	InstID     string
	InstType   string
	InstFamily string
	Ccy        string
	Action     string // "snapshot" | "update" for book channels; "" otherwise
	Data       jsontext.Value
}

// Into decodes the raw data array into out.
func (m Message) Into(out any) error { return json.Unmarshal(m.Data, out) }

// Handler is invoked directly on the read goroutine for every push on a
// subscribed channel. It MUST NOT block: heavy work should be handed to the
// caller's own worker, and per Agents.md the hot path deliberately avoids a
// channel hop. Keep it allocation-light.
type Handler func(Message)

// subEntry records an active subscription so it can be replayed after reconnect.
type subEntry struct {
	arg     subArg
	handler Handler
}

// WSClient is a single OKX WebSocket connection with automatic reconnect,
// re-login and re-subscribe. Safe for concurrent Subscribe/Close.
type WSClient struct {
	url     string
	kind    WSKind
	cred    Credentials
	signer  *sign.Signer
	log     Logger
	now     func() time.Time
	dialFn  func(context.Context, string) (*websocket.Conn, error)
	pingInt time.Duration
	rl      RateLimiter

	reconnect WSReconnectConfig

	// connection state
	connMu  sync.RWMutex
	conn    *websocket.Conn
	writeMu sync.Mutex // serializes all frame writes (coder/websocket: one writer)

	// handlers is a copy-on-write map read lock-free on the dispatch hot path.
	// Writers (Subscribe/Unsubscribe) swap in a fresh map under subMu.
	handlers atomic.Pointer[map[string]*subEntry]
	subMu    sync.Mutex

	// pending control-plane acks (login/subscribe/unsubscribe): low frequency.
	// Keys are semantic strings generated from the operation type and subscription
	// arguments (e.g. "control:subscribe:tickers|...|instId=BTC-USDT"). OKX does
	// NOT echo a client-supplied id for these operations, so numeric ids cannot
	// be used for correlation.
	pendingMu sync.Mutex
	pending   map[string]chan error
	ops       sync.Map // operation key -> chan WSOperationResponse

	systemEvents chan WSSystemEvent

	lastReadNano atomic.Int64
	reconnects   atomic.Int64
	reqID        atomic.Uint64

	cancel context.CancelFunc
	run    *struct{}
	wg     sync.WaitGroup
}

type WSReconnectConfig struct {
	Enabled     bool
	MinDelay    time.Duration
	MaxDelay    time.Duration
	MaxAttempts int
}

// WSOption configures a WSClient.
type WSOption func(*WSClient)

// WithWSCredentials supplies API-key material for login (required for Private).
func WithWSCredentials(apiKey, secretKey, passphrase string) WSOption {
	return func(c *WSClient) { c.cred = Credentials{apiKey, secretKey, passphrase} }
}

// WithWSLogger installs a logger (default no-op).
func WithWSLogger(l Logger) WSOption { return func(c *WSClient) { c.log = l } }

// WithWSRateLimiter installs a rate limiter for request/response WebSocket
// operations such as order, amend-order, and batch-orders.
func WithWSRateLimiter(r RateLimiter) WSOption { return func(c *WSClient) { c.rl = r } }

// WithWSURL overrides the WebSocket endpoint URL.
func WithWSURL(url string) WSOption {
	return func(c *WSClient) {
		if url != "" {
			c.url = url
		}
	}
}

// WithWSDemo routes to the demo-trading endpoints.
func WithWSDemo(demo bool) WSOption {
	return func(c *WSClient) { c.url = wsURL(c.kind, demo) }
}

// WithWSReconnect enables or disables automatic reconnect after a connected
// WebSocket drops. It does not retry a failed initial Connect call.
func WithWSReconnect(enabled bool) WSOption {
	return func(c *WSClient) {
		c.reconnect.Enabled = enabled
	}
}

// WithWSReconnectConfig configures reconnect backoff. Zero durations keep
// defaults; MaxAttempts <= 0 means unlimited attempts.
func WithWSReconnectConfig(config WSReconnectConfig) WSOption {
	return func(c *WSClient) {
		c.reconnect = normalizeWSReconnectConfig(config)
	}
}

// NewWSClient builds a WebSocket client for the given endpoint kind.
func NewWSClient(kind WSKind, opts ...WSOption) *WSClient {
	c := &WSClient{
		url:          wsURL(kind, false),
		kind:         kind,
		log:          noopLogger{},
		now:          time.Now,
		pingInt:      20 * time.Second,
		rl:           DefaultRateLimiter(),
		reconnect:    defaultWSReconnectConfig(),
		systemEvents: make(chan WSSystemEvent, defaultWSSystemEventBuffer),
		pending:      make(map[string]chan error),
	}
	c.dialFn = defaultDial
	empty := make(map[string]*subEntry)
	c.handlers.Store(&empty)
	for _, o := range opts {
		o(c)
	}
	if c.cred.SecretKey != "" {
		c.signer = sign.New([]byte(c.cred.SecretKey))
	}
	return c
}

// SystemEvents exposes low-frequency WebSocket control-plane events such as
// channel connection-count notifications and unmatched error events.
func (c *WSClient) SystemEvents() <-chan WSSystemEvent {
	return c.systemEvents
}

func wsURL(kind WSKind, demo bool) string {
	switch {
	case demo && kind == Public:
		return wsPublicDemoURL
	case demo && kind == Private:
		return wsPrivateDemoURL
	case demo && kind == Business:
		return wsBusinessDemoURL
	case kind == Private:
		return wsPrivateURL
	case kind == Business:
		return wsBusinessURL
	default:
		return wsPublicURL
	}
}

func defaultDial(ctx context.Context, u string) (*websocket.Conn, error) {
	conn, _, err := websocket.Dial(ctx, u, nil)
	return conn, err
}

// Connect dials the endpoint, logs in (Private), and starts the supervised
// read/ping loops. All goroutines are owned by ctx; cancelling ctx or calling
// Close tears everything down with no leak. Connect returns once the first dial
// (and login, for Private) succeeds or fails.
func (c *WSClient) Connect(ctx context.Context) error {
	return c.connect(ctx, 0)
}

// ConnectWithTimeout is like Connect, but bounds only the initial dial/login
// handshake. The parent ctx still owns the connected WebSocket lifetime after a
// successful return.
func (c *WSClient) ConnectWithTimeout(ctx context.Context, timeout time.Duration) error {
	return c.connect(ctx, timeout)
}

func (c *WSClient) connect(ctx context.Context, timeout time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if c.kind == Private && c.signer == nil {
		return ErrUnauthorized
	}
	runCtx, cancel := context.WithCancel(ctx)
	run := new(struct{})
	c.connMu.Lock()
	if c.run != nil {
		c.connMu.Unlock()
		cancel()
		return ErrWSAlreadyConnected
	}
	c.cancel = cancel
	c.run = run
	c.connMu.Unlock()

	first := make(chan error, 1)
	c.wg.Add(1)
	go c.supervise(runCtx, first, run)

	waitInitial := func() error {
		if timeout <= 0 {
			return <-first
		}
		t := time.NewTimer(timeout)
		defer t.Stop()
		select {
		case err := <-first:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			return context.DeadlineExceeded
		}
	}
	if err := waitInitial(); err != nil {
		cancel()
		c.clearRun(run)
		return err
	}
	return nil
}

// supervise owns the connection lifecycle: dial → serve → (on drop) backoff →
// redial, until ctx is cancelled. The first dial result is reported on first.
func (c *WSClient) supervise(ctx context.Context, first chan error, run *struct{}) {
	defer func() {
		c.clearRun(run)
		c.wg.Done()
	}()

	var once sync.Once
	report := func(err error) { once.Do(func() { first <- err }) }

	reconnect := c.reconnect
	backoff := reconnect.MinDelay
	attempts := 0
	for {
		conn, err := c.dialFn(ctx, c.url)
		if err != nil {
			report(err)
			if !reconnect.Enabled || !canReconnect(reconnect, attempts) || !sleep(ctx, backoff) {
				return
			}
			attempts++
			backoff = min(backoff*2, reconnect.MaxDelay)
			c.reconnects.Add(1)
			continue
		}
		backoff = reconnect.MinDelay
		attempts = 0

		// serve blocks until the connection dies or ctx is cancelled.
		served := c.serve(ctx, conn, report)
		if ctx.Err() != nil {
			return
		}
		if !reconnect.Enabled || !canReconnect(reconnect, attempts) {
			return
		}
		if served {
			c.reconnects.Add(1)
		}
		attempts++
		if !sleep(ctx, reconnect.MinDelay) {
			return
		}
	}
}

// serve installs conn, starts the read loop, performs login+resubscribe, then
// runs the ping loop until failure. reportOK signals the
// initial Connect once the connection is fully ready. Returns true if the
// connection had been established (so the caller counts a reconnect).
func (c *WSClient) serve(parent context.Context, conn *websocket.Conn, reportOK func(error)) bool {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	c.setConn(conn)
	defer c.clearConn(conn)
	c.lastReadNano.Store(c.now().UnixNano())

	readDone := make(chan error, 1)
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		readDone <- c.readLoop(ctx, conn)
	}()

	if c.kind == Private {
		if err := c.login(ctx); err != nil {
			c.log.Error("ws login failed", "err", err)
			reportOK(err)
			_ = conn.Close(websocket.StatusInternalError, "login failed")
			return false
		}
	}
	if err := c.resubscribeAll(ctx); err != nil {
		c.log.Warn("ws resubscribe failed", "err", err)
	}
	reportOK(nil)

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.pingLoop(ctx)
	}()

	readErr := <-readDone
	if readErr != nil {
		if ctx.Err() != nil {
			c.failPending(ctx.Err())
		} else {
			c.failPending(ErrWSConnectionLost)
		}
	}
	_ = conn.Close(websocket.StatusNormalClosure, "")
	return true
}

// readLoop reads frames until error/cancel and dispatches each to a handler.
func (c *WSClient) readLoop(ctx context.Context, conn *websocket.Conn) error {
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() == nil {
				c.log.Warn("ws read error", "err", err)
			}
			return err
		}
		c.lastReadNano.Store(c.now().UnixNano())
		c.dispatch(data)
	}
}

// pingLoop sends an application-level "ping" every pingInt and, if no frame has
// been read since connect or the previous read for 2*pingInt, drops the
// connection so supervise can reconnect.
func (c *WSClient) pingLoop(ctx context.Context) {
	t := time.NewTicker(c.pingInt)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if last := c.lastReadNano.Load(); last != 0 &&
				c.now().UnixNano()-last > int64(2*c.pingInt) {
				if conn := c.getConn(); conn != nil {
					_ = conn.Close(websocket.StatusGoingAway, "read timeout")
				}
				return
			}
			if err := c.writeText(ctx, "ping"); err != nil {
				return
			}
		}
	}
}

// dispatch parses one inbound frame and routes it. Handler lookup is lock-free.
func (c *WSClient) dispatch(data []byte) {
	if len(data) == 4 && data[0] == 'p' && string(data) == "pong" {
		return // application-level heartbeat reply
	}
	var in struct {
		ID      uint64         `json:"id,string"`
		Op      string         `json:"op"`
		Event   string         `json:"event"`
		Arg     subArg         `json:"arg"`
		Action  string         `json:"action"`
		Data    jsontext.Value `json:"data"`
		Code    string         `json:"code"`
		Msg     string         `json:"msg"`
		ConnID  string         `json:"connId"`
		Channel string         `json:"channel"`
		ConnCnt string         `json:"connCount"`
		InTime  string         `json:"inTime"`
		OutTime string         `json:"outTime"`
	}
	if err := json.Unmarshal(data, &in); err != nil {
		c.log.Warn("ws decode error", "err", err)
		return
	}

	if in.ID != 0 && in.Op != "" {
		c.deliverOperation(WSOperationResponse{
			ID:      strconv.FormatUint(in.ID, 10),
			Op:      in.Op,
			Event:   in.Event,
			Code:    in.Code,
			Msg:     in.Msg,
			Data:    in.Data,
			InTime:  in.InTime,
			OutTime: in.OutTime,
		})
		return
	}
	if in.Event != "" {
		delivered := c.deliverAck(in.Event, in.Code, in.Msg, in.Arg)
		if isWSSystemEvent(in.Event) || (!delivered && in.Event == "error") {
			c.dispatchSystemEvent(WSSystemEvent{
				Event:     in.Event,
				Code:      in.Code,
				Msg:       in.Msg,
				ConnID:    in.ConnID,
				Channel:   in.Channel,
				ConnCount: in.ConnCnt,
				Arg:       in.Arg.channelArg(),
				Raw:       append([]byte(nil), data...),
			})
		}
		return
	}
	if in.Arg.Channel == "" {
		return
	}
	m := *c.handlers.Load()
	if e := m[in.Arg.key()]; e != nil {
		e.handler(Message{
			Channel:    in.Arg.Channel,
			InstID:     in.Arg.InstID,
			InstType:   in.Arg.InstType,
			InstFamily: in.Arg.InstFamily,
			Ccy:        in.Arg.Ccy,
			Action:     in.Action,
			Data:       in.Data,
		})
	}
}

func (c *WSClient) deliverOperation(resp WSOperationResponse) {
	if ch, ok := c.ops.Load(operationResponsePendingKey(resp.ID, resp.Op)); ok {
		select {
		case ch.(chan WSOperationResponse) <- resp:
		default:
		}
	}
}

// deliverAck routes login/subscribe/unsubscribe/error events to the waiting
// caller by semantic key. OKX does NOT echo a client-supplied id for these
// operations, so correlation is done via the event type and the arg fields
// (channel, instId, etc.).
func (c *WSClient) deliverAck(event string, code, msg string, arg subArg) bool {
	var err error
	if event == "error" || (code != "" && code != "0") {
		err = wrapAPIError(&APIError{Code: code, Msg: msg})
	}

	switch event {
	case "login":
		return c.deliverPending(controlPendingKey("login", ""), err)
	case "subscribe", "unsubscribe":
		return c.deliverPending(controlPendingKey(event, arg.key()), err)
	case "error":
		// Errors with no channel arg are login errors; otherwise try
		// subscribe then unsubscribe.
		if arg.Channel == "" {
			return c.deliverPending(controlPendingKey("login", ""), err)
		}
		delivered := c.deliverPending(controlPendingKey("subscribe", arg.key()), err)
		if !delivered {
			delivered = c.deliverPending(controlPendingKey("unsubscribe", arg.key()), err)
		}
		return delivered
	default:
		return false
	}
}

func (c *WSClient) deliverPending(key string, err error) bool {
	c.pendingMu.Lock()
	ch := c.pending[key]
	if ch != nil {
		delete(c.pending, key)
	}
	c.pendingMu.Unlock()
	if ch == nil {
		return false
	}
	select {
	case ch <- err:
	default:
	}
	return true
}

func (c *WSClient) dispatchSystemEvent(event WSSystemEvent) {
	if c.systemEvents == nil {
		return
	}
	select {
	case c.systemEvents <- event:
	default:
		c.log.Warn("ws system event channel full", "event", event.Event)
	}
}

func isWSSystemEvent(event string) bool {
	switch event {
	case "channel-conn-count", "channel-conn-count-error", "notice":
		return true
	default:
		return false
	}
}

// login performs the WS login handshake. The signed timestamp is Unix epoch
// seconds (distinct from REST's ISO-8601 form).
func (c *WSClient) login(ctx context.Context) error {
	ts := strconv.FormatInt(c.now().Unix(), 10)
	sig := c.signer.Sign(ts, "GET", "/users/self/verify", "")
	msg := opMessage{
		Op: "login",
		Args: []any{loginArg{
			APIKey:     c.cred.APIKey,
			Passphrase: c.cred.Passphrase,
			Timestamp:  ts,
			Sign:       sig,
		}},
	}
	return c.sendAndWait(ctx, controlPendingKey("login", ""), msg, 10*time.Second)
}

// Login authenticates the current WebSocket connection. Private clients log in
// automatically during Connect; this method is useful for Business or manually
// authenticated connections.
func (c *WSClient) Login(ctx context.Context) error {
	if c.signer == nil {
		return ErrUnauthorized
	}
	return c.login(ctx)
}

// Subscribe registers handler for channel/instID and sends the subscribe op,
// waiting for the server ack. Handler is invoked on every subsequent push.
func (c *WSClient) Subscribe(ctx context.Context, channel, instID string, handler Handler) error {
	return c.SubscribeArg(ctx, WSChannelArg{Channel: channel, InstID: instID}, handler)
}

// WSChannelArg describes an OKX WebSocket subscription target.
type WSChannelArg struct {
	Channel    string
	InstType   string
	InstFamily string
	InstID     string
	Ccy        string
	UID        string
}

// WSSubscription is a live WebSocket subscription handle. Close removes the
// handler before sending unsubscribe so callbacks stop immediately from the
// caller's perspective.
type WSSubscription struct {
	c     *WSClient
	arg   WSChannelArg
	entry *subEntry
	once  sync.Once
	err   error
}

func (s *WSSubscription) Close(ctx context.Context) error {
	if s == nil || s.c == nil {
		return nil
	}
	s.once.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		s.err = s.c.unsubscribeArg(ctx, s.arg, true, s.entry)
	})
	return s.err
}

// WSOperationResponse is returned by WebSocket trading operations.
type WSOperationResponse struct {
	ID      string         `json:"id,omitzero"`
	Op      string         `json:"op,omitzero"`
	Event   string         `json:"event,omitzero"`
	Code    string         `json:"code,omitzero"`
	Msg     string         `json:"msg,omitzero"`
	Data    jsontext.Value `json:"data,omitzero"`
	InTime  string         `json:"inTime,omitzero"`
	OutTime string         `json:"outTime,omitzero"`
	Err     error          `json:"-"`
}

// WSSystemEvent is a low-frequency WebSocket control-plane event.
type WSSystemEvent struct {
	Event     string
	Code      string
	Msg       string
	ConnID    string
	Channel   string
	ConnCount string
	Arg       WSChannelArg
	Raw       []byte
}

func (r WSOperationResponse) DecodeData(out any) error {
	if len(r.Data) == 0 || out == nil {
		return nil
	}
	return json.Unmarshal(r.Data, out)
}

// Do sends an arbitrary WebSocket operation and waits for the matching id/op
// response. It is intended for private trading operations, not market pushes.
func (c *WSClient) Do(ctx context.Context, op string, args ...any) (WSOperationResponse, error) {
	if op == "" {
		return WSOperationResponse{}, required("op")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if c.rl != nil {
		if err := c.rl.Wait(ctx, "private:ws:"+op); err != nil {
			return WSOperationResponse{}, err
		}
	}
	id := c.reqID.Add(1)
	msg := opMessage{ID: id, Op: op, Args: args}
	return c.sendOperationAndWait(ctx, operationPendingKey(id, op), msg, 10*time.Second)
}

func (c *WSClient) PlaceOrder(ctx context.Context, req PlaceOrderRequest) ([]OrderAck, error) {
	if err := validatePlaceOrder(req); err != nil {
		return nil, err
	}
	return c.tradeOperation(ctx, "order", req)
}

func (c *WSClient) PlaceMultipleOrders(ctx context.Context, req []PlaceOrderRequest) ([]OrderAck, error) {
	return executeMultipleOrders(ctx, c, req, "batch-orders", validatePlaceOrder)
}

func (c *WSClient) CancelOrder(ctx context.Context, req CancelOrderRequest) ([]OrderAck, error) {
	if err := validateCancelOrder(req); err != nil {
		return nil, err
	}
	return c.tradeOperation(ctx, "cancel-order", req)
}

func (c *WSClient) CancelMultipleOrders(ctx context.Context, req []CancelOrderRequest) ([]OrderAck, error) {
	return executeMultipleOrders(ctx, c, req, "batch-cancel-orders", validateCancelOrder)
}

func (c *WSClient) AmendOrder(ctx context.Context, req AmendOrderRequest) ([]OrderAck, error) {
	if err := validateAmendOrder(req); err != nil {
		return nil, err
	}
	return c.tradeOperation(ctx, "amend-order", req)
}

func (c *WSClient) AmendMultipleOrders(ctx context.Context, req []AmendOrderRequest) ([]OrderAck, error) {
	return executeMultipleOrders(ctx, c, req, "batch-amend-orders", validateAmendOrder)
}

func executeMultipleOrders[T any](ctx context.Context, c *WSClient, req []T, op string, validate func(T) error) ([]OrderAck, error) {
	if len(req) == 0 {
		return nil, required("orders")
	}
	args := make([]any, 0, len(req))
	for i := range req {
		if err := validate(req[i]); err != nil {
			return nil, err
		}
		args = append(args, req[i])
	}
	return c.tradeOperationArgs(ctx, op, args...)
}

func (c *WSClient) tradeOperation(ctx context.Context, op string, arg any) ([]OrderAck, error) {
	return c.tradeOperationArgs(ctx, op, arg)
}

func (c *WSClient) tradeOperationArgs(ctx context.Context, op string, args ...any) ([]OrderAck, error) {
	resp, err := c.Do(ctx, op, args...)
	if err != nil {
		return nil, err
	}
	var out []OrderAck
	if len(resp.Data) > 0 {
		if err := resp.DecodeData(&out); err != nil {
			return nil, err
		}
	}
	if resp.Code != "" && resp.Code != "0" {
		apiErr := &APIError{Code: resp.Code, Msg: resp.Msg, Data: resp.Data}
		if resp.Code == "2" {
			return out, &PartialError{Err: apiErr}
		}
		return out, wrapAPIError(apiErr)
	}
	return out, nil
}

func (a WSChannelArg) subArg() subArg {
	return subArg(a)
}

// SubscribeArg registers handler for a structured OKX subscription argument.
// It supports channels that key by instType, ccy, or uid in addition to instId.
func (c *WSClient) SubscribeArg(ctx context.Context, arg WSChannelArg, handler Handler) error {
	_, err := c.SubscribeArgHandle(ctx, arg, handler)
	return err
}

// SubscribeArgHandle is like SubscribeArg and returns a closeable subscription
// handle. It is useful for callers that need deterministic handler removal.
func (c *WSClient) SubscribeArgHandle(ctx context.Context, arg WSChannelArg, handler Handler) (*WSSubscription, error) {
	wireArg := arg.subArg()
	if wireArg.Channel == "" {
		return nil, required("channel")
	}
	if handler == nil {
		return nil, errors.New("okx: nil websocket handler")
	}
	key := wireArg.key()

	entry := &subEntry{arg: wireArg, handler: handler}
	c.subMu.Lock()
	old := *c.handlers.Load()
	nm := make(map[string]*subEntry, len(old)+1)
	for k, v := range old {
		nm[k] = v
	}
	nm[key] = entry
	c.handlers.Store(&nm)
	c.subMu.Unlock()

	msg := opMessage{Op: "subscribe", Args: []any{wireArg}}
	if err := c.sendAndWait(ctx, controlPendingKey("subscribe", key), msg, 10*time.Second); err != nil {
		c.removeHandlerIfMatch(key, entry)
		return nil, err
	}
	return &WSSubscription{c: c, arg: WSChannelArg(wireArg), entry: entry}, nil
}

// Unsubscribe removes the handler and tells the server to stop the channel.
func (c *WSClient) Unsubscribe(ctx context.Context, channel, instID string) error {
	return c.UnsubscribeArg(ctx, WSChannelArg{Channel: channel, InstID: instID})
}

// UnsubscribeArg removes a structured subscription and tells OKX to stop it.
func (c *WSClient) UnsubscribeArg(ctx context.Context, arg WSChannelArg) error {
	return c.unsubscribeArg(ctx, arg, false, nil)
}

func (c *WSClient) unsubscribeArg(ctx context.Context, arg WSChannelArg, removeBeforeAck bool, entry *subEntry) error {
	wireArg := arg.subArg()
	key := wireArg.key()

	if removeBeforeAck {
		if entry != nil {
			c.removeHandlerIfMatch(key, entry)
		} else {
			c.removeHandler(key)
		}
	}
	msg := opMessage{Op: "unsubscribe", Args: []any{wireArg}}
	if err := c.sendAndWait(ctx, controlPendingKey("unsubscribe", key), msg, 10*time.Second); err != nil {
		return err
	}
	if !removeBeforeAck {
		c.removeHandler(key)
	}
	return nil
}

func (c *WSClient) removeHandler(key string) {
	c.subMu.Lock()
	old := *c.handlers.Load()
	nm := make(map[string]*subEntry, len(old))
	for k, v := range old {
		if k != key {
			nm[k] = v
		}
	}
	c.handlers.Store(&nm)
	c.subMu.Unlock()
}

func (c *WSClient) removeHandlerIfMatch(key string, entry *subEntry) {
	c.subMu.Lock()
	old := *c.handlers.Load()
	if old[key] != entry {
		c.subMu.Unlock()
		return
	}
	nm := make(map[string]*subEntry, len(old))
	for k, v := range old {
		if k != key {
			nm[k] = v
		}
	}
	c.handlers.Store(&nm)
	c.subMu.Unlock()
}

// resubscribeAll replays every registered subscription after a reconnect.
func (c *WSClient) resubscribeAll(ctx context.Context) error {
	m := *c.handlers.Load()
	if len(m) == 0 {
		return nil
	}
	args := make([]any, 0, len(m))
	for _, e := range m {
		args = append(args, e.arg)
	}
	return c.write(ctx, opMessage{Op: "subscribe", Args: args})
}

// sendAndWait registers a one-shot ack waiter keyed by a semantic string,
// sends msg, and blocks until the ack arrives, ctx is done, or the timeout
// fires. If a pending waiter already exists for the same key, it returns
// ErrWSConcurrentOperation immediately.
func (c *WSClient) sendAndWait(ctx context.Context, key string, msg opMessage, timeout time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}

	ack := make(chan error, 1)
	c.pendingMu.Lock()
	if _, exists := c.pending[key]; exists {
		c.pendingMu.Unlock()
		return fmt.Errorf("%w: %s", ErrWSConcurrentOperation, key)
	}
	c.pending[key] = ack
	c.pendingMu.Unlock()

	defer func() {
		c.pendingMu.Lock()
		if c.pending[key] == ack {
			delete(c.pending, key)
		}
		c.pendingMu.Unlock()
	}()

	if err := c.write(ctx, msg); err != nil {
		return err
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case err := <-ack:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return context.DeadlineExceeded
	}
}

func (c *WSClient) sendOperationAndWait(ctx context.Context, key string, msg any, timeout time.Duration) (WSOperationResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	resp := make(chan WSOperationResponse, 1)
	c.ops.Store(key, resp)
	defer c.ops.Delete(key)

	if err := c.write(ctx, msg); err != nil {
		return WSOperationResponse{}, err
	}
	t := time.NewTimer(timeout)
	defer t.Stop()
	select {
	case r := <-resp:
		if r.Err != nil {
			return WSOperationResponse{}, r.Err
		}
		return r, nil
	case <-ctx.Done():
		return WSOperationResponse{}, ctx.Err()
	case <-t.C:
		return WSOperationResponse{}, context.DeadlineExceeded
	}
}

// write marshals v and sends it as one text frame under the write mutex.
func (c *WSClient) write(ctx context.Context, v any) error {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	if err := json.MarshalWrite(buf, v); err != nil {
		return err
	}
	return c.writeFrame(ctx, buf.Bytes())
}

func (c *WSClient) writeText(ctx context.Context, s string) error {
	return c.writeFrame(ctx, []byte(s))
}

func (c *WSClient) writeFrame(ctx context.Context, b []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	conn := c.getConn()
	if conn == nil {
		return ErrWSNotConnected
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return conn.Write(ctx, websocket.MessageText, b)
}

func (c *WSClient) failPending(err error) {
	if err == nil {
		return
	}
	c.pendingMu.Lock()
	for _, ch := range c.pending {
		select {
		case ch <- err:
		default:
		}
	}
	c.pending = make(map[string]chan error)
	c.pendingMu.Unlock()
	c.ops.Range(func(_, value any) bool {
		ch := value.(chan WSOperationResponse)
		select {
		case ch <- WSOperationResponse{Err: err}:
		default:
		}
		return true
	})
}

// Close cancels all goroutines and waits for them to exit. Idempotent-safe to
// call once; subsequent Subscribes will fail as not-connected.
func (c *WSClient) Close() error {
	cancel, conn := c.closeState()
	if cancel != nil {
		cancel()
	}
	if conn != nil {
		_ = conn.Close(websocket.StatusNormalClosure, "client closing")
	}
	c.wg.Wait()
	return nil
}

// Reconnects reports how many times the connection has been re-established.
func (c *WSClient) Reconnects() int64 { return c.reconnects.Load() }

func (c *WSClient) setConn(conn *websocket.Conn) {
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()
}

func (c *WSClient) clearConn(conn *websocket.Conn) {
	c.connMu.Lock()
	if c.conn == conn {
		c.conn = nil
	}
	c.connMu.Unlock()
}

func (c *WSClient) getConn() *websocket.Conn {
	c.connMu.RLock()
	conn := c.conn
	c.connMu.RUnlock()
	return conn
}

func (c *WSClient) closeState() (context.CancelFunc, *websocket.Conn) {
	c.connMu.Lock()
	cancel := c.cancel
	conn := c.conn
	c.cancel = nil
	c.run = nil
	c.connMu.Unlock()
	return cancel, conn
}

func (c *WSClient) clearRun(run *struct{}) {
	c.connMu.Lock()
	if c.run == run {
		c.cancel = nil
		c.run = nil
	}
	c.connMu.Unlock()
}

// sleep waits d or until ctx is done; returns false if ctx was cancelled.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func defaultWSReconnectConfig() WSReconnectConfig {
	return WSReconnectConfig{
		Enabled:  true,
		MinDelay: defaultWSReconnectMinDelay,
		MaxDelay: defaultWSReconnectMaxDelay,
	}
}

func normalizeWSReconnectConfig(config WSReconnectConfig) WSReconnectConfig {
	if config.MinDelay <= 0 {
		config.MinDelay = defaultWSReconnectMinDelay
	}
	if config.MaxDelay <= 0 {
		config.MaxDelay = defaultWSReconnectMaxDelay
	}
	if config.MaxDelay < config.MinDelay {
		config.MaxDelay = config.MinDelay
	}
	return config
}

func canReconnect(config WSReconnectConfig, attempts int) bool {
	return config.MaxAttempts <= 0 || attempts < config.MaxAttempts
}

func operationPendingKey(id uint64, op string) string {
	return "operation:" + op + ":" + strconv.FormatUint(id, 10)
}

func operationResponsePendingKey(id, op string) string { return "operation:" + op + ":" + id }

// controlPendingKey builds the semantic pending-map key for login/subscribe/
// unsubscribe control-plane operations. For login the subKey is empty; for
// subscribe/unsubscribe it is the subArg.key().
func controlPendingKey(op, subKey string) string {
	return "control:" + op + ":" + subKey
}

// subKey is kept for tests and old internal call sites.
func subKey(channel, instID string) string {
	return subArg{Channel: channel, InstID: instID}.key()
}

// ---- wire types ----

type opMessage struct {
	ID   uint64 `json:"id,string,omitzero"`
	Op   string `json:"op"`
	Args []any  `json:"args"`
}

type subArg struct {
	Channel    string `json:"channel"`
	InstType   string `json:"instType,omitzero"`
	InstFamily string `json:"instFamily,omitzero"`
	InstID     string `json:"instId,omitzero"`
	Ccy        string `json:"ccy,omitzero"`
	UID        string `json:"uid,omitzero"`
}

func (a subArg) key() string {
	return a.Channel +
		"|instType=" + a.InstType +
		"|instFamily=" + a.InstFamily +
		"|instId=" + a.InstID +
		"|ccy=" + a.Ccy +
		"|uid=" + a.UID
}

func (a subArg) channelArg() WSChannelArg {
	return WSChannelArg(a)
}

type loginArg struct {
	APIKey     string `json:"apiKey"`
	Passphrase string `json:"passphrase"`
	Timestamp  string `json:"timestamp"`
	Sign       string `json:"sign"`
}
