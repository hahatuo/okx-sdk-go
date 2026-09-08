package okx

import (
	"context"
	"errors"
	"fmt"
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

// DefaultWSReadLimit bounds complete messages, including large snapshots.
const DefaultWSReadLimit int64 = 1024 * 1024

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
	UID        string
	EventType  string
	CurPage    int
	LastPage   bool
}

// Into decodes the raw data array into out.
func (m Message) Into(out any) error { return json.Unmarshal(m.Data, out) }

// Handler is invoked directly on the read goroutine for every push on a
// subscribed channel. It MUST NOT block: heavy work should be handed to the
// caller's own worker. Do not call methods that wait for ACKs or Close from
// a Handler; use RequestClose for shutdown. Keep it allocation-light.
type Handler func(Message)

// subEntry records an active subscription so it can be replayed after reconnect.
type subEntry struct {
	arg          subArg
	handler      Handler
	desired      atomic.Bool
	active       atomic.Pointer[wsSession]
	onDisconnect func(error)
}

// WSClient is a single OKX WebSocket connection with automatic reconnect,
// re-login and re-subscribe. Safe for concurrent Subscribe/Close.
type WSClient struct {
	url       string
	kind      WSKind
	cred      Credentials
	signer    *sign.Signer
	log       Logger
	now       func() time.Time
	dialFn    func(context.Context, string) (*websocket.Conn, error)
	pingInt   time.Duration
	rl        RateLimiter
	rateScope string
	catalog   *InstrumentCatalog

	reconnect WSReconnectConfig

	// connection state
	connMu  sync.RWMutex
	session *wsSession

	// handlers is a copy-on-write map read lock-free on the dispatch hot path.
	// Writers (Subscribe/Unsubscribe) swap in a fresh map under subMu.
	handlers atomic.Pointer[map[string]*subEntry]
	subMu    sync.Mutex

	// Reservations serialize subscription changes without blocking the read loop.
	subBusy        map[string]bool
	readLimit      int64
	requestTimeout time.Duration
	loginRequired  atomic.Bool

	systemEvents chan WSSystemEvent

	reconnects atomic.Int64
	reqID      atomic.Uint64

	run *wsRun
}

type WSReconnectConfig struct {
	Enabled     bool
	MinDelay    time.Duration
	MaxDelay    time.Duration
	MaxAttempts int
}

// WSOption configures a WSClient.
type WSOption func(*WSClient)

// WithWSReadLimit bounds complete messages, not individual wire fragments.
// Non-positive values retain the default bound.
func WithWSReadLimit(bytes int64) WSOption {
	return func(c *WSClient) {
		if bytes > 0 {
			c.readLimit = bytes
		}
	}
}

// WithWSRequestTimeout bounds rate-limit admission, writes and ACK waits.
func WithWSRequestTimeout(timeout time.Duration) WSOption {
	return func(c *WSClient) {
		if timeout > 0 {
			c.requestTimeout = timeout
		}
	}
}

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
		url:            wsURL(kind, false),
		kind:           kind,
		log:            noopLogger{},
		now:            time.Now,
		pingInt:        20 * time.Second,
		rl:             DefaultRateLimiter(),
		reconnect:      defaultWSReconnectConfig(),
		systemEvents:   make(chan WSSystemEvent, defaultWSSystemEventBuffer),
		subBusy:        make(map[string]bool),
		readLimit:      DefaultWSReadLimit,
		requestTimeout: 10 * time.Second,
	}
	c.dialFn = defaultDial
	empty := make(map[string]*subEntry)
	c.handlers.Store(&empty)
	for _, o := range opts {
		o(c)
	}
	c.rateScope = accountRateScope(c.rateScope, c.cred.APIKey)
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
	if err == nil {
		conn.SetReadLimit(DefaultWSReadLimit)
	}
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
// handler before sending unsubscribe. A callback already in progress may finish.
type WSSubscription struct {
	c     *WSClient
	arg   WSChannelArg
	entry *subEntry
}

func (s *WSSubscription) Close(ctx context.Context) error {
	if s == nil || s.c == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return s.c.unsubscribeArg(ctx, s.arg, true, s.entry)
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
	Err       error
}

func (r WSOperationResponse) DecodeData(out any) error {
	if len(r.Data) == 0 || out == nil {
		return nil
	}
	return json.Unmarshal(r.Data, out)
}

func (c *WSClient) PlaceOrder(ctx context.Context, req PlaceOrderRequest) ([]OrderAck, error) {
	if err := validatePlaceOrder(req); err != nil {
		return nil, err
	}
	return c.tradeOperationArgs(ctx, "order", req)
}

func (c *WSClient) PlaceMultipleOrders(ctx context.Context, req []PlaceOrderRequest) ([]OrderAck, error) {
	return executeMultipleOrders(ctx, c, req, "batch-orders", validatePlaceOrder)
}

func (c *WSClient) CancelOrder(ctx context.Context, req CancelOrderRequest) ([]OrderAck, error) {
	if err := validateCancelOrder(req); err != nil {
		return nil, err
	}
	return c.tradeOperationArgs(ctx, "cancel-order", req)
}

func (c *WSClient) CancelMultipleOrders(ctx context.Context, req []CancelOrderRequest) ([]OrderAck, error) {
	return executeMultipleOrders(ctx, c, req, "batch-cancel-orders", validateCancelOrder)
}

func (c *WSClient) AmendOrder(ctx context.Context, req AmendOrderRequest) ([]OrderAck, error) {
	if err := validateAmendOrder(req); err != nil {
		return nil, err
	}
	return c.tradeOperationArgs(ctx, "amend-order", req)
}

func (c *WSClient) AmendMultipleOrders(ctx context.Context, req []AmendOrderRequest) ([]OrderAck, error) {
	return executeMultipleOrders(ctx, c, req, "batch-amend-orders", validateAmendOrder)
}

func executeMultipleOrders[T any](ctx context.Context, c *WSClient, req []T, op string, validate func(T) error) ([]OrderAck, error) {
	if len(req) == 0 {
		return nil, required("orders")
	}
	if len(req) > MaxBatchOrderRequests {
		return nil, fmt.Errorf("%w: batch exceeds %d orders", ErrInvalidParameter, MaxBatchOrderRequests)
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

func (c *WSClient) tradeOperationArgs(ctx context.Context, op string, args ...any) ([]OrderAck, error) {
	// The typed entry points own this argument slice and copy each request value.
	// REST still uses InstID; WS sends only InstIDCode.
	for i, arg := range args {
		var err error
		switch req := arg.(type) {
		case PlaceOrderRequest:
			req.InstIDCode, err = c.catalog.wsCode(req.InstID, req.InstIDCode)
			req.InstID = ""
			args[i] = req
		case CancelOrderRequest:
			req.InstIDCode, err = c.catalog.wsCode(req.InstID, req.InstIDCode)
			req.InstID = ""
			args[i] = req
		case AmendOrderRequest:
			req.InstIDCode, err = c.catalog.wsCode(req.InstID, req.InstIDCode)
			req.InstID = ""
			args[i] = req
		}
		if err != nil {
			return nil, err
		}
	}
	resp, err := c.Do(ctx, op, args...)
	if err != nil {
		return nil, err
	}
	var out []OrderAck
	if err := resp.DecodeData(&out); err != nil {
		return nil, err
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
// It supports instType, instFamily and ccy filters; uid is server metadata.
func (c *WSClient) SubscribeArg(ctx context.Context, arg WSChannelArg, handler Handler) error {
	_, err := c.SubscribeArgHandle(ctx, arg, handler)
	return err
}

// Unsubscribe removes the handler and tells the server to stop the channel.
func (c *WSClient) Unsubscribe(ctx context.Context, channel, instID string) error {
	return c.UnsubscribeArg(ctx, WSChannelArg{Channel: channel, InstID: instID})
}

// UnsubscribeArg removes a structured subscription and tells OKX to stop it.
func (c *WSClient) UnsubscribeArg(ctx context.Context, arg WSChannelArg) error {
	return c.unsubscribeArg(ctx, arg, false, nil)
}

// Reconnects reports how many times the connection has been re-established.
func (c *WSClient) Reconnects() int64 { return c.reconnects.Load() }

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
