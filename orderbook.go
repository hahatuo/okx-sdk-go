package okx

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ErrOrderBookSequenceGap = errors.New("okx: order book sequence gap")
var ErrOrderBookNotReady = errors.New("okx: order book has no snapshot")
var ErrInvalidOrderBook = errors.New("okx: invalid order book data")

type OrderBookSequenceGapError struct {
	ExpectedPrevSeqID int64
	PrevSeqID         int64
	SeqID             int64
}

func (e *OrderBookSequenceGapError) Error() string {
	return fmt.Sprintf("okx: order book sequence gap: expected prevSeqId %d, got prevSeqId %d seqId %d",
		e.ExpectedPrevSeqID, e.PrevSeqID, e.SeqID)
}

func (e *OrderBookSequenceGapError) Is(target error) bool {
	return target == ErrOrderBookSequenceGap
}

// LocalOrderBook maintains one in-memory OKX order book. It is intentionally
// unsynchronized: own it from the read-loop/event-loop goroutine for the lowest
// overhead, or wrap it externally if cross-goroutine reads are required.
type LocalOrderBook struct {
	instID        string
	viewDepth     int
	retainedDepth int
	ready         bool
	seqID         int64
	ts            string
	bids          []BookLevel
	asks          []BookLevel
}

// NewLocalOrderBook creates a book that retains the maximum OKX WebSocket
// depth internally. maxDepth limits only the levels exposed by Bids and Asks;
// zero exposes every retained level.
func NewLocalOrderBook(instID string, maxDepth int) *LocalOrderBook {
	return newLocalOrderBook(instID, maxDepth, MaxOrderBookDepth)
}

func newLocalOrderBook(instID string, viewDepth, retainedDepth int) *LocalOrderBook {
	if viewDepth < 0 {
		viewDepth = 0
	}
	if retainedDepth <= 0 {
		retainedDepth = MaxOrderBookDepth
	}
	capacity := retainedDepth * 2
	return &LocalOrderBook{
		instID:        instID,
		viewDepth:     viewDepth,
		retainedDepth: retainedDepth,
		bids:          make([]BookLevel, 0, capacity),
		asks:          make([]BookLevel, 0, capacity),
	}
}

func (b *LocalOrderBook) Reset() {
	b.ready = false
	b.seqID = 0
	b.ts = ""
	b.bids = b.bids[:0]
	b.asks = b.asks[:0]
}

func (b *LocalOrderBook) Ready() bool       { return b.ready }
func (b *LocalOrderBook) InstID() string    { return b.instID }
func (b *LocalOrderBook) SeqID() int64      { return b.seqID }
func (b *LocalOrderBook) Timestamp() string { return b.ts }
func (b *LocalOrderBook) Bids() []BookLevel { return visibleLevels(b.bids, b.viewDepth) }
func (b *LocalOrderBook) Asks() []BookLevel { return visibleLevels(b.asks, b.viewDepth) }

func visibleLevels(levels []BookLevel, depth int) []BookLevel {
	if depth > 0 && len(levels) > depth {
		return levels[:depth:depth]
	}
	return levels[:len(levels):len(levels)]
}

// Apply invalidates the book on any rejected update. A new snapshot is required
// before further updates. Bids/Asks return read-only views valid until Apply/Reset.
func (b *LocalOrderBook) Apply(action string, data OrderBook) (err error) {
	defer func() {
		if err != nil {
			b.Reset()
		}
	}()
	if action == "" {
		action = "snapshot"
	}
	switch action {
	case "snapshot":
		if _, err := parseBookSequence(data.SeqID); err != nil {
			return err
		}
		if data.PrevSeqID != "" && data.PrevSeqID != "-1" {
			return fmt.Errorf("%w: snapshot prevSeqId must be -1", ErrInvalidOrderBook)
		}
		if err := validateBookLevels(data.Asks, false, true); err != nil {
			return err
		}
		if err := validateBookLevels(data.Bids, true, true); err != nil {
			return err
		}
		b.replace(data)
		return nil
	case "update":
		if !b.ready {
			return ErrOrderBookNotReady
		}
		prevSeqID, err := parseBookSequence(data.PrevSeqID)
		if err != nil {
			return err
		}
		seqID, err := parseBookSequence(data.SeqID)
		if err != nil {
			return err
		}
		if prevSeqID != b.seqID {
			return &OrderBookSequenceGapError{
				ExpectedPrevSeqID: b.seqID,
				PrevSeqID:         prevSeqID,
				SeqID:             seqID,
			}
		}
		if err := validateBookLevels(data.Asks, false, false); err != nil {
			return err
		}
		if err := validateBookLevels(data.Bids, true, false); err != nil {
			return err
		}
		b.asks = applyLevels(b.asks, data.Asks, false)
		b.bids = applyLevels(b.bids, data.Bids, true)
		b.truncate()
		b.seqID = seqID
		b.ts = data.TS
		return nil
	default:
		return fmt.Errorf("okx: unsupported order book action %q", action)
	}
}

func (b *LocalOrderBook) ApplyMessage(msg WSTypedMessage[OrderBook]) error {
	if len(msg.Data) == 0 && msg.Err == nil {
		b.Reset()
		return ErrInvalidOrderBook
	}
	if msg.Err != nil {
		b.Reset()
		return msg.Err
	}
	for i := range msg.Data {
		if err := b.Apply(msg.Action, msg.Data[i]); err != nil {
			return err
		}
	}
	return nil
}

func (b *LocalOrderBook) replace(data OrderBook) {
	b.asks = replaceLevels(b.asks, data.Asks)
	b.bids = replaceLevels(b.bids, data.Bids)
	b.truncate()
	b.seqID, _ = parseBookSequence(data.SeqID) // validated before mutation
	b.ts = data.TS
	b.ready = true
}

func (b *LocalOrderBook) truncate() {
	if b.retainedDepth <= 0 {
		return
	}
	if len(b.asks) > b.retainedDepth {
		clear(b.asks[b.retainedDepth:])
		b.asks = b.asks[:b.retainedDepth]
	}
	if len(b.bids) > b.retainedDepth {
		clear(b.bids[b.retainedDepth:])
		b.bids = b.bids[:b.retainedDepth]
	}
}

func replaceLevels(dst, src []BookLevel) []BookLevel {
	if cap(dst) < len(src) {
		dst = make([]BookLevel, len(src))
	} else {
		dst = dst[:len(src)]
	}
	copy(dst, src)
	return dst
}

func applyLevels(levels []BookLevel, updates []BookLevel, descending bool) []BookLevel {
	for i := range updates {
		levels = upsertLevel(levels, updates[i], descending)
	}
	return levels
}

// upsertLevel receives levels validated by Apply before any mutation.
func upsertLevel(levels []BookLevel, update BookLevel, descending bool) []BookLevel {
	price := update.Price()
	for i := range levels {
		cmp := cmpDecimal(price, levels[i].Price())
		if cmp == 0 {
			if update.IsDelete() {
				copy(levels[i:], levels[i+1:])
				var zero BookLevel
				levels[len(levels)-1] = zero
				return levels[:len(levels)-1]
			}
			levels[i] = update
			return levels
		}
		if (descending && cmp > 0) || (!descending && cmp < 0) {
			if update.IsDelete() {
				return levels
			}
			levels = append(levels, BookLevel{})
			copy(levels[i+1:], levels[i:])
			levels[i] = update
			return levels
		}
	}
	if update.IsDelete() {
		return levels
	}
	return append(levels, update)
}

func parseBookSequence(v Num) (int64, error) {
	out, err := strconv.ParseInt(v.String(), 10, 64)
	if err != nil || out < 0 {
		return 0, fmt.Errorf("%w: invalid sequence %q", ErrInvalidOrderBook, v)
	}
	return out, nil
}

func validateBookLevels(levels []BookLevel, descending, snapshot bool) error {
	for i := range levels {
		p, size := levels[i].Price(), levels[i].Size()
		if !unsignedDecimal(p) || isZeroDecimal(p) || !unsignedDecimal(size) || (snapshot && isZeroDecimal(size)) {
			return fmt.Errorf("%w: invalid price/size at level %d", ErrInvalidOrderBook, i)
		}
		if snapshot && i > 0 {
			cmp := cmpDecimal(levels[i-1].Price(), p)
			if (descending && cmp <= 0) || (!descending && cmp >= 0) {
				return fmt.Errorf("%w: snapshot levels are not strictly sorted", ErrInvalidOrderBook)
			}
		}
	}
	return nil
}

func unsignedDecimal(s string) bool {
	if len(s) == 0 {
		return false
	}
	dot, digits := false, 0
	for i := range s {
		if s[i] >= '0' && s[i] <= '9' {
			digits++
			continue
		}
		if s[i] == '.' && !dot {
			dot = true
			continue
		}
		return false
	}
	return digits > 0
}

func cmpDecimal(a, b string) int {
	if a == b {
		return 0
	}
	ai, af := splitDecimal(a)
	bi, bf := splitDecimal(b)
	ai = trimLeadingZeros(ai)
	bi = trimLeadingZeros(bi)
	if len(ai) > len(bi) {
		return 1
	}
	if len(ai) < len(bi) {
		return -1
	}
	if c := strings.Compare(ai, bi); c != 0 {
		if c > 0 {
			return 1
		}
		return -1
	}
	maxFrac := len(af)
	if len(bf) > maxFrac {
		maxFrac = len(bf)
	}
	for i := 0; i < maxFrac; i++ {
		ac := byte('0')
		if i < len(af) {
			ac = af[i]
		}
		bc := byte('0')
		if i < len(bf) {
			bc = bf[i]
		}
		if ac > bc {
			return 1
		}
		if ac < bc {
			return -1
		}
	}
	return 0
}

func splitDecimal(s string) (string, string) {
	s = strings.TrimPrefix(s, "+")
	if idx := strings.IndexByte(s, '.'); idx >= 0 {
		return s[:idx], s[idx+1:]
	}
	return s, ""
}

func trimLeadingZeros(s string) string {
	for len(s) > 1 && s[0] == '0' {
		s = s[1:]
	}
	if s == "" {
		return "0"
	}
	return s
}
