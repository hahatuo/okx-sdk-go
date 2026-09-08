package okx

import (
	"fmt"
	"sync"
	"sync/atomic"
)

type instrumentSnapshot struct {
	version uint64
	byID    map[string]Instrument
	byCode  map[int64]string
}

// InstrumentCatalog publishes immutable instrument snapshots. Seed it with
// Public.Instruments, then Update it from instruments pushes or periodic REST
// refreshes. Use separate catalogs for production and demo code mappings.
// Its zero value is ready for use.
type InstrumentCatalog struct {
	mu       sync.Mutex
	snapshot atomic.Pointer[instrumentSnapshot]
}

// Update atomically replaces the supplied instruments and advances the version.
// Instruments omitted from an update are retained.
func (c *InstrumentCatalog) Update(rows []Instrument) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	next := &instrumentSnapshot{version: 1, byID: make(map[string]Instrument), byCode: make(map[int64]string)}
	if old := c.snapshot.Load(); old != nil {
		next.version = old.version + 1
		for id, inst := range old.byID {
			next.byID[id] = inst
		}
	}
	for _, inst := range rows {
		if inst.InstID == "" {
			return required("instId")
		}
		next.byID[inst.InstID] = inst
	}
	for id, inst := range next.byID {
		if inst.InstIDCode == 0 {
			continue
		}
		if other := next.byCode[inst.InstIDCode]; other != "" && other != id {
			return fmt.Errorf("%w: duplicate instIdCode", ErrInvalidParameter)
		}
		next.byCode[inst.InstIDCode] = id
	}
	c.snapshot.Store(next)
	return nil
}

func (c *InstrumentCatalog) Lookup(id string) (Instrument, uint64, bool) {
	if c != nil {
		if s := c.snapshot.Load(); s != nil {
			inst, ok := s.byID[id]
			return inst, s.version, ok
		}
	}
	return Instrument{}, 0, false
}

func (c *InstrumentCatalog) resolve(id string, code int64) (string, error) {
	if code == 0 {
		if id == "" {
			return "", required("instId")
		}
		return id, nil
	}
	if c != nil {
		if s := c.snapshot.Load(); s != nil {
			resolved := s.byCode[code]
			if resolved != "" && (id == "" || id == resolved) {
				return resolved, nil
			}
			if resolved != "" {
				return "", fmt.Errorf("%w: instId and instIdCode differ", ErrInvalidParameter)
			}
		}
	}
	// Without a catalog both encodings could consume different quotas. Refuse
	// code-based requests until their identity has been established.
	return "", fmt.Errorf("%w: load instIdCode %d into InstrumentCatalog first", ErrInvalidParameter, code)
}

// Rules returns a detached rule snapshot and the catalog version that produced it.
func (c *InstrumentCatalog) Rules(id string) (InstrumentRules, uint64, error) {
	inst, version, ok := c.Lookup(id)
	if !ok {
		return InstrumentRules{}, version, ErrNotFound
	}
	rules, err := inst.Rules()
	return rules, version, err
}

func WithInstrumentCatalog(catalog *InstrumentCatalog) Option {
	return func(c *Client) { c.catalog = catalog }
}
func WithWSInstrumentCatalog(catalog *InstrumentCatalog) WSOption {
	return func(c *WSClient) { c.catalog = catalog }
}

// wsCode resolves the public SDK identifier into the current WS wire identity.
// Code-only requests can be sent without a catalog when rate limiting is disabled.
func (c *InstrumentCatalog) wsCode(id string, code int64) (int64, error) {
	if code < 0 {
		return 0, fmt.Errorf("%w: instIdCode must be positive", ErrInvalidParameter)
	}
	if code != 0 {
		if id != "" {
			if _, err := c.resolve(id, code); err != nil {
				return 0, err
			}
		}
		return code, nil
	}
	inst, _, ok := c.Lookup(id)
	if !ok || inst.InstIDCode <= 0 {
		return 0, fmt.Errorf("%w: load instIdCode for %q into InstrumentCatalog first", ErrInvalidParameter, id)
	}
	return inst.InstIDCode, nil
}
