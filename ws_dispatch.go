package okx

import (
	"fmt"
	"strconv"

	json "github.com/go-json-experiment/json"
	"github.com/go-json-experiment/json/jsontext"
)

type wsIncoming struct {
	ID        string         `json:"id"`
	Op        string         `json:"op"`
	Event     string         `json:"event"`
	Arg       subArg         `json:"arg"`
	Action    string         `json:"action"`
	Data      jsontext.Value `json:"data"`
	Code      string         `json:"code"`
	Msg       string         `json:"msg"`
	ConnID    string         `json:"connId"`
	Channel   string         `json:"channel"`
	ConnCnt   string         `json:"connCount"`
	InTime    string         `json:"inTime"`
	OutTime   string         `json:"outTime"`
	EventType string         `json:"eventType"`
	CurPage   int            `json:"curPage"`
	LastPage  bool           `json:"lastPage"`
}

func (c *WSClient) dispatchSession(s *wsSession, data []byte, in *wsIncoming) error {
	if string(data) == "pong" {
		return nil
	}
	*in = wsIncoming{Data: in.Data[:0]}
	if err := json.Unmarshal(data, in); err != nil {
		return fmt.Errorf("okx: decode websocket frame: %w", err)
	}
	if in.Event != "" || in.Op != "" {
		delivered := false
		if s != nil {
			delivered = s.deliver(in)
		}
		if isWSSystemEvent(in.Event) || (!delivered && in.Event == "error") {
			c.dispatchSystemEvent(WSSystemEvent{Event: in.Event, Code: in.Code, Msg: in.Msg, ConnID: in.ConnID, Channel: in.Channel, ConnCount: in.ConnCnt, Arg: in.Arg.channelArg(), Raw: append([]byte(nil), data...)})
		}
		return nil
	}
	if in.Arg.Channel == "" {
		return nil
	}
	msg := Message{Channel: in.Arg.Channel, InstID: in.Arg.InstID, InstType: in.Arg.InstType, InstFamily: in.Arg.InstFamily, Ccy: in.Arg.Ccy, UID: in.Arg.UID, Action: in.Action, Data: in.Data, EventType: in.EventType, CurPage: in.CurPage, LastPage: in.LastPage}
	handlers := *c.handlers.Load()
	if in.Arg.Channel == WSChannelAccount || in.Arg.Channel == WSChannelOrders {
		// Private push arguments contain server metadata and can describe a
		// narrower instrument than an ANY/family subscription. Filter by the
		// channel's actual subscription fields, never by uid metadata.
		for _, entry := range handlers {
			if privateRouteMatches(entry.arg, in.Arg) && (s == nil || entry.active.Load() == s) {
				entry.handler(msg)
			}
		}
		return nil
	}
	arg := in.Arg
	arg.UID = ""
	if entry := handlers[arg.key()]; entry != nil && (s == nil || entry.active.Load() == s) {
		entry.handler(msg)
	}
	return nil
}

func privateRouteMatches(want, got subArg) bool {
	if want.Channel != got.Channel {
		return false
	}
	if want.Ccy != "" && got.Ccy != "" && want.Ccy != got.Ccy {
		return false
	}
	if want.InstType != "" && want.InstType != "ANY" && got.InstType != "" && got.InstType != "ANY" && want.InstType != got.InstType {
		return false
	}
	if want.InstID != "" && got.InstID != "" && want.InstID != got.InstID {
		return false
	}
	if want.InstFamily != "" && got.InstFamily != "" && want.InstFamily != got.InstFamily {
		return false
	}
	return true
}

func (s *wsSession) deliver(in *wsIncoming) bool {
	id, _ := strconv.ParseUint(in.ID, 10, 64)
	s.mu.Lock()
	if id == 0 && in.ID == "" && in.Event == "login" {
		// OKX login ACKs may omit id. Only login uses this fallback; every
		// subscribe/unsubscribe request carries an id and requires its echo.
		for key, p := range s.pending {
			if p.op == "login" {
				id = key
				break
			}
		}
	}
	// A login error can likewise omit id/arg. A single login attempt is the
	// only unambiguous target; never guess a subscription from an error.
	if id == 0 && in.ID == "" && in.Event == "error" && in.Arg.Channel == "" {
		for key, p := range s.pending {
			if p.op == "login" {
				id = key
				break
			}
		}
	}
	p := s.pending[id]
	if p == nil {
		s.mu.Unlock()
		return false
	}
	if (p.control && in.Event != p.op && in.Event != "error") || (!p.control && in.Op != p.op && in.Event != "error") {
		s.mu.Unlock()
		return false
	}
	delete(s.pending, id)
	response := WSOperationResponse{ID: in.ID, Op: p.op, Event: in.Event, Code: in.Code, Msg: in.Msg, InTime: in.InTime, OutTime: in.OutTime}
	if p.control {
		if in.Event == "error" || (in.Code != "" && in.Code != "0") {
			response.Err = wrapAPIError(&APIError{Code: in.Code, Msg: in.Msg})
		}
	} else {
		// Trading results outlive this callback and must not alias the reused
		// connection envelope. Market messages remain borrowed during dispatch.
		response.Data = append(jsontext.Value(nil), in.Data...)
	}
	if response.Err == nil && p.onSuccess != nil {
		p.onSuccess()
	}
	p.done <- response
	s.mu.Unlock()
	return true
}
