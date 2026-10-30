// Package poolproto is the Go mirror of the pool protocol v1 contract
// (see ../catalog.md and ../schemas). Structs and JSON tags mirror the JSON
// schemas one-to-one; examples_test.go decodes every golden example and
// round-trips it to catch struct/schema drift.
//
// Stdlib only — no external dependencies. The protocol: router <->
// coordinator messages share one envelope (Event), sent as POST /pool/events
// (router -> coordinator) or as SSE data: lines on the held-open
// GET /pool/subscribe (coordinator -> router).
package poolproto

import (
	"encoding/json"
	"fmt"
)

// Message type names. Stable, snake_case; closed set for protocol v1.
const (
	TypeHandshake       = "handshake"
	TypeCapacity        = "capacity"
	TypeWorkOffer       = "work_offer"
	TypeOfferAccept     = "offer_accept"
	TypeOfferReject     = "offer_reject"
	TypeBodyAbort       = "body_abort"
	TypeCompletion      = "completion"
	TypeError           = "error"
	TypeAffinityReport  = "affinity_report"
	TypeExclusionUpdate = "exclusion_update"
	TypeEvictionNotice  = "eviction_notice"
)

// Event is the pool protocol envelope. All router -> coordinator messages are
// POSTed to /pool/events as an Event; coordinator -> router messages arrive
// as SSE data: lines on /pool/subscribe with the same shape.
type Event struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// Decode parses Event.Payload into the typed struct for Event.Type and
// returns it. Heartbeats (SSE comment lines ": hb <unix_ms>") are not events.
func (ev Event) Decode() (any, error) {
	var out any
	switch ev.Type {
	case TypeHandshake:
		out = &Handshake{}
	case TypeCapacity:
		out = &Capacity{}
	case TypeWorkOffer:
		out = &WorkOffer{}
	case TypeOfferAccept:
		out = &OfferAccept{}
	case TypeOfferReject:
		out = &OfferReject{}
	case TypeBodyAbort:
		out = &BodyAbort{}
	case TypeCompletion:
		out = &Completion{}
	case TypeError:
		out = &ErrorReport{}
	case TypeAffinityReport:
		out = &AffinityReport{}
	case TypeExclusionUpdate:
		out = &ExclusionUpdate{}
	case TypeEvictionNotice:
		out = &EvictionNotice{}
	default:
		return nil, fmt.Errorf("poolproto: unknown message type %q", ev.Type)
	}
	if err := json.Unmarshal(ev.Payload, out); err != nil {
		return nil, fmt.Errorf("poolproto: decode %s: %w", ev.Type, err)
	}
	return out, nil
}
