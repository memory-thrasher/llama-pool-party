package poolproto

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestExamplesDecode decodes every golden example (../../examples/*.json)
// into its typed struct and round-trips it, catching struct/schema drift:
// a json tag that stops matching the schema property name drops the field on
// decode and the round-trip comparison fails.
func TestExamplesDecode(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "examples", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no example files found")
	}
	for _, path := range paths {
		stem := strings.TrimSuffix(filepath.Base(path), ".json")
		t.Run(stem, func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			expected := expectedTypeFor(stem)
			if expected == "" {
				t.Fatalf("no known message type for example %q", stem)
			}
			if expected == "job_intake" {
				var ji JobIntake
				if err := json.Unmarshal(raw, &ji); err != nil {
					t.Fatalf("decode: %v", err)
				}
				out, err := json.Marshal(&ji)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(canonical(t, out), canonical(t, raw)) {
					t.Errorf("round-trip mismatch:\ngot  %s\nwant %s", out, raw)
				}
				return
			}
			if expected == "class_advertisement" {
				var ca ClassAdvertisement
				if err := json.Unmarshal(raw, &ca); err != nil {
					t.Fatalf("decode: %v", err)
				}
				out, err := json.Marshal(&ca)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(canonical(t, out), canonical(t, raw)) {
					t.Errorf("round-trip mismatch:\ngot  %s\nwant %s", out, raw)
				}
				return
			}
			var ev Event
			if err := json.Unmarshal(raw, &ev); err != nil {
				t.Fatalf("decode envelope: %v", err)
			}
			if ev.Type != expected {
				t.Fatalf("envelope type = %q, want %q", ev.Type, expected)
			}
			decoded, err := ev.Decode()
			if err != nil {
				t.Fatalf("decode payload: %v", err)
			}
			out, err := json.Marshal(decoded)
			if err != nil {
				t.Fatal(err)
			}
			var envelope struct {
				Type    string          `json:"type"`
				Payload json.RawMessage `json:"payload"`
			}
			if err := json.Unmarshal(raw, &envelope); err != nil {
				t.Fatalf("extract payload: %v", err)
			}
			if !bytes.Equal(canonical(t, out), canonical(t, envelope.Payload)) {
				t.Errorf("round-trip mismatch:\ngot  %s\nwant %s", out, envelope.Payload)
			}
		})
	}
}

// TestDecodeUnknownType ensures Event.Decode rejects types outside the closed
// v1 set.
func TestDecodeUnknownType(t *testing.T) {
	ev := Event{Type: "batch_offer", Payload: json.RawMessage(`{}`)}
	if _, err := ev.Decode(); err == nil {
		t.Fatal("expected error for unknown type, got nil")
	}
}

func expectedTypeFor(stem string) string {
	if stem == "job_intake" {
		return "job_intake"
	}
	if stem == "class_advertisement" {
		return "class_advertisement"
	}
	types := []string{
		TypeHandshake, TypeCapacity, TypeWorkOffer, TypeOfferAccept,
		TypeOfferReject, TypeBodyAbort, TypeCompletion, TypeError,
		TypeAffinityReport, TypeExclusionUpdate, TypeEvictionNotice,
	}
	for _, ty := range types {
		if stem == ty {
			return ty
		}
	}
	// "<type>_<variant>.json" documents <type>; no v1 type name is a prefix
	// of another, so at most one matches.
	var match string
	for _, ty := range types {
		if strings.HasPrefix(stem, ty+"_") {
			if match != "" {
				return ""
			}
			match = ty
		}
	}
	return match
}

// canonical re-serializes raw JSON with sorted keys so key order does not
// affect comparisons (maps marshal with sorted keys in encoding/json).
func canonical(t *testing.T, raw []byte) []byte {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return out
}
