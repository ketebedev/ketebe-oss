package ketebe

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

type RecordIDKind string

const (
	RecordIDString RecordIDKind = "string"
	RecordIDU64    RecordIDKind = "u64"
)

type RecordID struct {
	Kind  RecordIDKind
	Text  string
	Value uint64
}

func StringID(value string) RecordID { return RecordID{Kind: RecordIDString, Text: value} }
func U64ID(value uint64) RecordID    { return RecordID{Kind: RecordIDU64, Value: value} }

func (id RecordID) PathValue() string {
	if id.Kind == RecordIDU64 {
		return strconv.FormatUint(id.Value, 10)
	}
	return id.Text
}

func (id RecordID) MarshalJSON() ([]byte, error) {
	switch id.Kind {
	case RecordIDString:
		if id.Text == "" {
			return nil, errors.New("string RecordID must not be empty")
		}
		return json.Marshal(struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		}{"string", id.Text})
	case RecordIDU64:
		return []byte(fmt.Sprintf(`{"type":"u64","value":%d}`, id.Value)), nil
	default:
		return nil, fmt.Errorf("unsupported RecordID kind %q", id.Kind)
	}
}

func (id *RecordID) UnmarshalJSON(data []byte) error {
	var envelope struct {
		Type  string          `json:"type"`
		Value json.RawMessage `json:"value"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return err
	}
	switch envelope.Type {
	case "string":
		var value string
		if err := json.Unmarshal(envelope.Value, &value); err != nil {
			return err
		}
		if value == "" {
			return errors.New("string RecordID must not be empty")
		}
		*id = StringID(value)
		return nil
	case "u64":
		dec := json.NewDecoder(bytes.NewReader(envelope.Value))
		dec.UseNumber()
		var n json.Number
		if err := dec.Decode(&n); err != nil {
			return err
		}
		value, err := strconv.ParseUint(n.String(), 10, 64)
		if err != nil {
			return fmt.Errorf("invalid u64 RecordID: %w", err)
		}
		*id = U64ID(value)
		return nil
	default:
		return fmt.Errorf("unsupported RecordID type %q", envelope.Type)
	}
}
