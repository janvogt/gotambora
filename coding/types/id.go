package types

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strconv"
)

// Id is used to identifiy resurces.
type Id uint64

// AsString returns the decimal string representation of Id.
func (id *Id) AsString() (as string) {
	return strconv.FormatUint(uint64(*id), 10)
}

// FromString created an Id from the given string. An error is returned if parsing fails.
func IdFromString(from string) (id Id, parse error) {
	uintId, parse := strconv.ParseUint(from, 10, 64)
	id = Id(uintId)
	return
}

type OptionalId struct {
	Id    Id
	Valid bool
}

func (oid *OptionalId) Scan(src interface{}) error {
	if src == nil {
		oid.Id, oid.Valid = Id(0), false
		return nil
	}
	switch src := src.(type) {
	case int64:
		oid.Id, oid.Valid = Id(src), true
		return nil
	}
	return fmt.Errorf("Unsuported Typte %T for coding.Label", src)
}

func (oid OptionalId) Value() (driver.Value, error) {
	if oid.Valid {
		return driver.Value(oid.Id), nil
	}
	return nil, nil
}

func (oid OptionalId) MarshalJSON() ([]byte, error) {
	if !oid.Valid {
		return json.Marshal(nil)
	}
	return json.Marshal(oid.Id)
}

func (oid *OptionalId) UnmarshalJSON(data []byte) (err error) {
	if string(data) == "null" {
		oid.Id, oid.Valid = Id(0), false
		return nil
	}
	err = json.Unmarshal(data, &oid.Id)
	if err == nil {
		oid.Valid = true
	}
	return
}
