package types

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
)

type ScaleType string

const (
	ScaleInterval ScaleType = "interval"
	ScaleOrdinal  ScaleType = "ordinal"
	ScaleNominal  ScaleType = "nominal"
)

type Scale struct {
	Id    Id        `json:"id"`
	Label Label     `json:"label"`
	Type  ScaleType `json:"type"`
	*UnitDesc
	Values Values `json:"values"`
}

type UnitDesc struct {
	Unit Label           `json:"unit"`
	Min  JsonNullFloat64 `json:"min"`
	Max  JsonNullFloat64 `json:"max"`
}

type Value struct {
	Id    Id    `json:"id"`
	Label Label `json:"label"`
}

type Values []Value

// SetId implements the Document interface.
func (s *Scale) SetId(id Id) {
	s.Id = id
}

func (v *Values) Scan(src interface{}) error {
	var j []byte
	switch src := src.(type) {
	case []byte:
		j = src
	case string:
		j = []byte(src)
	default:
		return fmt.Errorf("Unsuported Typte %T for coding.ValueSlice", src)
	}
	if strings.Contains(string(j), `"id":null`) {
		*v = make([]Value, 0)
		return nil
	}
	return json.Unmarshal(j, v)
}

func (v *ScaleType) Scan(src interface{}) error {
	switch src := src.(type) {
	case []byte:
		*v = ScaleType(src)
	case string:
		*v = ScaleType(src)
	default:
		return fmt.Errorf("Unsuported Typte %T for coding.ScaleType", src)
	}
	return nil
}

func (t ScaleType) Value() (driver.Value, error) {
	return driver.Value(string(t)), nil
}
