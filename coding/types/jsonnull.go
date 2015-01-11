package types

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
)

type JsonNullFloat64 sql.NullFloat64

func (f JsonNullFloat64) MarshalJSON() ([]byte, error) {
	if f.Valid {
		return json.Marshal(f.Float64)
	} else {
		return []byte("null"), nil
	}
}

func (f *JsonNullFloat64) UnmarshalJSON(d []byte) error {
	if string(d) == "null" {
		f.Float64, f.Valid = 0, false
		return nil
	} else {
		f.Valid = true
		return json.Unmarshal(d, &f.Float64)
	}
}

func (j *JsonNullFloat64) Scan(src interface{}) error {
	return (*sql.NullFloat64)(j).Scan(src)
}

func (j JsonNullFloat64) Value() (driver.Value, error) {
	return sql.NullFloat64(j).Value()
}
