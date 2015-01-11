package types

import (
	"encoding/json"
)

type Links struct {
	Links map[string]Relation `json:"links"`
}

type Relation struct {
	ToOne  *Id
	ToMany []Id
}

func (r Relation) MarshalJSON() ([]byte, error) {
	if r.ToOne != nil {
		return json.Marshal(r.ToOne)
	}
	return json.Marshal(r.ToMany)
}

func (r *Relation) UnmarshalJSON(data []byte) error {
	if data[0] != '[' {
		return json.Unmarshal(data, &r.ToOne)
	}
	return json.Unmarshal(data, &r.ToMany)
}
