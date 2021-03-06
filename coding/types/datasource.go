package types

import (
	"encoding/json"
	"fmt"
)

type DataSource interface {
	NodeController() ResourceController
	ScaleController() ResourceController
	MetricController() ResourceController
	// EventController() ResourceController
}

type RelationToMany []Id

func (ids *RelationToMany) Scan(src interface{}) (err error) {
	switch src := src.(type) {
	case string:
		if src == "[null]" {
			*ids = RelationToMany([]Id{})
			return
		}
		err = json.Unmarshal([]byte(src), ids)
	case []byte:
		if string(src) == "[null]" {
			*ids = RelationToMany([]Id{})
			return
		}
		err = json.Unmarshal(src, ids)
	default:
		err = fmt.Errorf("Unsuported Typte %T for coding.RelationToMany", src)
	}
	return
}
