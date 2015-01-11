package types

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestUnmarshalJSONScale(t *testing.T) {
	tests := []struct {
		j []byte
		s Scale
	}{
		{[]byte(`{"id":12,"label":"scale","type":"interval","unit":"˚C","min":-273.15,"max":null}`), Scale{12, "scale", "interval", &Unit{"˚C", JsonNullFloat64{-273.15, true}, JsonNullFloat64{0, false}}, nil}},
		{[]byte(`{"id":12,"label":"scale","type":"ordinal","values":[{"id":2,"label":"No1"},{"id":5,"label":"No2"},{"label":"New"}]}`), Scale{12, "scale", "ordinal", nil, Values{Value{2, "No1"}, Value{5, "No2"}, Value{Label: "New"}}}},
	}
	for i, test := range tests {
		s := Scale{}
		e := json.Unmarshal(test.j, &s)
		if e != nil {
			t.Errorf("Testcase %d: Unexpected Error: %s\n", i, e)
		} else if !reflect.DeepEqual(s, test.s) {
			t.Errorf("Testcase %d: Unexpected Result:\n%+v\nexpected:\n%+v\n", i, s, test.s)
		}
	}
}

func TestMarshalJSONScale(t *testing.T) {
	tests := []struct {
		j []byte
		s Scale
	}{
		{[]byte(`{"id":12,"label":"scale","type":"interval","unit":"˚C","min":-273.15,"max":null}`), Scale{12, "scale", "interval", &Unit{"˚C", JsonNullFloat64{-273.15, true}, JsonNullFloat64{0, false}}, nil}},
		{[]byte(`{"id":12,"label":"scale","type":"ordinal","values":[{"id":2,"label":"No1"},{"id":5,"label":"No2"},{"id":0,"label":"New"}]}`), Scale{12, "scale", "ordinal", nil, Values{Value{2, "No1"}, Value{5, "No2"}, Value{Label: "New"}}}},
	}
	for i, test := range tests {
		j, e := json.Marshal(test.s)
		if e != nil {
			t.Errorf("Testcase %d: Unexpected Error: %s\n", i, e)
		} else if string(j) != string(test.j) {
			t.Errorf("Testcase %d: Unexpected Result:\n%s\nexpected:\n%s\n", i, j, test.j)
		}
	}
}

func ptrToFloat(f float64) *float64 {
	return &f
}
