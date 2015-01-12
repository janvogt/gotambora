package types

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestMetricMarshalJSON(t *testing.T) {
	tests := []struct {
		m *Metric
		j string
	}{
		{&Metric{4, "metric", RelationToMany{Id(1), Id(4), Id(5)}}, `{"id":4,"label":"metric","links":{"scales":[1,4,5]}}`},
		{&Metric{4, "metric", RelationToMany{}}, `{"id":4,"label":"metric","links":{"scales":[]}}`},
	}
	for i, test := range tests {
		j, err := json.Marshal(test.m)
		if err != nil {
			t.Errorf("Testcase %d: Unexpected Error: %s", i, err)
		} else if string(j) != test.j {
			t.Errorf("Testcase %d: Unexpected result:\n%s\n expected:\n%s\n", i, j, test.j)
		}
	}
}

func TestMetricUnmarshalJSON(t *testing.T) {
	tests := []struct {
		m *Metric
		j string
	}{
		{&Metric{4, "metric", RelationToMany{Id(1), Id(4), Id(5)}}, `{"id":4,"label":"metric","links":{"scales":[1,4,5]}}`},
		{&Metric{4, "metric", RelationToMany{}}, `{"id":4,"label":"metric","links":{"scales":[]}}`},
		{&Metric{0, "", RelationToMany{}}, `{}`},
	}
	for i, test := range tests {
		m := new(Metric)
		err := json.Unmarshal([]byte(test.j), m)
		if err != nil {
			t.Errorf("Testcase %d: Unexpected Error: %s", i, err)
		} else if !reflect.DeepEqual(m, test.m) {
			t.Errorf("Testcase %d: Unexpected result:\n%+v\n expected:\n%+v\n", i, m, test.m)
		}
	}
}
