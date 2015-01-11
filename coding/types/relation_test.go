package types

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestMarshalJSON(t *testing.T) {
	tests := []struct {
		r Relation
		j []byte
	}{
		{Relation{ToOne: ptrId(Id(3))}, []byte("3")},
		{Relation{ToOne: ptrId(Id(0))}, []byte("0")},
		{Relation{}, []byte("null")},
		{Relation{ToMany: []Id{}}, []byte("[]")},
		{Relation{ToMany: []Id{Id(0)}}, []byte("[0]")},
		{Relation{ToMany: []Id{Id(5)}}, []byte("[5]")},
		{Relation{ToMany: []Id{Id(5), Id(0), Id(2)}}, []byte("[5,0,2]")},
	}
	p, q := json.Marshal(Relation{ToOne: ptrId(Id(3))})
	t.Logf("%s, %s\n", p, q)
	for i, test := range tests {
		j, e := json.Marshal(test.r)
		if e != nil {
			t.Errorf("Testcase %d: Got unexpected Error: %s\n", i, e)
		} else if !reflect.DeepEqual(j, test.j) {
			t.Errorf("Testcase %d: Got unexpected JSON:\n%s\n was expecting:\n%s\n", i, j, test.j)
		}
		j, e = json.Marshal(&test.r)
		if e != nil {
			t.Errorf("Testcase %d: Got unexpected Error: %s\n", i, e)
		} else if !reflect.DeepEqual(j, test.j) {
			t.Errorf("Testcase %d: Got unexpected JSON:\n%s\n was expecting:\n%s\n", i, j, test.j)
		}
	}
}

func TestUnmarshalJSON(t *testing.T) {
	tests := []struct {
		r Relation
		j []byte
	}{
		{Relation{ToOne: ptrId(Id(3))}, []byte("3")},
		{Relation{ToOne: ptrId(Id(0))}, []byte("0")},
		{Relation{}, []byte("null")},
		{Relation{ToMany: []Id{}}, []byte("[]")},
		{Relation{ToMany: []Id{Id(0)}}, []byte("[0]")},
		{Relation{ToMany: []Id{Id(5)}}, []byte("[5]")},
		{Relation{ToMany: []Id{Id(5), Id(0), Id(2)}}, []byte("[5,0,2]")},
	}
	for i, test := range tests {
		var r Relation
		e := json.Unmarshal(test.j, &r)
		if e != nil {
			t.Errorf("Testcase %d: Got unexpected Error: %s\n", i, e)
		} else if !reflect.DeepEqual(r, test.r) {
			t.Errorf("Testcase %d: Got unexpected Relation:\n%v\n was expecting:\n%v\n", i, r, test.r)
		}
	}
}

func ptrId(i Id) *Id {
	return &i
}
