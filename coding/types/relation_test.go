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
		{Relation{kind: toOne, ToOne: Id(3)}, []byte("3")},
		{Relation{kind: toOne, ToOne: Id(0)}, []byte("0")},
		{Relation{}, []byte("null")},
		{Relation{kind: toMany, ToMany: []Id{}}, []byte("[]")},
		{Relation{kind: toMany, ToMany: []Id{Id(0)}}, []byte("[0]")},
		{Relation{kind: toMany, ToMany: []Id{Id(5)}}, []byte("[5]")},
		{Relation{kind: toMany, ToMany: []Id{Id(5), Id(0), Id(2)}}, []byte("[5,0,2]")},
	}
	for i, test := range tests {
		t.Log(test.r)
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
		{Relation{kind: toOne, ToOne: Id(3)}, []byte("3")},
		{Relation{kind: toOne, ToOne: Id(0)}, []byte("0")},
		{Relation{}, []byte("null")},
		{Relation{kind: toMany, ToMany: []Id{}}, []byte("[]")},
		{Relation{kind: toMany, ToMany: []Id{Id(0)}}, []byte("[0]")},
		{Relation{kind: toMany, ToMany: []Id{Id(5)}}, []byte("[5]")},
		{Relation{kind: toMany, ToMany: []Id{Id(5), Id(0), Id(2)}}, []byte("[5,0,2]")},
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
