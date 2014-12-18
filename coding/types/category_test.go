package types

import (
	"reflect"
	"testing"
)

func TestMarshalJSON(t *testing.T) {
	tests := []struct {
		category *Category
		json     []byte
	}{
		{&Category{2, "Test", []Id{1, 2, 3}}, []byte(`{"id":2,"label":"Test","links":{"nodes":[1,2,3]}}`)},
		{&Category{0, "", []Id{}}, []byte(`{"id":0,"label":"","links":{"nodes":[]}}`)},
		{&Category{}, []byte(`{"id":0,"label":"","links":{"nodes":[]}}`)},
		{nil, []byte(`null`)},
	}
	for i, test := range tests {
		json, err := test.category.MarshalJSON()
		if err != nil {
			t.Errorf("Test Case %d: Unexpected Error when testing Category.MarshalJSON: %s", i, err)
		}
		if string(json) != string(test.json) {
			t.Errorf("Test Case %d: Expected %#v to be marshaled to %s, but got %s", i, test.category, test.json, json)
		}
	}
}

func TestUnmarshalJSON(t *testing.T) {
	tests := []struct {
		category *Category
		json     []byte
	}{
		{&Category{2, "Test", []Id{1, 2, 3}}, []byte(`{"id":2,"label":"Test","links":{"nodes":[1,2,3]}}`)},
		{&Category{0, "", []Id{}}, []byte(`{"id":0,"label":"","links":{"nodes":[]}}`)},
		{&Category{}, []byte(`{"id":0,"label":"","links":{"nodes":null}}`)},
		{&Category{}, []byte(`null`)},
	}
	for i, test := range tests {
		category := &Category{}
		err := category.UnmarshalJSON(test.json)
		if err != nil {
			t.Errorf("Test Case %d: Unexpected Error when testing Category.UnarshalJSON: %s", i, err)
		}
		if !reflect.DeepEqual(category, test.category) {
			t.Errorf("Test Case %d: Expected %s to be marshaled to %#v, but got %#v", i, test.json, test.category, category)
		}
	}
}
