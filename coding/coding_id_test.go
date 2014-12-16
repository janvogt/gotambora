package coding

import (
	"testing"
)

func TestAsString(t *testing.T) {
	tests := []struct {
		id  Id
		str string
	}{
		{0, "0"},
		{12, "12"},
		{17, "17"},
	}
	for _, test := range tests {
		if res := test.id.AsString(); res != test.str {
			t.Errorf("Expected Id(%d).AsString to be \"%s\" but got \"%s\"", test.id, test.str, res)
		}
	}
}

func TestFromString(t *testing.T) {
	tests := []struct {
		str string
		id  Id
		err bool
	}{
		{"1", 1, false},
		{"12", 12, false},
		{"0", 0, false},
		{"83k", 0, true},
		{"0x2", 0, true},
	}
	for _, test := range tests {
		if id, err := IdFromString(test.str); test.id != id || (err != nil) != test.err {
			t.Errorf("Expected id, err := IdFromString(\"%s\") to be id = Id(%d) and err != nil to be %t, but got id = Id(%d) and err = %v", test.str, test.id, test.err, id, err)
		}
	}
}
