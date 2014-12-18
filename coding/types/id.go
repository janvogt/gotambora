package types

import (
	"strconv"
)

// Id is used to identifiy resurces.
type Id uint64

// AsString returns the decimal string representation of Id.
func (id *Id) AsString() (as string) {
	return strconv.FormatUint(uint64(*id), 10)
}

// FromString created an Id from the given string. An error is returned if parsing fails.
func IdFromString(from string) (id Id, parse error) {
	uintId, parse := strconv.ParseUint(from, 10, 64)
	id = Id(uintId)
	return
}
