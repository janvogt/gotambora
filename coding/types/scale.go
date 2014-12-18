package types

import (
	"database/sql"
)

type ScaleType uint

const (
	ScaleInterval ScaleType = iota
	ScaleOrdinal  ScaleType = iota
	ScaleNominal  ScaleType = iota
)

type Scale struct {
	Id    Id
	Label Label
	Type  ScaleType
}

type Nominal struct {
	Scale
	Values map[Id]bool
}

type Ordinal struct {
	Scale
	Values []Id
}

type Interval struct {
	Scale
	Unit string
	Min  sql.NullFloat64
	Max  sql.NullFloat64
}
