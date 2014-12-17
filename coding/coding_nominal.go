package coding

import (
	"database/sql"
)

type Scale struct {
	Id    Id
	Label string
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
