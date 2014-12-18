package types

import (
	"database/sql/driver"
	"fmt"
)

type Label string

func LabelFromString(src string) (l *Label) {
	lab := Label(src)
	return &lab
}

func (l *Label) Scan(src interface{}) error {
	switch src := src.(type) {
	case string:
		*l = Label(src)
		return nil
	case []byte:
		*l = Label(src)
		return nil
	}
	return fmt.Errorf("Unsuported Typte %T for coding.Label", src)
}

func (l Label) Value() (driver.Value, error) {
	return driver.Value(string(l)), nil
}
