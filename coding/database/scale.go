package database

import (
	"fmt"
	"github.com/janvogt/gotambora/coding/types"
	"github.com/jmoiron/sqlx"
)

type ScaleController struct {
	db *DB
}

// New satisfies the types.Controller interface
func (s *ScaleController) New() (r types.Resource) {
	return new(types.Scale)
}

// Query satisfies the types.Controller interface
func (s *ScaleController) Query(q map[string][]string) types.ResourceReader {
	return nil
}

// Create satisfies the types.Controller interface
func (s *ScaleController) Create(r types.Resource) (err error) {
	scale, err := assertScale(r)
	if err != nil {
		return
	}
	var q string
	args := make([]interface{}, 2)
	args[0], args[1] = scale.Label, scale.Type
	switch scale.Type {
	case types.ScaleNominal, types.ScaleOrdinal:
		vals := ""
		for i, v := range scale.Values {
			c := len(args)
			vals += fmt.Sprintf(",($%d, $%d)", c+1, c+2)
			args = append(args, i, v.Label)
		}
		q = `
WITH new_scale AS (
  INSERT INTO %[1]s_scales (label, type) VALUES ($1, $2) RETURNING *
),
new_scale_values AS (
  VALUES
   ` + vals[1:] + `
),
new_values AS (
  INSERT INTO %[1]s_values (label, scale, "index") SELECT v.column1, s.id, v.column2 FROM new_scale s, new_scale_values v RETURNING *
)
SELECT s.id, s.label, s.type, json_agg((v.id, v.label)::%[1]s_scale_value ORDER BY v."index") AS values
  FROM new_scale s
  LEFT JOIN new_values v ON s.id = v.scale
GROUP BY s.id, s.label, s.type`
	case types.ScaleInterval:
		args = append(args, scale.Unit.Unit, scale.Min, scale.Max)
		q = `
WITH new_scale AS (
  INSERT INTO %[1]s_scales (label, type) VALUES ($1, $2) RETURNING *
),
new_scale_units AS (
  VALUES
	($3, $4, $5)
),
new_units AS (
  INSERT INTO %[1]s_units (scale, unit, "min", "max") SELECT s.id, v.column1, v.column2, v.column3 FROM new_scale s, new_scale_units v RETURNING *
)
SELECT s.id, s.label, s.type, u.unit, u.min, u.max
  FROM new_scale s
  LEFT JOIN new_units u ON s.id = u.scale`
	default:
		err = fmt.Errorf("Invalid scale type for new scale!")
		return
	}
	stmt, err := s.db.Preparex(fmt.Sprintf(q, s.db.prefix))
	if err != nil {
		return
	}
	row := stmt.QueryRowx(args...)
	err = row.StructScan(scale)
	if err != nil {
		return
	}
	switch scale.Type {
	case types.ScaleNominal, types.ScaleOrdinal:
		scale.Unit = nil
	case types.ScaleInterval:
		scale.Values = nil
	default:
		err = fmt.Errorf("Invalid scale type for scale id %d", scale.Id)
		return
	}
	return nil
}

// Read satisfies the types.Controller interface
func (s *ScaleController) Read(id types.Id) (r types.Resource, err error) {
	stmt, err := s.db.Preparex(`SELECT s.id, s.label, s.type, json_agg((v.id, v.label)::` + s.db.prefix + `_scale_value ORDER BY v.index) AS values, COALESCE(u.unit, ""), u.min, u.max FROM ` + s.db.prefix + `_scales s LEFT JOIN ` + s.db.prefix + `_values v ON s.id = v.scale LEFT JOIN ` + s.db.prefix + `_units u ON s.id = u.scale WHERE s.id = $1 GROUP BY s.id, s.label, s.type, u.unit, u.min, u.max`)
	if err != nil {
		return
	}
	row := stmt.QueryRowx(id)
	scale := &types.Scale{}
	err = row.StructScan(scale)
	if err != nil {
		return
	}
	switch scale.Type {
	case types.ScaleNominal, types.ScaleOrdinal:
		scale.Unit = nil
	case types.ScaleInterval:
		scale.Values = nil
	default:
		err = fmt.Errorf("Invalid scale type for scale id %d", id)
		return
	}
	r = scale
	return
}

// Update satisfies the types.Controller interface
func (s *ScaleController) Update(r types.Resource) (err error) {
	return nil
}

// Delete satisfies the types.Controller interface
func (s *ScaleController) Delete(id types.Id) (err error) {
	return nil
}

type ScaleReader struct {
	err  error
	rows *sqlx.Rows
}

// Read implements the types.DocumentReader interface
func (s *ScaleReader) Read(r types.Resource) (ok bool, err error) {
	return false, nil
}

// Close implements the types.DocumentReader interface
func (s *ScaleReader) Close() error {
	return s.rows.Close()
}

func assertScale(r types.Resource) (s *types.Scale, err error) {
	switch scale := r.(type) {
	case *types.Scale:
		s = scale
	default:
		err = fmt.Errorf("Unsuported Resource type, expected *Scale.")
	}
	return
}
