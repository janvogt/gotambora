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
	err = cleanupScale(scale)
	return
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
	err = cleanupScale(scale)
	if err != nil {
		return
	}
	r = scale
	return
}

// Update satisfies the types.Controller interface
func (s *ScaleController) Update(r types.Resource) (err error) {
	scale, err := assertScale(r)
	if err != nil {
		return
	}
	qValue1 := `
WITH updated_scale AS (
  UPDATE %[1]s_scales SET label = $1 WHERE id = $2 RETURNING *
),
update_scale_values AS (
  VALUES
`
	qValue2 := `
),
updated_values AS (
  UPDATE %[1]s_values SET label = column2, "index" = column3 FROM update_scale_values WHERE id = column1 RETURNING %[1]s_values.*
),
new_scale_values AS (
  VALUES
`
	qValue3 := `
),
new_values AS (
  INSERT INTO %[1]s_values (label, scale, "index") SELECT v.column1, s.id, v.column2 FROM updated_scale s, new_scale_values v RETURNING *
),
changed_values AS (
  SELECT * FROM new_values UNION SELECT * FROM updated_values
),
all_values AS (
  SELECT * FROM changed_values UNION SELECT v.* FROM updated_scale s, %[1]s_values v WHERE s.id = v.scale AND v.id NOT IN ( SELECT id FROM changed_values )
)
SELECT s.id, s.label, s.type, json_agg((v.id, v.label)::%[1]s_scale_value ORDER BY v."index") AS values
  FROM updated_scale s
  LEFT JOIN all_values v ON s.id = v.scale
GROUP BY s.id, s.label, s.type`
	qUnit := `
WITH updated_scale AS (
  UPDATE %[1]s_scales SET label = :label WHERE id = :id RETURNING *
),
update_unit AS (
  UPDATE %[1]s_units SET unit = :unit, min = :min, max = :max FROM updated_scale WHERE scale = id RETURNING %[1]s_units.*
)
SELECT s.id, s.label, s.type, u.unit, u.min, u.max
  FROM updated_scale s
  LEFT JOIN update_unit u ON s.id = u.scale`
	var row *sqlx.Row
	args := make([]interface{}, 2)
	args[0], args[1] = scale.Label, scale.Id
	switch scale.Type {
	case types.ScaleNominal, types.ScaleOrdinal:
		toUpdate := make([]interface{}, 0)
		toCreate := make([]interface{}, 0)
		for i, v := range scale.Values {
			if v.Id == 0 {
				toCreate = append(toCreate, v.Label, i)
			} else {
				toUpdate = append(toUpdate, v.Id, v.Label, i)
			}
		}
		cA, cU, cC := len(args), len(toUpdate), len(toCreate)
		newArgs := make([]interface{}, cA+cU+cC)
		copy(newArgs[:cA], args)
		copy(newArgs[cA:cA+cU], toUpdate)
		copy(newArgs[cA+cU:cA+cU+cC], toCreate)
		args = newArgs
		var qU, qC string
		for i := 0; i < cU/3; i++ {
			qU += fmt.Sprintf(",($%d, $%d, $%d)", cA+i*3+1, cA+i*3+2, cA+i*3+3)
		}
		for i := 0; i < cC/2; i++ {
			qC += fmt.Sprintf(",($%d, $%d)", cA+cU+i*2+1, cA+cU+i*2+2)
		}
		q := qValue1 + qU[1:] + qValue2 + qC[1:] + qValue3
		var stmt *sqlx.Stmt
		stmt, err = s.db.Preparex(fmt.Sprintf(q, s.db.prefix))
		if err != nil {
			return
		}
		row = stmt.QueryRowx(args...)
	case types.ScaleInterval:
		var stmt *sqlx.NamedStmt
		stmt, err = s.db.PrepareNamed(fmt.Sprintf(qUnit, s.db.prefix))
		if err != nil {
			return
		}
		row = stmt.QueryRowx(scale)
	default:
		err = fmt.Errorf("Invalid scale type for new scale!")
		return
	}
	err = row.StructScan(scale)
	if err != nil {
		return
	}
	err = cleanupScale(scale)
	return
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

// cleanupScale nils out the unnecessary part of Scale according to it's type
func cleanupScale(s *types.Scale) error {
	switch s.Type {
	case types.ScaleNominal, types.ScaleOrdinal:
		s.Unit = nil
	case types.ScaleInterval:
		s.Values = nil
	default:
		return fmt.Errorf("Invalid scale type for scale id %d", s.Id)
	}
	return nil
}
