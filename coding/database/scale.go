package database

import (
	"fmt"
	"github.com/janvogt/gotambora/coding/types"
	"github.com/jmoiron/sqlx"
	"net/http"
)

type ScaleController struct {
	db *DB
}

func (db *DB) ScaleController() types.ResourceController {
	return &ScaleController{db}
}

// New satisfies the types.Controller interface
func (s *ScaleController) New() (r types.Resource) {
	return new(types.Scale)
}

// Query satisfies the types.Controller interface
func (s *ScaleController) Query(q map[string][]string) types.ResourceReader {
	reader := new(ScaleReader)
	reader.rows, reader.err = s.db.Queryx(`SELECT s.id, s.label, s.type, json_agg((v.id, v.label)::` + s.db.prefix + `_scale_value ORDER BY v.index) AS values, COALESCE(u.unit, '') AS unit, u.min, u.max FROM ` + s.db.prefix + `_scales s LEFT JOIN ` + s.db.prefix + `_values v ON s.id = v.scale LEFT JOIN ` + s.db.prefix + `_units u ON s.id = u.scale GROUP BY s.id, s.label, s.type, u.unit, u.min, u.max`)
	return reader
}

// Create satisfies the types.Controller interface
func (s *ScaleController) Create(r types.Resource) (err error) {
	scale, err := assertScale(r)
	if err != nil {
		return
	}
	args := make(map[string]interface{})
	q := "WITH"
	switch scale.Type {
	case types.ScaleNominal, types.ScaleOrdinal:
		q += newValues(scale, args) + `
SELECT s.id, s.label, s.type, json_agg((v.id, v.label)::::%[1]s_scale_value ORDER BY v."index") AS values
  FROM new_scale s
  LEFT JOIN new_values v ON s.id = v.scale
GROUP BY s.id, s.label, s.type`
	case types.ScaleInterval:
		q += newUnit(scale, args) + `
SELECT s.id, s.label, s.type, u.unit, u.min, u.max
  FROM new_scale s
  LEFT JOIN new_units u ON s.id = u.scale`
	default:
		err = fmt.Errorf("Invalid scale type for new scale!")
		return
	}
	stmt, err := s.db.PrepareNamed(fmt.Sprintf(q, s.db.prefix))
	if err != nil {
		return
	}
	row := stmt.QueryRowx(args)
	err = row.StructScan(scale)
	if err != nil {
		return
	}
	err = cleanupScale(scale)
	return
}

func newScale(s *types.Scale, args map[string]interface{}) string {
	args["newScaleLabel"], args["newScaleType"] = s.Label, s.Type
	return ` new_scale AS ( INSERT INTO %[1]s_scales (label, type) VALUES (:newScaleLabel, :newScaleType) RETURNING * )`
}

func newValues(s *types.Scale, args map[string]interface{}) (q string) {
	q = newScale(s, args) + ","
	if s.Values == nil || len(s.Values) == 0 {
		q += ` new_values AS ( SELECT * FROM %[1]s_values WHERE FALSE)`
		return
	}
	vs := ""
	for i, v := range s.Values {
		lab, ind := fmt.Sprintf("newValuesLabel%d", i), fmt.Sprintf("newValuesIndex%d", i)
		args[lab], args[ind] = v.Label, i
		vs += fmt.Sprintf(",(:%s, :%s)", lab, ind)
	}
	q += ` new_scale_values AS ( VALUES ` + vs[1:] + ` ), new_values AS ( INSERT INTO %[1]s_values ("index", label, scale) SELECT v.index::::bigint, v.label, s.id FROM new_scale s, new_scale_values AS v (label, "index") RETURNING * )`
	return
}

func newUnit(s *types.Scale, args map[string]interface{}) (q string) {
	if s.UnitDesc == nil {
		s.UnitDesc = new(types.UnitDesc)
	}
	q = newScale(s, args) + ","
	args["newUnitUnit"], args["newUnitMin"], args["newUnitMax"] = s.Unit, s.Min, s.Max
	q += ` new_units AS ( INSERT INTO %[1]s_units (scale, unit, "min", "max") SELECT s.id, v.unit, v.min::::double precision, v.max::::double precision FROM new_scale s, (VALUES (:newUnitUnit, :newUnitMin, :newUnitMax)) AS v (unit, "min", "max") RETURNING * )`
	return
}

// Read satisfies the types.Controller interface
func (s *ScaleController) Read(id types.Id) (r types.Resource, err error) {
	stmt, err := s.db.Preparex(`SELECT s.id, s.label, s.type, json_agg((v.id, v.label)::` + s.db.prefix + `_scale_value ORDER BY v.index) AS values, COALESCE(u.unit, '') AS unit, u.min, u.max FROM ` + s.db.prefix + `_scales s LEFT JOIN ` + s.db.prefix + `_values v ON s.id = v.scale LEFT JOIN ` + s.db.prefix + `_units u ON s.id = u.scale WHERE s.id = $1 GROUP BY s.id, s.label, s.type, u.unit, u.min, u.max`)
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
	q := "WITH"
	args := make(map[string]interface{})
	switch scale.Type {
	case types.ScaleNominal, types.ScaleOrdinal:
		q += changedValues(scale, args) + `
SELECT s.id, s.label, s.type, json_agg((v.id, v.label)::::%[1]s_scale_value ORDER BY v."index") AS values
  FROM updated_scale s
  LEFT JOIN changed_values v ON s.id = v.scale
GROUP BY s.id, s.label, s.type`
	case types.ScaleInterval:
		q += updatedUnit(scale, args) + `
SELECT s.id, s.label, s.type, u.unit, u.min, u.max
  FROM updated_scale s
  LEFT JOIN updated_unit u ON s.id = u.scale`
	default:
		err = fmt.Errorf("Invalid scale type for new scale!")
		return
	}
	stmt, err := s.db.PrepareNamed(fmt.Sprintf(q, s.db.prefix))
	if err != nil {
		return
	}
	row := stmt.QueryRowx(args)
	err = row.StructScan(scale)
	if err != nil {
		return
	}
	err = cleanupScale(scale)
	return
}

func updatedScale(s *types.Scale, args map[string]interface{}) string {
	args["updatedScaleLabel"], args["updatedScaleId"] = s.Label, s.Id
	return ` updated_scale AS ( UPDATE %[1]s_scales SET label = :updatedScaleLabel WHERE id = :updatedScaleId RETURNING * )`
}

func changedValues(s *types.Scale, args map[string]interface{}) (q string) {
	q = updatedScale(s, args)
	del := `, deleted AS ( DELETE FROM %[1]s_values v USING updated_scale s WHERE v.scale = s.id AND v.id NOT IN ( SELECT id FROM changed_values ) )`
	if s.Values == nil || len(s.Values) == 0 {
		q += `, changed_values AS ( SELECT * FROM %[1]s_values WHERE FALSE )` + del
		return
	}
	uV, cV := "", ""
	for i, v := range s.Values {
		if v.Id != 0 {
			id, lab, ind := fmt.Sprintf("changesValuesId%d", i), fmt.Sprintf("changesValuesLabel%d", i), fmt.Sprintf("changesValuesIndex%d", i)
			args[id], args[lab], args[ind] = v.Id, v.Label, i
			uV += fmt.Sprintf(",(:%s, :%s, :%s)", id, lab, ind)
		} else {
			lab, ind := fmt.Sprintf("changesValuesLabel%d", i), fmt.Sprintf("changesValuesIndex%d", i)
			args[lab], args[ind] = v.Label, i
			cV += fmt.Sprintf(",(:%s, :%s)", lab, ind)
		}
	}
	if len(uV) > 0 {
		q += `, updated_values AS ( UPDATE %[1]s_values v SET label = n.label, "index" = n.index::::bigint FROM updated_scale s, ( VALUES ` + uV[1:] + ` ) AS n (id, label, "index") WHERE v.id = n.id::::bigint AND v.scale = s.id RETURNING v.* )`
	} else {
		q += `, updated_values AS ( SELECT * FROM %[1]s_values WHERE FALSE )`
	}
	if len(cV) > 0 {
		q += `, new_values AS ( INSERT INTO %[1]s_values (label, scale, "index") SELECT v.label, s.id, v.index::::bigint FROM updated_scale s, ( VALUES ` + cV[1:] + ` ) AS v (label, "index") RETURNING * )`
	} else {
		q += `, new_values AS ( SELECT * FROM %[1]s_values WHERE FALSE )`
	}
	q += `, changed_values AS ( SELECT * FROM new_values UNION SELECT * FROM updated_values )` + del
	return
}

func updatedUnit(s *types.Scale, args map[string]interface{}) (q string) {
	if s.UnitDesc == nil {
		s.UnitDesc = new(types.UnitDesc)
	}
	q = updatedScale(s, args)
	args["updatedUnitUnit"], args["updatedUnitMin"], args["updatedUnitMax"] = s.Unit, s.Min, s.Max
	q += `, updated_unit AS ( UPDATE %[1]s_units SET unit = :updatedUnitUnit, min = :updatedUnitMin, max = :updatedUnitMax FROM updated_scale s WHERE scale = s.id RETURNING %[1]s_units.* )`
	return
}

// Delete satisfies the types.Controller interface
func (s *ScaleController) Delete(id types.Id) (err error) {
	res, err := s.db.Exec("DELETE FROM "+s.db.prefix+"_scales WHERE id = $1", id)
	if err != nil {
		return
	}
	n, err := res.RowsAffected()
	if err != nil {
		return
	}
	if n != 1 {
		err = types.NewHttpError(http.StatusNotFound, fmt.Errorf("No scale found with id %d", id))
	}
	return
}

type ScaleReader struct {
	err  error
	rows *sqlx.Rows
}

// Read implements the types.DocumentReader interface
func (s *ScaleReader) Read(r types.Resource) (ok bool, err error) {
	if s.err != nil {
		err = s.err
		return
	}
	scale, err := assertScale(r)
	if err != nil {
		return
	}
	if ok = s.rows.Next(); ok {
		err = s.rows.StructScan(scale)
	} else {
		s.rows.Close()
	}
	if err != nil {
		ok, s.err = false, err
	} else {
		s.err = cleanupScale(scale)
		if s.err != nil {
			ok, s.err = false, err
		}
	}
	return
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
		s.UnitDesc = nil
	case types.ScaleInterval:
		s.Values = nil
	default:
		return fmt.Errorf("Invalid scale type for scale id %d", s.Id)
	}
	return nil
}
