package database

import (
	"database/sql/driver"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/janvogt/gotambora/coding/types"
	"reflect"
	"testing"
)

func TestCreateScale(t *testing.T) {
	cValue := []string{"id", "label", "type", "values"}
	cInterval := []string{"id", "label", "type", "unit", "min", "max"}
	qBeginValue := `WITH new_scale AS \( INSERT INTO prefix_scales \(label, type\) VALUES \(\$1, \$2\) RETURNING \* \), `
	qEndValue := ` SELECT s.id, s.label, s.type, json_agg\(\(v.id, v.label\)::prefix_scale_value ORDER BY v."index"\) AS values FROM new_scale s LEFT JOIN new_values v ON s.id = v.scale GROUP BY s.id, s.label, s.type`
	qUnit := `WITH new_scale AS \( INSERT INTO prefix_scales \(label, type\) VALUES \(\$1, \$2\) RETURNING \* \), new_units AS \( INSERT INTO prefix_units \(scale, unit, "min", "max"\) SELECT s.id, v.unit, v.min::double precision, v.max::double precision FROM new_scale s, \(VALUES \(\$3, \$4, \$5\)\) AS v \(unit, "min", "max"\) RETURNING \* \) SELECT s.id, s.label, s.type, u.unit, u.min, u.max FROM new_scale s LEFT JOIN new_units u ON s.id = u.scale`
	tests := []struct {
		q   string
		sn  *types.Scale
		a   []driver.Value
		r   []driver.Value
		se  *types.Scale
		col []string
	}{
		{
			qBeginValue + `new_scale_values AS \( VALUES \(\$3, \$4\),\(\$5, \$6\),\(\$7, \$8\) \), new_values AS \( INSERT INTO prefix_values \("index", label, scale\) SELECT v.index::bigint, v.label, s.id FROM new_scale s, new_scale_values AS v \(label, "index"\) RETURNING \* \)` + qEndValue,
			&types.Scale{0, "yeah", types.ScaleOrdinal, nil, types.Values{types.Value{0, "No1"}, types.Value{0, "No2"}, types.Value{0, "No3"}}},
			[]driver.Value{"yeah", "ordinal", "No1", 0, "No2", 1, "No3", 2},
			[]driver.Value{2, "yeah", "ordinal", `[{"id":1,"label":"No1"},{"id":2,"label":"No2"},{"id":3,"label":"No3"}]`},
			&types.Scale{2, "yeah", types.ScaleOrdinal, nil, types.Values{types.Value{1, "No1"}, types.Value{2, "No2"}, types.Value{3, "No3"}}},
			cValue,
		},
		{
			qBeginValue + `new_values AS \( SELECT \* FROM prefix_values WHERE FALSE\)` + qEndValue,
			&types.Scale{0, "yeah", types.ScaleOrdinal, nil, types.Values{}},
			[]driver.Value{"yeah", "ordinal"},
			[]driver.Value{2, "yeahR", "ordinal", `[{"id":null,"label":null}]`},
			&types.Scale{2, "yeahR", types.ScaleOrdinal, nil, types.Values{}},
			cValue,
		},
		{
			qUnit,
			&types.Scale{0, "yo", types.ScaleInterval, &types.UnitDesc{"˚C", types.JsonNullFloat64{-273.15, true}, types.JsonNullFloat64{0, false}}, nil},
			[]driver.Value{"yo", "interval", "˚C", -273.15, nil},
			[]driver.Value{2, "yo", "interval", "˚C", -273.15, nil},
			&types.Scale{2, "yo", types.ScaleInterval, &types.UnitDesc{"˚C", types.JsonNullFloat64{-273.15, true}, types.JsonNullFloat64{0, false}}, nil},
			cInterval,
		},
	}
	for i, test := range tests {
		db := newTestDB(t, "prefix")
		sqlmock.ExpectPrepare()
		sqlmock.ExpectQuery(test.q).WithArgs(test.a...).WillReturnRows(sqlmock.NewRows(test.col).AddRow(test.r...))
		c := &ScaleController{db}
		e := c.Create(test.sn)
		if e != nil {
			t.Errorf("Testcase %d: Unexcpected Error: %s\n", i, e)
		} else if !reflect.DeepEqual(test.sn, test.se) {
			t.Errorf("Testcase %d: Unexpected result:\n%+v\nexpected:\n%+v\n", i, test.sn, test.se)
		} else if e = db.Close(); e != nil {
			t.Errorf("Testcase %d: Unexpected database interaction: %s \n", i, e)
		}
	}
}

func TestReadScale(t *testing.T) {
	q := `SELECT s.id, s.label, s.type, json_agg\(\(v.id, v.label\)::prefix_scale_value ORDER BY v.index\) AS values, COALESCE\(u.unit, ''\) AS unit, u.min, u.max FROM prefix_scales s LEFT JOIN prefix_values v ON s.id = v.scale LEFT JOIN prefix_units u ON s.id = u.scale WHERE s.id = \$1 GROUP BY s.id, s.label, s.type, u.unit, u.min, u.max`
	col := []string{"id", "label", "type", "values", "unit", "min", "max"}
	tests := []struct {
		r  []driver.Value
		s  *types.Scale
		id types.Id
	}{
		{
			[]driver.Value{2, "scale", "nominal", `[{"id":3,"label":"No1"},{"id":2,"label":"No2"},{"id":1,"label":"No3"}]`, "", nil, nil},
			&types.Scale{2, "scale", types.ScaleNominal, nil, types.Values{types.Value{3, "No1"}, types.Value{2, "No2"}, types.Value{1, "No3"}}},
			types.Id(2),
		},
		{
			[]driver.Value{5, "scale", "interval", `[null]`, "˚C", -273.15, nil},
			&types.Scale{5, "scale", types.ScaleInterval, &types.UnitDesc{"˚C", types.JsonNullFloat64{-273.15, true}, types.JsonNullFloat64{0, false}}, nil},
			types.Id(5),
		},
	}
	for i, test := range tests {
		db := newTestDB(t, "prefix")
		sqlmock.ExpectPrepare()
		sqlmock.ExpectQuery(q).WithArgs(int(test.id)).WillReturnRows(sqlmock.NewRows(col).AddRow(test.r...))
		c := &ScaleController{db}
		s, e := c.Read(test.id)
		if e != nil {
			t.Errorf("Testcase %d: Unexcpected Error: %s\n", i, e)
		} else if !reflect.DeepEqual(s, test.s) {
			t.Errorf("Testcase %d: Unexpected result:\n%+v\nexpected:\n%+v\n", i, s, test.s)
		} else if e = db.Close(); e != nil {
			t.Errorf("Testcase %d: Unexpected database interaction: %s \n", i, e)
		}
	}
}

func TestUpdateScale(t *testing.T) {
	cValue := []string{"id", "label", "type", "values"}
	cInterval := []string{"id", "label", "type", "unit", "min", "max"}
	qSUpdate := `WITH updated_scale AS \( UPDATE prefix_scales SET label = \$1 WHERE id = \$2 RETURNING \* \)`
	// qChangeEmpty := `, changed_values AS \( SELECT \* FROM prefix_values WHERE FALSE \), deleted AS \( DELETE FROM prefix_values v USING updated_scale s WHERE v.scale = s.id AND v.id NOT IN \( SELECT id FROM changed_values \) \)`
	// qUpdateEmpty := `, updated_values AS \( SELECT \* FROM prefix_values WHERE FALSE \)`
	// qNewEmpty := `, new_values AS \( SELECT \* FROM prefix_values WHERE FALSE \)`
	qUpdateBegin := `, updated_values AS \( UPDATE prefix_values v SET label = n.label, "index" = n.index::bigint FROM updated_scale s, \( VALUES `
	qUpdateEnd := ` \) AS n \(id, label, "index"\) WHERE v.id = n.id::bigint AND v.scale = s.id RETURNING v.\* \)`
	qNewBegin := `, new_values AS \( INSERT INTO prefix_values \(label, scale, "index"\) SELECT v.label, s.id, v.index::bigint FROM updated_scale s, \( VALUES `
	qNewEnd := ` \) AS v \(label, "index"\) RETURNING \* \)`
	qChanges := `, changed_values AS \( SELECT \* FROM new_values UNION SELECT \* FROM updated_values \), deleted AS \( DELETE FROM prefix_values v USING updated_scale s WHERE v.scale = s.id AND v.id NOT IN \( SELECT id FROM changed_values \) \)`
	qValue := ` SELECT s.id, s.label, s.type, json_agg\(\(v.id, v.label\)::prefix_scale_value ORDER BY v."index"\) AS values FROM updated_scale s LEFT JOIN changed_values v ON s.id = v.scale GROUP BY s.id, s.label, s.type`
	qUnit := `, updated_unit AS \( UPDATE prefix_units SET unit = \$3, min = \$4, max = \$5 FROM updated_scale s WHERE scale = s.id RETURNING prefix_units.\* \) SELECT s.id, s.label, s.type, u.unit, u.min, u.max FROM updated_scale s LEFT JOIN updated_unit u ON s.id = u.scale`
	tests := []struct {
		q   string
		sn  *types.Scale
		a   []driver.Value
		r   []driver.Value
		se  *types.Scale
		col []string
	}{
		{
			qSUpdate + qUpdateBegin + `\(\$3, \$4, \$5\),\(\$6, \$7, \$8\),\(\$9, \$10, \$11\)` + qUpdateEnd + qNewBegin + `\(\$12, \$13\),\(\$14, \$15\)` + qNewEnd + qChanges + qValue,
			&types.Scale{2, "yeah", types.ScaleOrdinal, nil, types.Values{types.Value{1, "NewNo1"}, types.Value{2, "No2"}, types.Value{0, "NewNo3"}, types.Value{3, "NewNo4"}, types.Value{0, "New5"}}},
			[]driver.Value{"yeah", 2, 1, "NewNo1", 0, 2, "No2", 1, 3, "NewNo4", 3, "NewNo3", 2, "New5", 4},
			[]driver.Value{3, "yeahR", "ordinal", `[{"id":2,"label":"NewNo1R"},{"id":3,"label":"No2R"},{"id":5,"label":"NewNo3R"},{"id":4,"label":"NewNo4R"},{"id":6,"label":"New5R"}]`},
			&types.Scale{3, "yeahR", types.ScaleOrdinal, nil, types.Values{types.Value{2, "NewNo1R"}, types.Value{3, "No2R"}, types.Value{5, "NewNo3R"}, types.Value{4, "NewNo4R"}, types.Value{6, "New5R"}}},
			cValue,
		}, {
			qSUpdate + qUnit,
			&types.Scale{2, "yo", types.ScaleInterval, &types.UnitDesc{"˚K", types.JsonNullFloat64{0, true}, types.JsonNullFloat64{0, false}}, nil},
			[]driver.Value{"yo", 2, "˚K", 0., nil},
			[]driver.Value{3, "yoR", "interval", "˚KR", nil, 0.},
			&types.Scale{3, "yoR", types.ScaleInterval, &types.UnitDesc{"˚KR", types.JsonNullFloat64{0, false}, types.JsonNullFloat64{0, true}}, nil},
			cInterval,
		},
	}
	for i, test := range tests {
		db := newTestDB(t, "prefix")
		sqlmock.ExpectPrepare()
		sqlmock.ExpectQuery(test.q).WithArgs(test.a...).WillReturnRows(sqlmock.NewRows(test.col).AddRow(test.r...))
		c := &ScaleController{db}
		e := c.Update(test.sn)
		if e != nil {
			t.Errorf("Testcase %d: Unexcpected Error: %s\n", i, e)
		} else if !reflect.DeepEqual(test.sn, test.se) {
			t.Errorf("Testcase %d: Unexpected result:\n%+v\nexpected:\n%+v\n", i, test.sn, test.se)
		} else if e = db.Close(); e != nil {
			t.Errorf("Testcase %d: Unexpected database interaction: %s \n", i, e)
		}
	}
}
