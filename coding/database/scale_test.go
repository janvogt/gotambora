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
	qBeginValue := `WITH new_scale AS \( INSERT INTO prefix_scales \(label, type\) VALUES \(\$1, \$2\) RETURNING \* \), new_scale_values AS \( VALUES `
	qEndValue := ` \), new_values AS \( INSERT INTO prefix_values \(label, scale, "index"\) SELECT v.column1, s.id, v.column2 FROM new_scale s, new_scale_values v RETURNING \* \) SELECT s.id, s.label, s.type, json_agg\(\(v.id, v.label\)::prefix_scale_value ORDER BY v."index"\) AS values FROM new_scale s LEFT JOIN new_values v ON s.id = v.scale GROUP BY s.id, s.label, s.type`
	qInterval := `WITH new_scale AS \( INSERT INTO prefix_scales \(label, type\) VALUES \(\$1, \$2\) RETURNING \* \), new_scale_units AS \( VALUES \(\$3, \$4, \$5\) \), new_units AS \( INSERT INTO prefix_units \(scale, unit, "min", "max"\) SELECT s.id, v.column1, v.column2, v.column3 FROM new_scale s, new_scale_units v RETURNING \* \) SELECT s.id, s.label, s.type, u.unit, u.min, u.max FROM new_scale s LEFT JOIN new_units u ON s.id = u.scale`
	tests := []struct {
		q   string
		sn  *types.Scale
		se  *types.Scale
		r   []driver.Value
		a   []driver.Value
		col []string
	}{
		{
			qBeginValue + `\(\$3, \$4\),\(\$5, \$6\),\(\$7, \$8\)` + qEndValue,
			&types.Scale{0, "yeah", types.ScaleOrdinal, nil, types.Values{types.Value{0, "No1"}, types.Value{0, "No2"}, types.Value{0, "No3"}}},
			&types.Scale{2, "yeah", types.ScaleOrdinal, nil, types.Values{types.Value{1, "No1"}, types.Value{2, "No2"}, types.Value{3, "No3"}}},
			[]driver.Value{2, "yeah", "ordinal", `[{"id":1,"label":"No1"},{"id":2,"label":"No2"},{"id":3,"label":"No3"}]`},
			[]driver.Value{"yeah", "ordinal", 0, "No1", 1, "No2", 2, "No3"},
			cValue,
		},
		{
			qInterval,
			&types.Scale{0, "yo", types.ScaleInterval, &types.Unit{"˚C", types.JsonNullFloat64{-273.15, true}, types.JsonNullFloat64{0, false}}, nil},
			&types.Scale{2, "yo", types.ScaleInterval, &types.Unit{"˚C", types.JsonNullFloat64{-273.15, true}, types.JsonNullFloat64{0, false}}, nil},
			[]driver.Value{2, "yo", "interval", "˚C", -273.15, nil},
			[]driver.Value{"yo", "interval", "˚C", -273.15, nil},
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
	q := `SELECT s.id, s.label, s.type, json_agg\(\(v.id, v.label\)::prefix_scale_value ORDER BY v.index\) AS values, COALESCE\(u.unit, ""\), u.min, u.max FROM prefix_scales s LEFT JOIN prefix_values v ON s.id = v.scale LEFT JOIN prefix_units u ON s.id = u.scale WHERE s.id = \$1 GROUP BY s.id, s.label, s.type, u.unit, u.min, u.max`
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
			&types.Scale{5, "scale", types.ScaleInterval, &types.Unit{"˚C", types.JsonNullFloat64{-273.15, true}, types.JsonNullFloat64{0, false}}, nil},
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
	qValue1 := `WITH updated_scale AS \( UPDATE prefix_scales SET label = \$1 WHERE id = \$2 RETURNING \* \), update_scale_values AS \( VALUES `
	qValue2 := ` \), updated_values AS \( UPDATE prefix_values SET label = column2, "index" = column3 FROM update_scale_values WHERE id = column1 RETURNING prefix_values.\* \), new_scale_values AS \( VALUES `
	qValue3 := ` \), new_values AS \( INSERT INTO prefix_values \(label, scale, "index"\) SELECT v.column1, s.id, v.column2 FROM updated_scale s, new_scale_values v RETURNING \* \), changed_values AS \( SELECT \* FROM new_values UNION SELECT \* FROM updated_values \), all_values AS \( SELECT \* FROM changed_values UNION SELECT v.\* FROM updated_scale s, prefix_values v WHERE s.id = v.scale AND v.id NOT IN \( SELECT id FROM changed_values \) \) SELECT s.id, s.label, s.type, json_agg\(\(v.id, v.label\)::prefix_scale_value ORDER BY v."index"\) AS values FROM updated_scale s LEFT JOIN all_values v ON s.id = v.scale GROUP BY s.id, s.label, s.type`
	qUnit := `WITH updated_scale AS \( UPDATE prefix_scales SET label = \$1 WHERE id = \$2 RETURNING \* \), update_unit AS \( UPDATE prefix_units SET unit = \$3, min = \$4, max = \$5 FROM updated_scale WHERE scale = id RETURNING prefix_units.\* \) SELECT s.id, s.label, s.type, u.unit, u.min, u.max FROM updated_scale s LEFT JOIN update_unit u ON s.id = u.scale`
	tests := []struct {
		q   string
		sn  *types.Scale
		a   []driver.Value
		r   []driver.Value
		se  *types.Scale
		col []string
	}{
		{
			qValue1 + `\(\$3, \$4, \$5\),\(\$6, \$7, \$8\),\(\$9, \$10, \$11\)` + qValue2 + `\(\$12, \$13\),\(\$14, \$15\)` + qValue3,
			&types.Scale{2, "yeah", types.ScaleOrdinal, nil, types.Values{types.Value{1, "NewNo1"}, types.Value{2, "No2"}, types.Value{0, "NewNo3"}, types.Value{3, "NewNo4"}, types.Value{0, "New5"}}},
			[]driver.Value{"yeah", 2, 1, "NewNo1", 0, 2, "No2", 1, 3, "NewNo4", 3, "NewNo3", 2, "New5", 4},
			[]driver.Value{3, "yeahR", "ordinal", `[{"id":2,"label":"NewNo1R"},{"id":3,"label":"No2R"},{"id":5,"label":"NewNo3R"},{"id":4,"label":"NewNo4R"},{"id":6,"label":"New5R"}]`},
			&types.Scale{3, "yeahR", types.ScaleOrdinal, nil, types.Values{types.Value{2, "NewNo1R"}, types.Value{3, "No2R"}, types.Value{5, "NewNo3R"}, types.Value{4, "NewNo4R"}, types.Value{6, "New5R"}}},
			cValue,
		}, {
			qUnit,
			&types.Scale{2, "yo", types.ScaleInterval, &types.Unit{"˚K", types.JsonNullFloat64{0, true}, types.JsonNullFloat64{0, false}}, nil},
			[]driver.Value{"yo", 2, "˚K", 0., nil},
			[]driver.Value{3, "yoR", "interval", "˚KR", nil, 0.},
			&types.Scale{3, "yoR", types.ScaleInterval, &types.Unit{"˚KR", types.JsonNullFloat64{0, false}, types.JsonNullFloat64{0, true}}, nil},
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
