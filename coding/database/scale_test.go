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

// WITH new_scale AS \( INSERT INTO prefix_scales \(label, type\) VALUES \(\$1, \$2\) RETURNING \* \), new_scale_units AS \( VALUES \(\$3, \$4, \$5\) \), new_units AS \( INSERT INTO prefix_units \(scale, unit, "min", "max"\) SELECT s.id, v.column1, v.column2, v.column3 FROM new_scale s, new_scale_units v RETURNING \* \) SELECT s.id, s.label, s.type, u.unit, u.min, u.max FROM new_scale s LEFT JOIN new_units u ON s.id = u.scale
