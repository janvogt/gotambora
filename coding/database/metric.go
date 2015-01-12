package database

import (
	"fmt"
	"github.com/janvogt/gotambora/coding/types"
	"github.com/jmoiron/sqlx"
	"net/http"
)

func (db *DB) MetricController() types.ResourceController {
	return &MetricController{db}
}

type MetricController struct {
	db *DB
}

// New implements the ResourceController interface
func (mc *MetricController) New() (r types.Resource) {
	return new(types.Metric)
}

// Query implements the ResourceController interface
func (mc *MetricController) Query(q map[string][]string) (res types.ResourceReader) {
	reader := new(MetricReader)
	reader.rows, reader.err = mc.db.Queryx(selectMetrics(mc.db.table("metrics"), mc.db.table("metric_scale"), ""))
	return reader
}

// Create implements the ResourceController interface
func (mc *MetricController) Create(r types.Resource) (err error) {
	m, err := assertMetric(r)
	if err != nil {
		return
	}
	args := make(map[string]interface{})
	q := "WITH " + mc.newMetricScale(m, args) + " " + selectMetrics("new_metric", "new_metric_scale", "")
	stmt, err := mc.db.PrepareNamed(q)
	if err != nil {
		return
	}
	err = stmt.Get(m, args)
	return
}

func (mc *MetricController) newMetric(m *types.Metric, args map[string]interface{}) string {
	args["newMetricLabel"] = m.Label
	return ` new_metric AS ( INSERT INTO ` + mc.db.table("metrics") + `( label ) VALUES ( :newMetricLabel ) RETURNING * )`
}

func (mc *MetricController) newMetricScale(m *types.Metric, args map[string]interface{}) (q string) {
	q = mc.newMetric(m, args)
	if m.Scales == nil || len(m.Scales) == 0 {
		q += `, new_metric_scale AS ( SELECT * FROM ` + mc.db.table("metric_scale") + ` WHERE FALSE )`
		return
	}
	v := ""
	for i, id := range m.Scales {
		sca := fmt.Sprintf("newMetricScaleScale%d", i)
		args[sca] = id
		v += fmt.Sprintf(",(:%s)", sca)
	}
	q += `, new_metric_scale AS ( INSERT INTO ` + mc.db.table("metric_scale") + ` ( metric, scale ) SELECT m.id, s.id::::bigint FROM new_metric m, ( VALUES ` + v[1:] + ` ) AS s (id) RETURNING * )`
	return
}

// Read implements the ResourceController interface
func (mc *MetricController) Read(id types.Id) (r types.Resource, err error) {
	stmt, err := mc.db.Preparex(selectMetrics(mc.db.table("metrics"), mc.db.table("metric_scale"), "WHERE m.id = $1"))
	if err != nil {
		return
	}
	m := new(types.Metric)
	err = stmt.Get(m, id)
	if err != nil {
		return
	}
	r = m
	return
}

// Update implements the ResourceController interface
func (mc *MetricController) Update(r types.Resource) (err error) {
	m, err := assertMetric(r)
	if err != nil {
		return
	}
	args := make(map[string]interface{})
	q := "WITH " + mc.updatedMetricScale(m, args) + " " + selectMetrics("updated_metric", "updated_metric_scale", "")
	stmt, err := mc.db.PrepareNamed(q)
	if err != nil {
		return
	}
	err = stmt.Get(m, args)
	return
}

func (mc *MetricController) updatedMetric(m *types.Metric, args map[string]interface{}) string {
	args["updatedMetricId"], args["updatedMetricLabel"] = m.Id, m.Label
	return ` updated_metric AS ( UPDATE ` + mc.db.table("metrics") + ` SET label = :updatedMetricLabel WHERE id = :updatedMetricId RETURNING * )`
}

func (mc *MetricController) updatedMetricScale(m *types.Metric, args map[string]interface{}) (q string) {
	q = mc.updatedMetric(m, args) + `, deleted AS ( DELETE FROM ` + mc.db.table("metric_scale") + ` WHERE metric IN ( SELECT id FROM updated_metric ) )`
	if m.Scales == nil || len(m.Scales) == 0 {
		q += `, updated_metric_scale AS ( SELECT * FROM ` + mc.db.table("metric_scale") + ` WHERE FALSE )`
		return
	}
	v := ""
	for i, id := range m.Scales {
		sca := fmt.Sprintf("updatedMetricScaleScale%d", i)
		args[sca] = id
		v += fmt.Sprintf(",(:%s)", sca)
	}
	q += `, updated_metric_scale AS ( INSERT INTO ` + mc.db.table("metric_scale") + ` ( metric, scale ) SELECT m.id, s.id::::bigint FROM updated_metric m, ( VALUES ` + v[1:] + ` ) AS s (id) RETURNING * )`
	return
}

// Delete implements the ResourceController interface
func (mc *MetricController) Delete(id types.Id) (err error) {
	res, err := mc.db.Exec("DELETE FROM "+mc.db.table("metrics")+" WHERE id = $1", id)
	if err != nil {
		return
	}
	n, err := res.RowsAffected()
	if err != nil {
		return
	}
	if n != 1 {
		err = types.NewHttpError(http.StatusNotFound, fmt.Errorf("No metric found with id %d", id))
	}
	return
}

type MetricReader struct {
	err  error
	rows *sqlx.Rows
}

// Read implements the types.DocumentReader interface
func (mr *MetricReader) Read(r types.Resource) (ok bool, err error) {
	if mr.err != nil {
		err = mr.err
		return
	}
	m, err := assertMetric(r)
	if err != nil {
		return
	}
	if ok = mr.rows.Next(); ok {
		err = mr.rows.StructScan(m)
	} else {
		mr.rows.Close()
	}
	if err != nil {
		ok, mr.err = false, err
	}
	return
}

// Close implements the types.DocumentReader interface
func (mr *MetricReader) Close() error {
	return mr.rows.Close()
}

func assertMetric(r types.Resource) (m *types.Metric, err error) {
	switch r := r.(type) {
	case *types.Metric:
		m = r
	default:
		err = fmt.Errorf("Unsuported Resource type, expected *Metric.")
	}
	return
}

func selectMetrics(metrics, metricScale, where string) string {
	return "SELECT m.id, m.label, json_agg(ms.scale) AS scales FROM " + metrics + " m LEFT JOIN " + metricScale + " ms ON m.id = ms.metric " + where + " GROUP BY m.id, m.label"
}
