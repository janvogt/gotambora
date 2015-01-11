package database

import (
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"reflect"
	"time"
)

// A DB datasource.
type DB struct {
	*sqlx.DB
	prefix string
}

// NewDB creates a new DB datasource using a given sql.DB. Creates the necessary schema if it does not exist.
func NewDB(db *sqlx.DB, prefix string) (ds *DB, err error) {
	newDB := &DB{db, prefix}
	v, err := newDB.Version()
	if err != nil {
		return
	}
	switch {
	case v == 0:
		err = newDB.createSchema()
	case v > 1:
		err = errors.New(fmt.Sprintf("Database Version for coding is %d. Can't downgrade to needed version 1. (db.Version() returned %[1]d)", v, newDB.prefix))
	}
	if err != nil {
		return
	}
	ds = newDB
	return
}

// Version returns the verion of the current schema. 0 means there is no schema set.
func (db *DB) Version() (v uint64, err error) {
	res, err := db.Query(fmt.Sprintf("SELECT 1 FROM pg_proc WHERE proname = '%[1]s_version';", db.prefix))
	if err != nil || !res.Next() {
		return
	}
	row := db.QueryRow(fmt.Sprintf("SELECT %[1]s_version();", db.prefix))
	err = row.Scan(&v)
	return
}

// Clean deletes all changes to the DB done by this package. It's a no-op if nothing was changed.
func (db *DB) Clean() error {
	return db.performWithTransaction(func(tx *sqlx.Tx) (err error) {
		_, err = tx.Exec(fmt.Sprintf(dropSchemaSQLTemplate, db.prefix))
		return
	})
}

// exponentialRetry retries to execute f in exponentially growing intervalls until it does not return an error or it recieves an value on the abort channel. Returns true if f succeeded.
func exponentialRetry(abort <-chan chan<- error, f func() error) bool {
	err := f()
	if err == nil {
		return true
	}
	i := uint(0)
	for err != nil {
		retry := time.After(10 * (1 << i) * time.Millisecond)
		select {
		case errChan := <-abort:
			errChan <- err
			return false
		case <-retry:
			err = f()
		}
		i++
	}
	return true
}

// inParameter creates the sql string for a named IN clause. The paremeters are numbered sequencially and prefix with the given string. If values is not a slice it panics.
func inParameter(prefix string, values interface{}, parameter map[string]interface{}) (sqlStr string) {
	first := true
	v := reflect.ValueOf(values)
	if v.Len() == 0 {
		return
	}
	sqlStr += "("
	for i := 0; i < v.Len(); i++ {
		parName := fmt.Sprintf("%s%d", prefix, i)
		parameter[parName] = v.Index(i).Interface()
		if !first {
			sqlStr += ", "
		} else {
			first = false
		}
		sqlStr += ":" + parName
	}
	sqlStr += ") "
	return
}

// table returns the prefixed tablename based on the given subfix.
func (db *DB) table(name string) string {
	return db.prefix + "_" + name
}

// performWithTransaction performs the given function f embedded in a transaction performing a roll back on failure.
func (db *DB) performWithTransaction(f func(tx *sqlx.Tx) error) (err error) {
	txn, err := db.Beginx()
	if err != nil {
		return
	}
	defer txn.Rollback()
	err = f(txn)
	if err != nil {
		return
	}
	err = txn.Commit()
	return
}

// createSchema creates the necessary DB schema. It is an error if it exists already.
func (db *DB) createSchema() error {
	return db.performWithTransaction(func(tx *sqlx.Tx) (err error) {
		_, err = tx.Exec(fmt.Sprintf(createSchemaSQLTemplate, db.prefix))
		return
	})
}

const labelFieldType = `text NOT NULL`

const idFieldType = `bigint`

const nodesTable = `
CREATE SEQUENCE %[1]s_nodes_id_seq MINVALUE 1;
CREATE TABLE %[1]s_nodes (
	id     ` + idFieldType + ` PRIMARY KEY DEFAULT nextval('%[1]s_nodes_id_seq'),
	label  ` + labelFieldType + `,
  parent ` + idFieldType + ` REFERENCES %[1]s_nodes(id) ON DELETE CASCADE
);
ALTER SEQUENCE %[1]s_nodes_id_seq OWNED BY %[1]s_nodes.id;
`

const linksTable = `
CREATE TABLE %[1]s_links (
  "from" ` + idFieldType + ` NOT NULL REFERENCES %[1]s_nodes(id) ON DELETE CASCADE,
  "to"   ` + idFieldType + ` NOT NULL REFERENCES %[1]s_nodes(id) ON DELETE CASCADE,
	PRIMARY KEY ("from", "to")
);
`

const scalesTables = `
CREATE SEQUENCE %[1]s_scales_id_seq;
CREATE TABLE %[1]s_scales (
	id    ` + idFieldType + ` PRIMARY KEY DEFAULT nextval('%[1]s_scales_id_seq'),
	label ` + labelFieldType + `,
	type text NOT NULL
);
ALTER SEQUENCE %[1]s_scales_id_seq OWNED BY %[1]s_scales.id;

CREATE SEQUENCE %[1]s_values_id_seq;
CREATE TABLE %[1]s_values (
  id      ` + idFieldType + ` PRIMARY KEY DEFAULT nextval('%[1]s_values_id_seq'),
	scale   ` + idFieldType + ` NOT NULL REFERENCES %[1]s_scales(id) ON DELETE CASCADE,
	"index" int NOT NULL,
	label   ` + labelFieldType + `,
  UNIQUE (scale, "index") DEFERRABLE
);
ALTER SEQUENCE %[1]s_values_id_seq OWNED BY %[1]s_values.id;

CREATE TABLE %[1]s_units (
	scale ` + idFieldType + ` PRIMARY KEY REFERENCES %[1]s_scales(id) ON DELETE CASCADE,
	unit ` + labelFieldType + `,
  "min" double precision,
  "max" double precision
);

CREATE TYPE %[1]s_scale_value AS (
  id bigint,
  label text
);
`

const metricsTable = `
CREATE SEQUENCE %[1]s_metrics_id_seq;
CREATE TABLE %[1]s_metrics (
  id    ` + idFieldType + ` PRIMARY KEY DEFAULT nextval('%[1]s_metrics_id_seq'),
  label ` + labelFieldType + `
);
ALTER SEQUENCE %[1]s_metrics_id_seq OWNED BY %[1]s_metrics.id;

CREATE TABLE %[1]s_metric_scale (
  metric ` + idFieldType + ` NOT NULL REFERENCES %[1]s_metrics(id) ON DELETE CASCADE,
  scale   ` + idFieldType + ` NOT NULL REFERENCES %[1]s_scales(id) ON DELETE CASCADE,
  PRIMARY KEY (metric, scale)
);

CREATE TABLE %[1]s_node_metric (
  node    ` + idFieldType + ` NOT NULL REFERENCES %[1]s_nodes(id) ON DELETE CASCADE,
  metric ` + idFieldType + ` NOT NULL REFERENCES %[1]s_metrics(id) ON DELETE RESTRICT,
  PRIMARY KEY (node, metric)
);
`

const eventsTable = `
CREATE SEQUENCE %[1]s_events_id_seq;
CREATE TABLE %[1]s_events (
  id   ` + idFieldType + ` PRIMARY KEY DEFAULT nextval('%[1]s_events_id_seq'),
  type ` + idFieldType + ` REFERENCES %[1]s_nodes(id) ON DELETE RESTRICT
);
ALTER SEQUENCE %[1]s_events_id_seq OWNED BY %[1]s_events.id;

CREATE TABLE %[1]s_event_ratings (
  event   ` + idFieldType + ` NOT NULL REFERENCES %[1]s_events(id) ON DELETE CASCADE,
  value   ` + idFieldType + ` REFERENCES %[1]s_values(id) ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE TABLE %[1]s_event_values (
  event   ` + idFieldType + ` NOT NULL REFERENCES %[1]s_events(id) ON DELETE CASCADE,
  scale   ` + idFieldType + ` NOT NULL REFERENCES %[1]s_units(scale) ON UPDATE CASCADE ON DELETE RESTRICT,
  value   double precision NOT NULL
);
`

const createSchemaSQLTemplate = nodesTable + linksTable + scalesTables + metricsTable + eventsTable + `
CREATE FUNCTION %[1]s_version() RETURNS bigint
  AS 'SELECT CAST(1 AS bigint);'
  LANGUAGE SQL
  IMMUTABLE;
`

const dropSchemaSQLTemplate = `
DROP FUNCTION IF EXISTS %[1]s_version();
DROP TABLE IF EXISTS %[1]s_event_values;
DROP TABLE IF EXISTS %[1]s_event_ratings;
DROP TABLE IF EXISTS %[1]s_events;
DROP TABLE IF EXISTS %[1]s_node_metric;
DROP TABLE IF EXISTS %[1]s_metric_scale;
DROP TABLE IF EXISTS %[1]s_metrics;
DROP TABLE IF EXISTS %[1]s_units;
DROP TABLE IF EXISTS %[1]s_values;
DROP TYPE IF EXISTS %[1]s_scale_value;
DROP TABLE IF EXISTS %[1]s_scales;
DROP TABLE IF EXISTS %[1]s_links;
DROP TABLE IF EXISTS %[1]s_nodes;
`
