package coding

import (
	"database/sql"
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

// QueryNodes implements NodeDatasource interface.
func (db *DB) QueryNodes(q *NodeQuery, res chan<- *Node, abort <-chan chan<- error) {
	defer close(res)
	par := make(map[string]interface{})
	sqlStr := "SELECT nodes.id, nodes.label, nodes.parent, json_agg(children.id) AS children FROM " + db.table("nodes") + " nodes LEFT JOIN " + db.table("nodes") + " children ON nodes.id = children.parent WHERE nodes.id != 0 AND "
	if len(q.Labels) != 0 {
		sqlStr += "nodes.label IN " + inParameter("label", q.Labels, par)
		if len(q.Parents) != 0 {
			sqlStr += "AND "
		}
	}
	if len(q.Parents) != 0 {
		sqlStr += "nodes.parent IN " + inParameter("parent", q.Parents, par)
	} else if len(q.Labels) == 0 {
		sqlStr += "nodes.parent = 0 "
	}
	sqlStr += "GROUP BY nodes.id"
	var stmt *sqlx.NamedStmt
	ok := exponentialRetry(abort, func() error {
		var err error
		stmt, err = db.PrepareNamed(sqlStr)
		return err
	})
	if !ok {
		return
	}
	var rows *sqlx.Rows
	ok = exponentialRetry(abort, func() error {
		var err error
		rows, err = stmt.Queryx(par)
		return err
	})
	if !ok {
		return
	}
	for rows.Next() {
		n := &Node{}
		ok := exponentialRetry(abort, func() error {
			err := rows.StructScan(n)
			return err
		})
		if !ok {
			return
		}
		select {
		case res <- n:
		case errChan := <-abort:
			errChan <- nil
			return
		}
	}
	return
}

// CreateNode implements NodeDatasource interface.
func (db *DB) CreateNode(n *Node) (err error) {
	stmt, err := db.PrepareNamed("WITH ins AS (INSERT INTO " + db.table("nodes") + " (label, parent) VALUES (:label, :parent) RETURNING *) SELECT node.id, node.label, node.parent, json_agg(children.id) AS children FROM ins node LEFT JOIN " + db.table("nodes") + " children ON node.id = children.parent GROUP BY node.id, node.label, node.parent")
	if err != nil {
		return
	}
	err = stmt.Get(n, &n)
	return
}

var (
	ErrUnknownId = errors.New("Unknown Id")
)

// ReadNode implements NodeDatasource interface.
func (db *DB) ReadNode(id Id) (n *Node, err error) {
	if id == 0 {
		err = ErrUnknownId
		return
	}
	stmt, err := db.Preparex("SELECT node.id, node.label, node.parent, json_agg(children.id) AS children FROM " + db.table("nodes") + " node LEFT JOIN " + db.table("nodes") + " children ON node.id = children.parent WHERE node.id = $1 GROUP BY node.id")
	if err != nil {
		return
	}
	n = &Node{}
	err = stmt.Get(n, id)
	if err == sql.ErrNoRows {
		err = ErrUnknownId
	}
	if err != nil {
		n = nil
	}
	return
}

// UpdateNode implements NodeDatasource interface.
func (db *DB) UpdateNode(n *Node) (err error) {
	if n.Id == 0 {
		err = ErrUnknownId
		return
	}
	stmt, err := db.PrepareNamed("WITH upd AS (UPDATE " + db.table("nodes") + " SET (label, parent) = (:label, :parent) WHERE id = :id RETURNING *) SELECT node.id, node.label, node.parent, json_agg(children.id) AS children FROM upd node LEFT JOIN " + db.table("nodes") + " children ON node.id = children.parent GROUP BY node.id, node.label, node.parent")
	if err != nil {
		return
	}
	err = stmt.Get(n, &n)
	if err == sql.ErrNoRows {
		err = ErrUnknownId
	}
	return
}

// DeleteNode implements NodeDatasource interface.
func (db *DB) DeleteNode(id Id) (err error) {
	if id == 0 {
		return ErrUnknownId
	}
	stmt, err := db.Preparex("DELETE FROM " + db.table("nodes") + " WHERE id = $1")
	if err != nil {
		return
	}
	res, err := stmt.Exec(id)
	if err != nil {
		return
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 0 {
		err = ErrUnknownId
	}
	return
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

const createSchemaSQLTemplate = `
CREATE SEQUENCE %[1]s_nodes_id_seq MINVALUE 1;
CREATE TABLE %[1]s_nodes (
  id        bigint PRIMARY KEY DEFAULT nextval('%[1]s_nodes_id_seq'),
  label     text NOT NULL,
  parent    bigint
);
ALTER SEQUENCE %[1]s_nodes_id_seq OWNED BY %[1]s_nodes.id;
INSERT INTO %[1]s_nodes (id, label, parent) VALUES (0, 'root', 0);
ALTER TABLE %[1]s_nodes ADD CONSTRAINT %[1]s_nodes_parent_fkey
  FOREIGN KEY (parent) REFERENCES %[1]s_nodes(id) ON DELETE CASCADE;
CREATE FUNCTION %[1]s_version() RETURNS bigint
  AS 'SELECT CAST(1 AS bigint);'
  LANGUAGE SQL
  IMMUTABLE;
`

const dropSchemaSQLTemplate = `
DROP TABLE IF EXISTS %[1]s_nodes;
DROP SEQUENCE IF EXISTS %[1]s_nodes_id_seq;
DROP FUNCTION IF EXISTS %[1]s_version();
`
