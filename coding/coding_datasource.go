package coding

import (
	"errors"
	"fmt"
	"github.com/jmoiron/sqlx"
	"log"
	"os"
)

// Datasource for the coding package.
type DataSource interface {
	QueryNodes(id uint64, parent uint64) (nodes []Node, err error) // Gets all nodes satisfying the given criteria
	InsertNode(n *Node) (insNode *Node, err error)                 // Inserts the given node and returns the result (including new id)
	UpdateNode(n *Node) (updNode *Node, err error)                 // Updates the given node.
	DeleteNode(id uint64) (err error)                              // Delete the given node.
}

// A DB datasource.
type DB struct {
	*sqlx.DB
	*log.Logger
	prefix string
}

var (
	ErrNotFound = errors.New("Not found")
)

// NewDB creates a new DB datasource using a given sql.DB. Creates the necessary schema if it does not exist.
func NewDB(db *sqlx.DB, prefix string) (ds *DB, err error) {
	newDb := &DB{db, log.New(os.Stderr, "coding/DB", log.LstdFlags), prefix}
	v, err := newDb.version()
	if err != nil {
		return
	}
	switch {
	case v == 0:
		err = newDb.createSchema()
	case v > 1:
		err = errors.New(fmt.Sprintf("Database Version for coding is %d. Can't downgrade to needed version 1. (db.Version() returned %[1]d)", v, newDb.prefix))
	}
	if err != nil {
		return
	}
	ds = newDb
	return
}

// QueryNodes gets nodes based on a filter node.
func (db *DB) QueryNodes(id uint64, parent uint64) (nodes []Node, err error) {
	sql := "SELECT * FROM " + db.table("nodes") + " WHERE"
	if id != 0 {
		sql += fmt.Sprintf(" id = %d", id)
	} else {
		sql += fmt.Sprintf(" parent = %d", parent)
	}
	err = db.Select(&nodes, sql)
	return
}

// InsertNode saves the given node to DB. The id will be set on succes.
func (db *DB) InsertNode(n *Node) (insNode *Node, err error) {
	stmt, err := db.PrepareNamed("INSERT INTO " + db.table("nodes") + " (label, parent) VALUES (:label, :parent) RETURNING *")
	if err != nil {
		return
	}
	insNode = &Node{}
	err = stmt.Get(insNode, &n)
	return
}

// UpdateNode updates the given node based on it's id
func (db *DB) UpdateNode(n *Node) (updNode *Node, err error) {
	if n.Id == 0 {
		err = ErrNotFound
		return
	}
	stmt, err := db.PrepareNamed("UPDATE " + db.table("nodes") + " SET (label, parent) = (:label, :parent) WHERE id = :id RETURNING *")
	if err != nil {
		return
	}
	updNode = &Node{}
	err = stmt.Get(updNode, &n)
	return
}

// DeleteNode deletes the node with given id and all of its children.
func (db *DB) DeleteNode(id uint64) (err error) {
	if id == 0 {
		err = ErrNotFound
		return
	}
	_, err = db.Exec("DELETE FROM "+db.table("nodes")+" WHERE id = $1", id)
	return
}

// Clean deletes all changes to the DB done by this package. It's a no-op if nothing was changed.
func (db *DB) Clean() error {
	return db.performWithTransaction(func(tx *sqlx.Tx) (err error) {
		_, err = tx.Exec(fmt.Sprintf(dropSchemaSQLTemplate, db.prefix))
		return
	})
}

// createSchema creates the necessary DB schema. It is an error if it exists already.
func (db *DB) createSchema() error {
	return db.performWithTransaction(func(tx *sqlx.Tx) (err error) {
		_, err = tx.Exec(fmt.Sprintf(createSchemaSQLTemplate, db.prefix))
		return
	})
}

// Version returns the verion of the current schema. 0 means there is no schema set.
func (db *DB) version() (v uint64, err error) {
	res, err := db.Query(fmt.Sprintf("SELECT 1 FROM pg_proc WHERE proname = '%[1]s_version';", db.prefix))
	if err != nil || !res.Next() {
		return
	}
	row := db.QueryRow(fmt.Sprintf("SELECT %[1]s_version();", db.prefix))
	err = row.Scan(&v)
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
