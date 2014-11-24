package coding

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
)

// Datasource for the coding package.
type DataSource interface {
	RootNodes() ([]Node, error)           // Gets all nodes with parent == 0.
	ChildNodes(id uint64) ([]Node, error) // Gets all nodes with parent == id.
	Node(id uint64) (*Node, error)        // Gets node with given id.
	CreateNode(n *Node) error             // Saves the given node to DB. The id will be set on succes. Providing a node with id already set is a failure.
	UpdateNode(n *Node) error             // Update the given node. Providing a node without id is a failure.
	DeleteNode(id uint64) error           // Delete the node with given id and all of its children.
}

// A DB datasource.
type DB struct {
	*sql.DB
	*log.Logger
	prefix string
}

// NewDB creates a new DB datasource using a given sql.DB. Creates the necessary schema if it does not exist.
func NewDB(db *sql.DB, prefix string) (ds *DB, err error) {
	newDb := &DB{db, log.New(os.Stderr, "coding/DB", log.LstdFlags), prefix}
	v, err := newDb.Version()
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

// RootNodes gets all nodes with parent == 0.
func (db *DB) RootNodes() ([]Node, error) {
	return nil, nil
}

// ChildNodes gets all nodes with parent == id.
func (db *DB) ChildNodes(id uint64) ([]Node, error) {
	return nil, nil
}

// Node gets node with given id.
func (db *DB) Node(id uint64) (*Node, error) {
	return &Node{}, nil
}

// CreateNode saves the given node to DB. The id will be set on succes. Providing a node with id already set is a failure.
func (db *DB) CreateNode(n *Node) error {
	return nil
}

// UpdateNode updates the given node. Providing a node without id is a failure.
func (db *DB) UpdateNode(n *Node) error {
	return nil
}

// DeleteNode deletes the node with given id and all of its children.
func (db *DB) DeleteNode(id uint64) error {
	return nil
}

// performWithTransaction performs the given function f embedded in a transaction performing a roll back on failure.
func (db *DB) performWithTransaction(f func(tx *sql.Tx) error) (err error) {
	txn, err := db.Begin()
	if err != nil {
		return
	}
	err = f(txn)
	if err != nil {
		mustRollback(txn)
		return
	}
	err = txn.Commit()
	if err != nil {
		mustRollback(txn)
		return
	}
	return
}

// Clean deletes all changes to the DB done by this package. It's a no-op if nothing was changed.
func (db *DB) Clean() error {
	return db.performWithTransaction(func(tx *sql.Tx) (err error) {
		_, err = tx.Exec(fmt.Sprintf(dropSchemaSQLTemplate, db.prefix))
		return
	})
}

func mustRollback(txn *sql.Tx) {
	if e := txn.Rollback(); e != nil {
		panic(e)
	}
}

// createSchema creates the necessary DB schema. It is an error if it exists already.
func (db *DB) createSchema() error {
	return db.performWithTransaction(func(tx *sql.Tx) (err error) {
		_, err = tx.Exec(fmt.Sprintf(createSchemaSQLTemplate, db.prefix))
		return
	})
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

const createSchemaSQLTemplate = `
CREATE SEQUENCE %[1]s_nodes_id_seq MINVALUE 1;
CREATE TABLE %[1]s_nodes (
  id        bigint PRIMARY KEY DEFAULT nextval('%[1]s_nodes_id_seq'),
  label     text NOT NULL,
  parent    bigint
);
ALTER SEQUENCE %[1]s_nodes_id_seq OWNED BY %[1]s_nodes.id;
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
