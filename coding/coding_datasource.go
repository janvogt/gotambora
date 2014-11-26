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
	RootNodes() ([]Node, error)           // Gets all nodes with parent == 0.
	ChildNodes(id uint64) ([]Node, error) // Gets all nodes with parent == id.
	Node(id uint64) (*Node, error)        // Gets node with given id.
	CreateNode(n *Node) error             // Saves the given node to DB. The id will be set on succes. Providing a node with id already set is a failure.
	UpdateNode(n *Node) error             // Update the given node. Providing a node without id is a failure.
	DeleteNode(id uint64) error           // Delete the node with given id and all of its children.
}

// A DB datasource.
type DB struct {
	*sqlx.DB
	*log.Logger
	prefix string
}

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

// RootNodes gets all nodes with parent == 0.
func (db *DB) RootNodes() ([]Node, error) {
	return db.ChildNodes(0)
}

// ChildNodes gets all nodes with parent == id.
func (db *DB) ChildNodes(id uint64) (ns []Node, err error) {
	ns = make([]Node, 0)
	rows, err := db.Queryx("SELECT * FROM "+db.table("nodes")+" WHERE parent = $1 AND id != 0", id)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		n, e := db.scanNode(rows)
		if e != nil {
			err = e
			return
		}
		ns = append(ns, *n)
	}
	return
}

// Node gets node with given id.
func (db *DB) Node(id uint64) (np *Node, err error) {
	if id == 0 {
		err = errors.New(fmt.Sprintf("Unknown node (id = %d)", id))
		return
	}
	rows, err := db.Queryx("SELECT * FROM "+db.table("nodes")+" WHERE id = $1", id)
	if err != nil {
		return
	}
	if !rows.Next() {
		err = errors.New(fmt.Sprintf("Unknown node (id = %d)", id))
		return
	}
	n, err := db.scanNode(rows)
	if err != nil {
		return
	}
	np = n
	return
}

// scanNode creates a new Node based on scanning the current line in rows
func (db *DB) scanNode(rows *sqlx.Rows) (np *Node, err error) {
	n := Node{}
	err = rows.StructScan(&n)
	if err != nil {
		return
	}
	n.Children = make([]uint64, 0)
	err = db.Select(&n.Children, "SELECT id FROM "+db.table("nodes")+" WHERE parent = $1", n.Id)
	if err != nil {
		return
	}
	np = &n
	return
}

// CreateNode saves the given node to DB. The id will be set on succes.
func (db *DB) CreateNode(n *Node) (err error) {
	err = db.performWithTransaction(func(tx *sqlx.Tx) (err error) {
		rows, err := tx.NamedQuery("INSERT INTO "+db.table("nodes")+" (label, parent) VALUES (:label, :parent) RETURNING *", &n)
		if err != nil {
			return
		}
		defer rows.Close()
		if !rows.Next() {
			err = errors.New("Could not recieve created node")
			return
		}
		np, err := db.scanNode(rows)
		if err != nil {
			return
		}
		*n = *np
		return
	})
	return
}

// UpdateNode updates the given node based on it's id
func (db *DB) UpdateNode(n *Node) (err error) {
	err = db.performWithTransaction(func(tx *sqlx.Tx) (err error) {
		rows, err := db.NamedQuery("UPDATE "+db.table("nodes")+" SET (label, parent) = (:label, :parent) WHERE id = :id RETURNING *", &n)
		if err != nil {
			return
		}
		defer rows.Close()
		if !rows.Next() {
			err = errors.New("Could not recieve updated node")
			return
		}
		np, err := db.scanNode(rows)
		if err != nil {
			return
		}
		*n = *np
		return
	})
	return
}

// DeleteNode deletes the node with given id and all of its children.
func (db *DB) DeleteNode(id uint64) (err error) {
	_, err = db.Exec("DELETE FROM "+db.table("nodes")+" WHERE id = $1", id)
	return
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
