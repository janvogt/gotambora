package database

import (
	"database/sql"
	"errors"
	"github.com/janvogt/gotambora/coding/types"
	"github.com/jmoiron/sqlx"
)

// QueryNodes implements NodeDatasource interface.
func (db *DB) QueryNodes(q *types.NodeQuery, res chan<- *types.Node, abort <-chan chan<- error) {
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
		n := &types.Node{}
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
func (db *DB) CreateNode(n *types.Node) (err error) {
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
func (db *DB) ReadNode(id types.Id) (n *types.Node, err error) {
	if id == 0 {
		err = ErrUnknownId
		return
	}
	stmt, err := db.Preparex("SELECT node.id, node.label, node.parent, json_agg(children.id) AS children FROM " + db.table("nodes") + " node LEFT JOIN " + db.table("nodes") + " children ON node.id = children.parent WHERE node.id = $1 GROUP BY node.id")
	if err != nil {
		return
	}
	n = &types.Node{}
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
func (db *DB) UpdateNode(n *types.Node) (err error) {
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
func (db *DB) DeleteNode(id types.Id) (err error) {
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
