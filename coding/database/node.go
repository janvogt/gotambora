package database

import (
	"database/sql"
	"errors"
	"github.com/janvogt/gotambora/coding/types"
	"github.com/jmoiron/sqlx"
)

type NodeController struct {
	db *DB
}

// New satisfies the types.Controller interface
func (n *NodeController) New() (r types.Resource) {
	return new(types.Node)
}

// Query satisfies the types.Controller interface
func (n *NodeController) Query(q map[string][]string) types.ResourceReader {
	par := make(map[string]interface{})
	sqlStr := "SELECT nodes.id, nodes.label, nodes.parent, json_agg(children.id) AS children FROM " + n.db.table("nodes") + " nodes LEFT JOIN " + n.db.table("nodes") + " children ON nodes.id = children.parent WHERE nodes.id != 0 AND "
	if len(q["label"]) != 0 {
		sqlStr += "nodes.label IN " + inParameter("label", q["label"], par)
		if len(q["parent"]) != 0 {
			sqlStr += "AND "
		}
	}
	if len(q["parent"]) != 0 {
		sqlStr += "nodes.parent IN " + inParameter("parent", q["parent"], par)
	} else if len(q["label"]) == 0 {
		sqlStr += "nodes.parent = 0 "
	}
	sqlStr += "GROUP BY nodes.id"
	res := new(NodeReader)
	var stmt *sqlx.NamedStmt
	stmt, res.err = n.db.PrepareNamed(sqlStr)
	if res.err != nil {
		return res
	}
	defer stmt.Close()
	res.rows, res.err = stmt.Queryx(par)
	return res
}

// Create satisfies the types.Controller interface
func (n *NodeController) Create(r types.Resource) (err error) {
	node, err := assertNode(r)
	if err != nil {
		return
	}
	err = n.db.CreateNode(node)
	return
}

// Read satisfies the types.Controller interface
func (n *NodeController) Read(id types.Id) (r types.Resource, err error) {
	r, err = n.db.ReadNode(id)
	return
}

// Update satisfies the types.Controller interface
func (n *NodeController) Update(r types.Resource) (err error) {
	node, err := assertNode(r)
	if err != nil {
		return
	}
	err = n.db.UpdateNode(node)
	return
}

// Delete satisfies the types.Controller interface
func (n *NodeController) Delete(id types.Id) (err error) {
	err = n.db.DeleteNode(id)
	return
}

type NodeReader struct {
	err  error
	rows *sqlx.Rows
}

// Read implements the types.DocumentReader interface
func (n *NodeReader) Read(r types.Resource) (ok bool, err error) {
	if n.err != nil {
		err = n.err
		return
	}
	node, err := assertNode(r)
	if err != nil {
		return
	}
	if ok = n.rows.Next(); ok {
		err = n.rows.StructScan(node)
	} else {
		n.rows.Close()
	}
	if err != nil {
		n.err = err
		ok = false
	}
	return
}

// Close implements the types.DocumentReader interface
func (n *NodeReader) Close() error {
	return n.rows.Close()
}

func assertNode(r types.Resource) (n *types.Node, err error) {
	switch node := r.(type) {
	case *types.Node:
		n = node
	default:
		err = errors.New("Unsuported Resource type, expected *Node.")
	}
	return
}

func (db *DB) NodeController() types.ResourceController {
	return &NodeController{db}
}

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
