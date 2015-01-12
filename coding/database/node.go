package database

import (
	"database/sql"
	"errors"
	"fmt"
	"github.com/janvogt/gotambora/coding/types"
	"github.com/jmoiron/sqlx"
	"net/http"
)

func (db *DB) NodeController() types.ResourceController {
	return &NodeController{db}
}

type NodeController struct {
	db *DB
}

// New satisfies the types.Controller interface
func (n *NodeController) New() (r types.Resource) {
	return new(types.Node)
}

// Query satisfies the types.Controller interface
func (nc *NodeController) Query(q map[string][]string) types.ResourceReader {
	args := make(map[string]interface{})
	where := "WHERE "
	if len(q["label"]) != 0 {
		where += "n.label IN " + inParameter("label", q["label"], args)
		if len(q["parent"]) != 0 {
			where += "AND "
		}
	}
	if len(q["parent"]) != 0 {
		where += "n.parent IN " + inParameter("parent", q["parent"], args)
	} else if len(q["label"]) == 0 {
		where += "n.parent is NULL "
	}
	qSql := selectNode(nc.db.table("nodes"), nc.db.table("nodes"), nc.db.table("links"), nc.db.table("node_metric"), where)
	res := new(NodeReader)
	var stmt *sqlx.NamedStmt
	stmt, res.err = nc.db.PrepareNamed(qSql)
	if res.err != nil {
		return res
	}
	res.rows, res.err = stmt.Queryx(args)
	return res
}

// Create satisfies the types.Controller interface
func (nc *NodeController) Create(r types.Resource) (err error) {
	n, err := assertNode(r)
	if err != nil {
		return
	}
	args := make(map[string]interface{})
	q := "WITH " + nc.newNode(n, args) + "," + nc.newLinks(n, args) + "," + nc.newNodeMetric(n, args) + " " + selectNode("new_node", nc.db.table("nodes"), "new_links", "new_node_metric", "")
	stmt, err := nc.db.PrepareNamed(q)
	if err != nil {
		return
	}
	err = stmt.Get(n, args)
	return
}

func (nc *NodeController) newNode(n *types.Node, args map[string]interface{}) string {
	args["newNodeLabel"], args["newNodeParent"] = n.Label, n.Parent
	return ` new_node AS ( INSERT INTO ` + nc.db.table("nodes") + ` ( label, parent ) VALUES ( :newNodeLabel, :newNodeParent ) RETURNING * )`
}

func (nc *NodeController) newLinks(n *types.Node, args map[string]interface{}) string {
	if n.References == nil || len(n.References) == 0 {
		return ` new_links AS ( SELECT * FROM ` + nc.db.table("links") + ` WHERE FALSE )`
	}
	v := ""
	for i, id := range n.References {
		to := fmt.Sprintf("newLinksId%d", i)
		args[to] = id
		v += fmt.Sprintf(",(:%s)", to)
	}
	return ` new_links AS ( INSERT INTO ` + nc.db.table("links") + ` ("from", "to") SELECT n.id, t.id::::bigint FROM new_node n, ( VALUES ` + v[1:] + ` ) AS t ( id ) RETURNING * )`
}

func (nc *NodeController) newNodeMetric(n *types.Node, args map[string]interface{}) string {
	if n.Metrics == nil || len(n.Metrics) == 0 {
		return ` new_node_metric AS ( SELECT * FROM ` + nc.db.table("node_metric") + ` WHERE FALSE )`
	}
	v := ""
	for i, id := range n.Metrics {
		met := fmt.Sprintf("newNodeMetric%d", i)
		args[met] = id
		v += fmt.Sprintf(",(:%s)", met)
	}
	return ` new_node_metric AS ( INSERT INTO ` + nc.db.table("node_metric") + ` (node, metric) SELECT n.id, m.id::::bigint FROM new_node n, ( VALUES ` + v[1:] + ` ) AS m ( id ) RETURNING * )`
}

// Read satisfies the types.Controller interface
func (nc *NodeController) Read(id types.Id) (r types.Resource, err error) {
	stmt, err := nc.db.Preparex(selectNode(nc.db.table("nodes"), nc.db.table("nodes"), nc.db.table("links"), nc.db.table("node_metric"), "WHERE n.id = $1"))
	if err != nil {
		return
	}
	n := new(types.Node)
	err = stmt.Get(n, id)
	if err == nil {
		r = n
	} else if err == sql.ErrNoRows {
		err = types.NewHttpError(http.StatusNotFound, fmt.Errorf("No node with id %d", id))
	}
	return
}

// Update satisfies the types.Controller interface
func (nc *NodeController) Update(r types.Resource) (err error) {
	n, err := assertNode(r)
	if err != nil {
		return
	}
	args := make(map[string]interface{})
	q := "WITH" + nc.updatedNode(n, args) + "," + nc.updatedLinks(n, args) + "," + nc.updatedNodeMetric(n, args) + " " + selectNode("updated_node", nc.db.table("nodes"), "updated_links", "updated_node_metric", "")
	stmt, err := nc.db.PrepareNamed(q)
	if err != nil {
		return
	}
	err = stmt.Get(n, args)
	return
}

func (nc *NodeController) updatedNode(n *types.Node, args map[string]interface{}) string {
	args["updatedNodeId"], args["updatedNodeLabel"], args["updatedNodeParent"] = n.Id, n.Label, n.Parent
	return ` updated_node AS ( UPDATE ` + nc.db.table("nodes") + ` SET label = :updatedNodeLabel, parent = :updatedNodeParent WHERE id = :updatedNodeId RETURNING * )`
}

func (nc *NodeController) updatedLinks(n *types.Node, args map[string]interface{}) (q string) {
	q = ` deleted_links AS ( DELETE FROM ` + nc.db.table("links") + ` WHERE "from" = :updatedNodeId )`
	if n.References == nil || len(n.References) == 0 {
		q += `, updated_links AS ( SELECT * FROM ` + nc.db.table("links") + ` WHERE FALSE )`
		return
	}
	v := ""
	for i, id := range n.References {
		to := fmt.Sprintf("updatedLinksId%d", i)
		args[to] = id
		v += fmt.Sprintf(",(:%s)", to)
	}
	q += `, updated_links AS ( INSERT INTO ` + nc.db.table("links") + ` ("from", "to") SELECT n.id, t.id::::bigint FROM updated_node n, ( VALUES ` + v[1:] + ` ) AS t ( id ) RETURNING * )`
	return
}

func (nc *NodeController) updatedNodeMetric(n *types.Node, args map[string]interface{}) (q string) {
	q = ` deleted_node_metric AS ( DELETE FROM ` + nc.db.table("node_metric") + ` WHERE node = :updatedNodeId )`
	if n.Metrics == nil || len(n.Metrics) == 0 {
		q += `, updated_node_metric AS ( SELECT * FROM ` + nc.db.table("node_metric") + ` WHERE FALSE )`
		return
	}
	v := ""
	for i, id := range n.Metrics {
		met := fmt.Sprintf("updatedNodeMetric%d", i)
		args[met] = id
		v += fmt.Sprintf(",(:%s)", met)
	}
	q += `, updated_node_metric AS ( INSERT INTO ` + nc.db.table("node_metric") + ` (node, metric) SELECT n.id, m.id::::bigint FROM updated_node n, ( VALUES ` + v[1:] + ` ) AS m ( id ) RETURNING * )`
	return
}

// Delete satisfies the types.Controller interface
func (nc *NodeController) Delete(id types.Id) (err error) {
	stmt, err := nc.db.Preparex("DELETE FROM " + nc.db.table("nodes") + " WHERE id = $1")
	if err != nil {
		return
	}
	res, err := stmt.Exec(id)
	if err != nil {
		return
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 0 {
		err = types.NewHttpError(http.StatusNotFound, fmt.Errorf("No node with id %d found!", id))
	}
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
		ok, n.err = false, err
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

func selectNode(nodesTable, childrenTable, linksTable, metricsTable, where string) string {
	return `SELECT n.id, n.label, n.parent, json_agg(DISTINCT c.id) AS children, json_agg(DISTINCT l.to) AS references, json_agg(DISTINCT m.metric) AS metrics FROM ` + nodesTable + ` n LEFT JOIN ` + childrenTable + ` c ON n.id = c.parent LEFT JOIN ` + linksTable + ` l ON n.id = l.from LEFT JOIN ` + metricsTable + ` m ON n.id = m.node ` + where + ` GROUP BY n.id, n.label, n.parent`
}
