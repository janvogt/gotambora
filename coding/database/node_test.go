package database

import (
	"database/sql"
	"database/sql/driver"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/janvogt/gotambora/coding/types"
	"testing"
	"time"
)

/**
 * Implementation of NodeDatasource
 */
func TestQueryNode(t *testing.T) {
	db := newTestDB(t, "coding")
	defer closeDb(t, db.DB)
	tests := []struct {
		query    *types.NodeQuery
		sqlQuery string
		args     []driver.Value
		sqlResp  driver.Rows
		err      error
		nodes    []*types.Node
		timeout  time.Duration
	}{
		{&types.NodeQuery{},
			"SELECT nodes.id, nodes.label, nodes.parent, json_agg\\(children.id\\) AS children FROM coding_nodes nodes LEFT JOIN coding_nodes children ON nodes.id = children.parent WHERE nodes.id != 0 AND nodes.parent = 0 GROUP BY nodes.id",
			[]driver.Value{},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,Hallo,2,\"[2,3,4]\""),
			nil,
			[]*types.Node{
				&types.Node{1, "Hallo", 2, []types.Id{2, 3, 4}},
			},
			500 * time.Millisecond},
		{&types.NodeQuery{Parents: []types.Id{3}},
			"SELECT nodes.id, nodes.label, nodes.parent, json_agg\\(children.id\\) AS children FROM coding_nodes nodes LEFT JOIN coding_nodes children ON nodes.id = children.parent WHERE nodes.id != 0 AND nodes.parent IN \\(\\$1\\) GROUP BY nodes.id",
			[]driver.Value{3},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,Hallo,2,\"[2,3,4]\""),
			nil,
			[]*types.Node{
				&types.Node{1, "Hallo", 2, []types.Id{2, 3, 4}},
			},
			500 * time.Millisecond},
		{&types.NodeQuery{Parents: []types.Id{0}, Labels: []types.Label{"H"}},
			"SELECT nodes.id, nodes.label, nodes.parent, json_agg\\(children.id\\) AS children FROM coding_nodes nodes LEFT JOIN coding_nodes children ON nodes.id = children.parent WHERE nodes.id != 0 AND nodes.label IN \\(\\$1\\) AND nodes.parent IN \\(\\$2\\) GROUP BY nodes.id",
			[]driver.Value{"H", 0},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,Hallo,2,\"[2,3,4]\""),
			nil,
			[]*types.Node{
				&types.Node{1, "Hallo", 2, []types.Id{2, 3, 4}},
			},
			500 * time.Millisecond},
		{&types.NodeQuery{Labels: []types.Label{"H"}},
			"SELECT nodes.id, nodes.label, nodes.parent, json_agg\\(children.id\\) AS children FROM coding_nodes nodes LEFT JOIN coding_nodes children ON nodes.id = children.parent WHERE nodes.id != 0 AND nodes.label IN \\(\\$1\\) GROUP BY nodes.id",
			[]driver.Value{"H"},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,Hallo,2,\"[2,3,4]\""),
			nil,
			[]*types.Node{
				&types.Node{1, "Hallo", 2, []types.Id{2, 3, 4}},
			},
			500 * time.Millisecond},
		{&types.NodeQuery{Labels: []types.Label{"H", "I"}},
			"SELECT nodes.id, nodes.label, nodes.parent, json_agg\\(children.id\\) AS children FROM coding_nodes nodes LEFT JOIN coding_nodes children ON nodes.id = children.parent WHERE nodes.id != 0 AND nodes.label IN \\(\\$1, \\$2\\) GROUP BY nodes.id",
			[]driver.Value{"H", "I"},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,Hallo,2,\"[2,3,4]\"\n2,Bello,4,\"[4,3,2]\""),
			nil,
			[]*types.Node{
				&types.Node{1, "Hallo", 2, []types.Id{2, 3, 4}},
				&types.Node{2, "Bello", 4, []types.Id{4, 3, 2}},
			},
			500 * time.Millisecond},
		{&types.NodeQuery{Labels: []types.Label{"H", "I"}},
			"SELECT nodes.id, nodes.label, nodes.parent, json_agg\\(children.id\\) AS children FROM coding_nodes nodes LEFT JOIN coding_nodes children ON nodes.id = children.parent WHERE nodes.id != 0 AND nodes.label IN \\(\\$1, \\$2\\) GROUP BY nodes.id",
			[]driver.Value{"H", "I"},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,Hallo,2,\"[null]\"\n2,Bello,4,\"[null]\""),
			nil,
			[]*types.Node{
				&types.Node{1, "Hallo", 2, []types.Id{}},
				&types.Node{2, "Bello", 4, []types.Id{}},
			},
			500 * time.Millisecond},
	}
	for caseId, test := range tests {
		if test.sqlQuery != "" {
			exp := sqlmock.ExpectQuery(test.sqlQuery).WithArgs(test.args...)
			if test.err != nil {
				exp.WillReturnError(test.err)
			} else {
				exp.WillReturnRows(test.sqlResp)
			}
		}
		abtChan := make(chan chan<- error)
		resChan := make(chan *types.Node)
		go db.QueryNodes(test.query, resChan, abtChan)
		timout := time.After(test.timeout)
		i := 0
	L:
		for {
			select {
			case res, more := <-resChan:
				if !more {
					if i < len(test.nodes) {
						t.Errorf("Test Case: %d: Got not all nodes while querying. Expected %d, but got only %d.\n", caseId, len(test.nodes), i)
					}
					break L
				} else if i == len(test.nodes) {
					t.Errorf("Test Case: %d: Result Channel not closed while querying after recieving all %d expected nodes.\n", caseId, i)
					break L
				}
				if !compareNode(res, test.nodes[i]) {
					t.Errorf("Test Case: %d: Unexpected node when querying. Expected %#v, but got %#v.\n", caseId, test.nodes[i], res)
				}
				i++
			case <-timout:
				errChan := make(chan error)
				abtChan <- errChan
				err := <-errChan
				if err != test.err {
					t.Errorf("Test Case: %d: Unexpected Error when querying nodes: %s.\n", caseId, err)
				} else if err == nil {
					t.Errorf("Test Case: %d: Timout after %T when querying nodes.\nTest Case: %d", caseId, test.timeout)
				}
				break L
			}
		}
	}
}

func TestCreateNode(t *testing.T) {
	db := newTestDB(t, "coding")
	defer closeDb(t, db.DB)
	sqlCreateRegex := "WITH ins AS \\(INSERT INTO coding_nodes \\(label, parent\\) VALUES \\(\\$1, \\$2\\) RETURNING \\*\\) SELECT node.id, node.label, node.parent, json_agg\\(children.id\\) AS children FROM ins node LEFT JOIN coding_nodes children ON node.id = children.parent GROUP BY node.id, node.label, node.parent"
	tests := []struct {
		node    types.Node
		sqlResp driver.Rows
		expNode *types.Node
		err     error
	}{
		{types.Node{Id: types.Id(5), Label: "label", Parent: types.Id(2)},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "3,label,5,[]"),
			&types.Node{Id: types.Id(3), Label: "label", Parent: types.Id(5)},
			nil},
		{types.Node{Label: "label", Parent: types.Id(2)},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "8,l,0,[]"),
			&types.Node{Id: types.Id(8), Label: "l", Parent: types.Id(0)},
			nil},
		{types.Node{Id: types.Id(5), Label: "label", Parent: types.Id(2)},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "0,,0,[]"),
			&types.Node{Id: types.Id(0), Parent: types.Id(0)},
			nil},
		{types.Node{Id: types.Id(5), Label: "label", Parent: types.Id(2), Children: []types.Id{5, 8, 0}},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "4,label,2,[]"),
			&types.Node{Id: types.Id(4), Label: "label", Parent: types.Id(2), Children: []types.Id{}},
			nil},
		{types.Node{Label: "label", Parent: types.Id(2), Children: []types.Id{5, 8, 0}},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,label,2,[]"),
			&types.Node{Id: types.Id(1), Label: "label", Parent: types.Id(2), Children: []types.Id{}},
			nil},
		{types.Node{Label: "label", Parent: types.Id(2), Children: []types.Id{5, 8, 0}},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, ""),
			&types.Node{Label: "label", Parent: types.Id(2), Children: []types.Id{5, 8, 0}},
			ErrTest},
		{types.Node{Label: "label", Parent: types.Id(2), Children: []types.Id{5, 8, 0}},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, ""),
			&types.Node{Label: "label", Parent: types.Id(2), Children: []types.Id{5, 8, 0}},
			sql.ErrNoRows},
	}
	for _, test := range tests {
		exp := sqlmock.ExpectQuery(sqlCreateRegex).WithArgs(test.node.Label, int(test.node.Parent))
		if test.err != nil {
			exp.WillReturnError(test.err)
		} else {
			exp.WillReturnRows(test.sqlResp)
		}
		err := db.CreateNode(&test.node)
		if err != test.err {
			t.Errorf("Expected creation of node return err = %s, but got err %s. Test case: %#v", test.err, err, test)
		} else if test.node.Id != test.expNode.Id || test.node.Label != test.expNode.Label || test.node.Parent != test.expNode.Parent || !compareIdSlice(test.node.Children, test.expNode.Children) {
			t.Errorf("Expected creation to update given node to %#v, but got %#v. Test case %#v", test.expNode, test.node, test)
		}
	}
}

func TestReadNode(t *testing.T) {
	db := newTestDB(t, "coding")
	defer closeDb(t, db.DB)
	sqlReadRegex := "SELECT node.id, node.label, node.parent, json_agg\\(children.id\\) AS children FROM coding_nodes node LEFT JOIN coding_nodes children ON node.id = children.parent WHERE node.id = \\$1 GROUP BY node.id"
	tests := []struct {
		id      types.Id
		sqlResp driver.Rows
		expNode *types.Node
		err     error
	}{
		{types.Id(3),
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "3,label,5,[]"),
			&types.Node{Id: types.Id(3), Label: "label", Parent: types.Id(5), Children: []types.Id{}},
			nil},
		{types.Id(8),
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "8,l,0,[]"),
			&types.Node{Id: types.Id(8), Label: "l", Parent: types.Id(0), Children: []types.Id{}},
			nil},
		{types.Id(1),
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,,0,[]"),
			&types.Node{Id: types.Id(1), Parent: types.Id(0), Children: []types.Id{}},
			nil},
		{types.Id(4),
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "4,label,2,[]"),
			&types.Node{Id: types.Id(4), Label: "label", Parent: types.Id(2), Children: []types.Id{5, 8, 0}},
			nil},
		{types.Id(4),
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, ""),
			nil,
			ErrUnknownId},
		{types.Id(0),
			nil,
			nil,
			ErrUnknownId},
	}
	for _, test := range tests {
		if test.sqlResp != nil {
			exp := sqlmock.ExpectQuery(sqlReadRegex).WithArgs(int(test.id))
			if test.err != nil {
				exp.WillReturnError(test.err)
			} else {
				exp.WillReturnRows(test.sqlResp)
			}
		}
		node, err := db.ReadNode(test.id)
		if err != test.err {
			t.Errorf("Expected retrieval of node %d not to succeed, but got err %s. Test case: %#v", test.id, err, test)
		} else if test.expNode != nil && (test.id != test.expNode.Id || node.Label != test.expNode.Label || node.Parent != test.expNode.Parent) {
			t.Errorf("Expected retrieval of node %#v, but got %#v. Test case %#v", test.expNode, node, test)
		} else if test.expNode == nil && node != nil {
			t.Errorf("Expected no retrieval of any node, but got %#v. Test case %#v", test.expNode, node, test)
		}
	}
}

func TestUpdateNode(t *testing.T) {
	db := newTestDB(t, "coding")
	defer closeDb(t, db.DB)
	sqlUpdateRegex := "WITH upd AS \\(UPDATE coding_nodes SET \\(label, parent\\) = \\(\\$1, \\$2\\) WHERE id = \\$3 RETURNING \\*\\) SELECT node.id, node.label, node.parent, json_agg\\(children.id\\) AS children FROM upd node LEFT JOIN coding_nodes children ON node.id = children.parent GROUP BY node.id, node.label, node.parent"
	tests := []struct {
		node    types.Node
		sqlResp driver.Rows
		expNode *types.Node
		err     error
	}{
		{types.Node{Id: types.Id(5), Label: "label", Parent: types.Id(2)},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "3,label,5,[]"),
			&types.Node{Id: types.Id(3), Label: "label", Parent: types.Id(5)},
			nil},
		{types.Node{Id: types.Id(1), Label: "label", Parent: types.Id(2)},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "8,l,0,[]"),
			&types.Node{Id: types.Id(8), Label: "l", Parent: types.Id(0)},
			nil},
		{types.Node{Id: types.Id(5), Label: "label", Parent: types.Id(2)},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "0,,0,[]"),
			&types.Node{Id: types.Id(0), Parent: types.Id(0)},
			nil},
		{types.Node{Label: "label", Parent: types.Id(2), Children: []types.Id{5, 8, 0}},
			nil,
			&types.Node{Label: "label", Parent: types.Id(2), Children: []types.Id{5, 8, 0}},
			ErrUnknownId},
		{types.Node{Id: types.Id(7), Label: "label", Parent: types.Id(2), Children: []types.Id{5, 8, 0}},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "5,labl,3,[]"),
			&types.Node{Id: types.Id(5), Label: "labl", Parent: types.Id(3), Children: []types.Id{}},
			nil},
		{types.Node{Id: types.Id(7), Label: "label", Parent: types.Id(2), Children: []types.Id{5, 8, 0}},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, ""),
			&types.Node{Id: types.Id(7), Label: "label", Parent: types.Id(2), Children: []types.Id{5, 8, 0}},
			ErrUnknownId},
		{types.Node{Label: "label", Parent: types.Id(0), Children: []types.Id{5, 8, 0}},
			nil,
			&types.Node{Label: "label", Parent: types.Id(0), Children: []types.Id{5, 8, 0}},
			ErrUnknownId},
	}
	for _, test := range tests {
		if test.sqlResp != nil {
			exp := sqlmock.ExpectQuery(sqlUpdateRegex).WithArgs(test.node.Label, int(test.node.Parent), int(test.node.Id))
			if test.err != nil {
				exp.WillReturnError(test.err)
			} else {
				exp.WillReturnRows(test.sqlResp)
			}
		}
		err := db.UpdateNode(&test.node)
		if err != test.err {
			t.Errorf("Expected update of node to return err %s, but got err %s. Test case: %#v", test.err, err, test)
		} else if test.node.Id != test.expNode.Id || test.node.Label != test.expNode.Label || test.node.Parent != test.expNode.Parent || !compareIdSlice(test.node.Children, test.expNode.Children) {
			t.Errorf("Expected update to update given node to %#v, but got %#v. Test case %#v", test.expNode, test.node, test)
		}
	}
}

func TestDeleteNode(t *testing.T) {
	db := newTestDB(t, "coding")
	defer closeDb(t, db.DB)
	sqlReadRegex := "DELETE FROM coding_nodes WHERE id = \\$1"
	tests := []struct {
		id      types.Id
		sqlResp driver.Result
		err     error
	}{
		{types.Id(3), sqlmock.NewResult(0, 1), nil},
		{types.Id(8), sqlmock.NewResult(0, 0), ErrUnknownId},
		{types.Id(1), sqlmock.NewErrorResult(ErrTest), ErrTest},
		{types.Id(0), nil, ErrUnknownId},
	}
	for _, test := range tests {
		if test.sqlResp != nil {
			sqlmock.ExpectExec(sqlReadRegex).WithArgs(int(test.id)).WillReturnResult(test.sqlResp)
		}
		err := db.DeleteNode(test.id)
		if err != test.err {
			t.Errorf("Expected deletion of node %d to return err = %s, but got err %s. Test case: %#v", test.id, test.err, err, test)
		}
	}
}

/**
 * test helper functions
 */

func compareNode(a *types.Node, b *types.Node) bool {
	if a == nil || b == nil && a != b {
		return false
	}
	return a.Id == b.Id && a.Label == b.Label && a.Parent == b.Parent && compareIdSlice(a.Children, b.Children)
}
