package coding

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"testing"
	"time"
)

var ErrTest = errors.New("Test Error")

func TestVersion(t *testing.T) {
	db := newTestDB(t, "coding")
	defer closeDb(t, db.DB)
	sqlmock.ExpectQuery("SELECT 1 FROM pg_proc WHERE proname = 'coding_version';").WillReturnError(ErrTest)
	v, err := db.Version()
	if v != 0 || err != ErrTest {
		t.Errorf("db.Version() should return 0 and error from DB, but got version %d and err %#v", v, err)
	}
	sqlmock.ExpectQuery("SELECT 1 FROM pg_proc WHERE proname = 'coding_version';").WillReturnRows(sqlmock.NewRows([]string{"row"}))
	v, err = db.Version()
	if v != 0 || err != nil {
		t.Errorf("db.Version() should return 0 and no error if version routine does not exist, but got version %d and err %#v", v, err)
	}
	sqlmock.ExpectQuery("SELECT 1 FROM pg_proc WHERE proname = 'coding_version';").WillReturnRows(sqlmock.NewRows([]string{"row"}).AddRow(""))
	sqlmock.ExpectQuery("SELECT coding_version\\(\\);").WillReturnError(ErrTest)
	v, err = db.Version()
	if v != 0 || err != ErrTest {
		t.Errorf("db.Version() should return 0 and error from db or scan if call to version routing fails, but got version %d and err %#v", v, err)
	}
	sqlmock.ExpectQuery("SELECT 1 FROM pg_proc WHERE proname = 'coding_version';").WillReturnRows(sqlmock.NewRows([]string{"row"}).AddRow(""))
	sqlmock.ExpectQuery("SELECT coding_version\\(\\);").WillReturnRows(sqlmock.NewRows([]string{"row"}).AddRow(" "))
	v, err = db.Version()
	if v != 0 || err == nil {
		t.Errorf("db.Version() should return 0 and error if version routine returns unexpected result \" \", but got version %d and err %#v", v, err)
	}
	var someVersion uint64 = 3
	sqlmockExpectVersion("coding", 3)
	v, err = db.Version()
	if v != someVersion || err != nil {
		t.Errorf("db.Version() should return %d and no error if version routine returns that version, but got version %d and err %#v", someVersion, v, err)
	}
}

func TestTable(t *testing.T) {
	somePrefix := "prefix"
	someTable := "table"
	db := DB{prefix: somePrefix}
	if tab := db.table(someTable); tab != somePrefix+"_"+someTable {
		t.Errorf("Expected table name for \"%s\" with prefix \"%s\" to be \"%s\", but got \"%s\"", someTable, somePrefix, somePrefix+"_"+someTable, tab)
	}
}

func TestPerformWithTransaction(t *testing.T) {
	db := newTestDB(t, "coding")
	defer closeDb(t, db.DB)
	sqlmock.ExpectBegin()
	err := db.performWithTransaction(func(tx *sqlx.Tx) (err error) {
		if tx == nil {
			t.Errorf("Argument of performWithTransaction() should never be called without valid transaction.")
		}
		sqlmock.ExpectExec("Protected SQL").WillReturnResult(sqlmock.NewResult(1, 1))
		tx.MustExec("Protected SQL")
		sqlmock.ExpectCommit()
		return
	})
	if err != nil {
		t.Errorf("Successful call to argument of performWithTransaction() should not return error, but got %s", err)
	}
	sqlmock.ExpectBegin().WillReturnError(ErrTest)
	err = db.performWithTransaction(func(tx *sqlx.Tx) (err error) {
		t.Errorf("Argument of performWithTransaction() should never be called if Begin fails.")
		return
	})
	if err != ErrTest {
		t.Errorf("performWithTransaction() should return error of failed db.Begin(), but got %s", err)
	}
	sqlmock.ExpectBegin()
	err = db.performWithTransaction(func(tx *sqlx.Tx) (err error) {
		sqlmock.ExpectRollback()
		return ErrTest
	})
	if err != ErrTest {
		t.Errorf("performWithTransaction() should return error returned by its argument, but got %s", err)
	}
	sqlmock.ExpectBegin()
	err = db.performWithTransaction(func(tx *sqlx.Tx) (err error) {
		sqlmock.ExpectCommit().WillReturnError(ErrTest)
		return
	})
	if err != ErrTest {
		t.Errorf("performWithTransaction() should return error of failing commit, but got %s", err)
	}
}

func TestCreateSchema(t *testing.T) {
	db := newTestDB(t, "coding")
	defer closeDb(t, db.DB)
	sqlmockExpectCreateSchema("coding")
	err := db.createSchema()
	if err != nil {
		t.Errorf("createSchema() should suceed if schema creation is sucessful, but got %s", err)
	}
	sqlmock.ExpectBegin()
	sqlmock.ExpectExec(".*? CREATE FUNCTION coding_version\\(\\) RETURNS bigint AS 'SELECT CAST\\(1 AS bigint\\);' LANGUAGE SQL IMMUTABLE;").WillReturnError(ErrTest)
	sqlmock.ExpectRollback()
	err = db.createSchema()
	if err != ErrTest {
		t.Errorf("createSchema() should return error if schema creation fails, but got err = %s", err)
	}
}

func TestClean(t *testing.T) {
	db := newTestDB(t, "coding")
	defer closeDb(t, db.DB)
	sqlmock.ExpectBegin()
	sqlmock.ExpectExec(".*?DROP FUNCTION IF EXISTS coding_version\\(\\);").WillReturnResult(sqlmock.NewResult(1, 1))
	sqlmock.ExpectCommit()
	err := db.Clean()
	if err != nil {
		t.Errorf("Clean() should suceed if schema creation is sucessful, but got %s", err)
	}
	sqlmock.ExpectBegin()
	sqlmock.ExpectExec(".*?DROP FUNCTION IF EXISTS coding_version\\(\\);").WillReturnError(ErrTest)
	sqlmock.ExpectRollback()
	err = db.Clean()
	if err != ErrTest {
		t.Errorf("Clean() should return error if schema creation fails, but got err = %s", err)
	}
}

func TestNewDb(t *testing.T) {
	dbx := setUpTestDB(t)
	defer closeDb(t, dbx)
	somePrefix := "prefix"
	sqlmockExpectVersion(somePrefix, 2)
	db, err := NewDB(dbx, somePrefix)
	if db != nil || err == nil {
		t.Errorf("NewDB() should fail and not create a db schema if version > 1, but got db = %#v", db)
	}
	sqlmockExpectVersion(somePrefix, 0)
	sqlmockExpectCreateSchema(somePrefix)
	db, err = NewDB(dbx, somePrefix)
	if db == nil || err != nil {
		t.Errorf("NewDB() should sucessfully create schema if version < 1, but got %s", err)
	}
	if db.DB == nil || db.prefix != somePrefix {
		t.Errorf("Expected NewDB() to have valid non-nil DB field and correct prefix %s, but got %#v", somePrefix, db)
	}
	sqlmockExpectVersion(somePrefix, 1)
	db, err = NewDB(dbx, somePrefix)
	if err != nil {
		t.Errorf("NewDB() should succeed and leave DB untouched if version = 1, but got err = %#v", db)
	}
	if db.DB == nil || db.prefix != somePrefix {
		t.Errorf("Expected NewDB() to have valid non-nil DB field and correct prefix %s, but got %#v", somePrefix, db)
	}
}

/**
 * Implementation of NodeDatasource
 */
func TestQueryNode(t *testing.T) {
	db := newTestDB(t, "coding")
	defer closeDb(t, db.DB)
	tests := []struct {
		query    *NodeQuery
		sqlQuery string
		args     []driver.Value
		sqlResp  driver.Rows
		err      error
		nodes    []*Node
		timeout  time.Duration
	}{
		{&NodeQuery{},
			"SELECT nodes.id, nodes.label, nodes.parent, json_agg\\(children.id\\) AS children FROM coding_nodes nodes LEFT JOIN coding_nodes children ON nodes.id = children.parent WHERE nodes.id != 0 AND nodes.parent = 0 GROUP BY nodes.id",
			[]driver.Value{},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,Hallo,2,\"[2,3,4]\""),
			nil,
			[]*Node{
				&Node{1, "Hallo", 2, []Id{2, 3, 4}},
			},
			500 * time.Millisecond},
		{&NodeQuery{Parents: []Id{3}},
			"SELECT nodes.id, nodes.label, nodes.parent, json_agg\\(children.id\\) AS children FROM coding_nodes nodes LEFT JOIN coding_nodes children ON nodes.id = children.parent WHERE nodes.id != 0 AND nodes.parent IN \\(\\$1\\) GROUP BY nodes.id",
			[]driver.Value{3},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,Hallo,2,\"[2,3,4]\""),
			nil,
			[]*Node{
				&Node{1, "Hallo", 2, []Id{2, 3, 4}},
			},
			500 * time.Millisecond},
		{&NodeQuery{Parents: []Id{0}, Labels: []Label{"H"}},
			"SELECT nodes.id, nodes.label, nodes.parent, json_agg\\(children.id\\) AS children FROM coding_nodes nodes LEFT JOIN coding_nodes children ON nodes.id = children.parent WHERE nodes.id != 0 AND nodes.label IN \\(\\$1\\) AND nodes.parent IN \\(\\$2\\) GROUP BY nodes.id",
			[]driver.Value{"H", 0},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,Hallo,2,\"[2,3,4]\""),
			nil,
			[]*Node{
				&Node{1, "Hallo", 2, []Id{2, 3, 4}},
			},
			500 * time.Millisecond},
		{&NodeQuery{Labels: []Label{"H"}},
			"SELECT nodes.id, nodes.label, nodes.parent, json_agg\\(children.id\\) AS children FROM coding_nodes nodes LEFT JOIN coding_nodes children ON nodes.id = children.parent WHERE nodes.id != 0 AND nodes.label IN \\(\\$1\\) GROUP BY nodes.id",
			[]driver.Value{"H"},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,Hallo,2,\"[2,3,4]\""),
			nil,
			[]*Node{
				&Node{1, "Hallo", 2, []Id{2, 3, 4}},
			},
			500 * time.Millisecond},
		{&NodeQuery{Labels: []Label{"H", "I"}},
			"SELECT nodes.id, nodes.label, nodes.parent, json_agg\\(children.id\\) AS children FROM coding_nodes nodes LEFT JOIN coding_nodes children ON nodes.id = children.parent WHERE nodes.id != 0 AND nodes.label IN \\(\\$1, \\$2\\) GROUP BY nodes.id",
			[]driver.Value{"H", "I"},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,Hallo,2,\"[2,3,4]\"\n2,Bello,4,\"[4,3,2]\""),
			nil,
			[]*Node{
				&Node{1, "Hallo", 2, []Id{2, 3, 4}},
				&Node{2, "Bello", 4, []Id{4, 3, 2}},
			},
			500 * time.Millisecond},
		{&NodeQuery{Labels: []Label{"H", "I"}},
			"SELECT nodes.id, nodes.label, nodes.parent, json_agg\\(children.id\\) AS children FROM coding_nodes nodes LEFT JOIN coding_nodes children ON nodes.id = children.parent WHERE nodes.id != 0 AND nodes.label IN \\(\\$1, \\$2\\) GROUP BY nodes.id",
			[]driver.Value{"H", "I"},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,Hallo,2,\"[null]\"\n2,Bello,4,\"[null]\""),
			nil,
			[]*Node{
				&Node{1, "Hallo", 2, []Id{}},
				&Node{2, "Bello", 4, []Id{}},
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
		resChan := make(chan *Node)
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
		node    Node
		sqlResp driver.Rows
		expNode *Node
		err     error
	}{
		{Node{Id: Id(5), Label: "label", Parent: Id(2)},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "3,label,5,[]"),
			&Node{Id: Id(3), Label: "label", Parent: Id(5)},
			nil},
		{Node{Label: "label", Parent: Id(2)},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "8,l,0,[]"),
			&Node{Id: Id(8), Label: "l", Parent: Id(0)},
			nil},
		{Node{Id: Id(5), Label: "label", Parent: Id(2)},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "0,,0,[]"),
			&Node{Id: Id(0), Parent: Id(0)},
			nil},
		{Node{Id: Id(5), Label: "label", Parent: Id(2), Children: []Id{5, 8, 0}},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "4,label,2,[]"),
			&Node{Id: Id(4), Label: "label", Parent: Id(2), Children: []Id{}},
			nil},
		{Node{Label: "label", Parent: Id(2), Children: []Id{5, 8, 0}},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,label,2,[]"),
			&Node{Id: Id(1), Label: "label", Parent: Id(2), Children: []Id{}},
			nil},
		{Node{Label: "label", Parent: Id(2), Children: []Id{5, 8, 0}},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, ""),
			&Node{Label: "label", Parent: Id(2), Children: []Id{5, 8, 0}},
			ErrTest},
		{Node{Label: "label", Parent: Id(2), Children: []Id{5, 8, 0}},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, ""),
			&Node{Label: "label", Parent: Id(2), Children: []Id{5, 8, 0}},
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
		id      Id
		sqlResp driver.Rows
		expNode *Node
		err     error
	}{
		{Id(3),
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "3,label,5,[]"),
			&Node{Id: Id(3), Label: "label", Parent: Id(5), Children: []Id{}},
			nil},
		{Id(8),
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "8,l,0,[]"),
			&Node{Id: Id(8), Label: "l", Parent: Id(0), Children: []Id{}},
			nil},
		{Id(1),
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "1,,0,[]"),
			&Node{Id: Id(1), Parent: Id(0), Children: []Id{}},
			nil},
		{Id(4),
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "4,label,2,[]"),
			&Node{Id: Id(4), Label: "label", Parent: Id(2), Children: []Id{5, 8, 0}},
			nil},
		{Id(4),
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, ""),
			nil,
			ErrUnknownId},
		{Id(0),
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
		node    Node
		sqlResp driver.Rows
		expNode *Node
		err     error
	}{
		{Node{Id: Id(5), Label: "label", Parent: Id(2)},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "3,label,5,[]"),
			&Node{Id: Id(3), Label: "label", Parent: Id(5)},
			nil},
		{Node{Id: Id(1), Label: "label", Parent: Id(2)},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "8,l,0,[]"),
			&Node{Id: Id(8), Label: "l", Parent: Id(0)},
			nil},
		{Node{Id: Id(5), Label: "label", Parent: Id(2)},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "0,,0,[]"),
			&Node{Id: Id(0), Parent: Id(0)},
			nil},
		{Node{Label: "label", Parent: Id(2), Children: []Id{5, 8, 0}},
			nil,
			&Node{Label: "label", Parent: Id(2), Children: []Id{5, 8, 0}},
			ErrUnknownId},
		{Node{Id: Id(7), Label: "label", Parent: Id(2), Children: []Id{5, 8, 0}},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, "5,labl,3,[]"),
			&Node{Id: Id(5), Label: "labl", Parent: Id(3), Children: []Id{}},
			nil},
		{Node{Id: Id(7), Label: "label", Parent: Id(2), Children: []Id{5, 8, 0}},
			sqlmock.RowsFromCSVString([]string{"id", "label", "parent", "children"}, ""),
			&Node{Id: Id(7), Label: "label", Parent: Id(2), Children: []Id{5, 8, 0}},
			ErrUnknownId},
		{Node{Label: "label", Parent: Id(0), Children: []Id{5, 8, 0}},
			nil,
			&Node{Label: "label", Parent: Id(0), Children: []Id{5, 8, 0}},
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
		id      Id
		sqlResp driver.Result
		err     error
	}{
		{Id(3), sqlmock.NewResult(0, 1), nil},
		{Id(8), sqlmock.NewResult(0, 0), ErrUnknownId},
		{Id(1), sqlmock.NewErrorResult(ErrTest), ErrTest},
		{Id(0), nil, ErrUnknownId},
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
 * internal functions
 */
func TestExponentialRetry(t *testing.T) {
	i := 0
	abtChan := make(chan chan<- error)
	errChan := make(chan error)
	var ok bool
	go func() {
		ok = exponentialRetry(abtChan, func() error {
			i++
			return ErrTest
		})
	}()
	time.Sleep(time.Second)
	abtChan <- errChan
	err := <-errChan
	if err != ErrTest {
		t.Errorf("Unexpected Error when testing exponentialRetry: %s", err)
	}
	if i != 7 {
		t.Errorf("Unexpected Number (%s) of retries within %T when testing exponentialRetry", i, time.Second)
	}
	if ok {
		t.Errorf("Unexpected Return Value %t of abortet exponentialRetry", ok)
	}
	i = 0
	ok = exponentialRetry(abtChan, func() error {
		i++
		if i < 3 {
			return ErrTest
		}
		return nil
	})
	if i != 3 {
		t.Errorf("Unexpected Number (%s) of retries when suceeding after 3 retries when testing exponentialRetry", i)
	}
	if !ok {
		t.Errorf("Unexpected Return Value %t of suceeded exponentialRetry", ok)
	}
}

func TestInParameter(t *testing.T) {
	tests := []struct {
		prefix         string
		values         interface{}
		parameter      map[string]interface{}
		parameterAfter map[string]interface{}
		result         string
	}{
		{"prefix", []int{1, 2, 3}, make(map[string]interface{}), map[string]interface{}{"prefix0": 1, "prefix1": 2, "prefix2": 3}, "(:prefix0, :prefix1, :prefix2) "},
		{"prefix", []int{}, make(map[string]interface{}), map[string]interface{}{}, ""},
		{"prefix2", []string{"H", "B"}, make(map[string]interface{}), map[string]interface{}{"prefix20": "H", "prefix21": "B"}, "(:prefix20, :prefix21) "},
		{"prefix", []string{"H"}, make(map[string]interface{}), map[string]interface{}{"prefix0": "H"}, "(:prefix0) "},
	}
	for testId, test := range tests {
		res := inParameter(test.prefix, test.values, test.parameter)
		if res != test.result {
			t.Errorf("Test Case %d: Expected %s to be %s, when testing inParameter().", testId, res, test.result)
		}
		if !compareStringMap(test.parameter, test.parameterAfter) {
			t.Errorf("Test Case %d: Expected %#v to be %#v, when testing inParameter().", testId, test.parameter, test.parameterAfter)
		}
	}
}

/**
 * test helper functions
 */
func compareNode(a *Node, b *Node) bool {
	if a == nil || b == nil && a != b {
		return false
	}
	return a.Id == b.Id && a.Label == b.Label && a.Parent == b.Parent && compareIdSlice(a.Children, b.Children)
}

func compareIdSlice(a []Id, b []Id) (equal bool) {
	equal = len(a) == len(b)
	if !equal || a == nil || b == nil {
		return
	}
	i := 0
	for equal {
		if i == len(a) {
			break
		}
		equal = a[i] == b[i]
		i++
	}
	return
}

func compareStringMap(a map[string]interface{}, b map[string]interface{}) (equal bool) {
	equal = len(a) == len(b)
	if !equal || a == nil || b == nil {
		return
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok {
			return false
		}
		if va != vb {
			return false
		}
	}
	return
}

func sqlmockExpectVersion(prefix string, version uint64) {
	sqlmock.ExpectQuery("SELECT 1 FROM pg_proc WHERE proname = '" + prefix + "_version';").WillReturnRows(sqlmock.NewRows([]string{"row"}).AddRow(""))
	sqlmock.ExpectQuery("SELECT " + prefix + "_version\\(\\);").WillReturnRows(sqlmock.NewRows([]string{"row"}).AddRow(version))
}

func sqlmockExpectCreateSchema(prefix string) {
	sqlmock.ExpectBegin()
	sqlmock.ExpectExec(".*? CREATE FUNCTION " + prefix + "_version\\(\\) RETURNS bigint AS 'SELECT CAST\\(1 AS bigint\\);' LANGUAGE SQL IMMUTABLE;").WillReturnResult(sqlmock.NewResult(1, 1))
	sqlmock.ExpectCommit()
}

func newTestDB(t *testing.T, prefix string) (db *DB) {
	dbx := setUpTestDB(t)
	db = &DB{DB: dbx, prefix: prefix}
	return
}

func setUpTestDB(t *testing.T) (db *sqlx.DB) {
	mdb, err := sqlmock.New()
	if err != nil {
		t.Fatalf("Creation of DB-mock schould succeed, but got error %s", err)
	}
	db = sqlx.NewDb(mdb, "postgres")
	return
}

func closeDb(t *testing.T, db *sqlx.DB) {
	err := db.Close()
	if err != nil {
		t.Errorf("All expected statments should be called, but %s", err)
	}
}
