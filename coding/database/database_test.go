package database

import (
	"errors"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/janvogt/gotambora/coding/types"
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

func compareIdSlice(a []types.Id, b []types.Id) (equal bool) {
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
