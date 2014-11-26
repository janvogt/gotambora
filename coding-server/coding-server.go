package main

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/janvogt/gotambora/coding"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/vharitonsky/iniflags"
	"log"
	"net/http"
	"time"
)

var (
	dburl    = flag.String("dburl", "", "URL to the database in the form postgres://username:password@host/dbname?parameter=value...")
	port     = flag.Int("port", 80, "Port to listen on.")
	dbprefix = flag.String("dbprefix", "coding", "Use this prefix for all tables.")
	cleandb  = flag.Bool("cleandb", false, "Deletes everyting written to the DB and exit.")
)

func main() {
	iniflags.Parse()
	if *dburl == "" {
		log.Fatal("No data source name set. Please set the --dburl flag appropriately.")
	}
	db, err := sqlx.Connect("postgres", *dburl)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	cdb, err := coding.NewDB(db, *dbprefix)
	if err != nil {
		log.Fatal(err)
	}
	if *cleandb {
		if err := cdb.Clean(); err != nil {
			log.Fatal(err)
		}
		return
	}
	// http.HandleFunc("/", makeDbHandler(testHandler, db))
	http.HandleFunc("/", coding.Handler(cdb))
	log.Printf("tambora-coding starting to listen on localhost:%d ...", *port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), nil); err != nil {
		log.Fatal(err)
	}
}

func makeDbHandler(dbHandler func(http.ResponseWriter, *http.Request, *sql.DB), db *sql.DB) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		dbHandler(rw, req, db)
	}
}

func testHandler(rw http.ResponseWriter, req *http.Request, db *sql.DB) {
	t0 := time.Now()
	rows, err := db.Query("SELECT name.id as id, name, json_agg(event.id) as events FROM name JOIN event ON event.name_id = name.id GROUP BY name.id;")
	t1 := time.Now()
	if err != nil {
		http.Error(rw, err.Error(), 500)
		return
	}
	fmt.Fprintf(rw, "The call took %v to run.\n", t1.Sub(t0))
	defer rows.Close()
	var rowstructs []struct {
		name   string
		events []uint64
		id     uint64
	}
	var arr []byte
	var rowstruct struct {
		name   string
		events []uint64
		id     uint64
	}
	for rows.Next() {
		err := rows.Scan(&rowstruct.id, &rowstruct.name, &arr)
		if err != nil {
			http.Error(rw, err.Error(), 500)
			return
		}
		json.Unmarshal(arr, &rowstruct.events)
		rowstructs = append(rowstructs, rowstruct)
	}
	t2 := time.Now()
	fmt.Fprintf(rw, "The call took %v to run.\n", t2.Sub(t1))
	fmt.Fprintln(rw, "Names:")
	for _, row := range rowstructs {
		fmt.Fprintf(rw, "%v\n", row)
	}
	t3 := time.Now()
	fmt.Fprintf(rw, "The call took %v to run.\n", t3.Sub(t2))
}
