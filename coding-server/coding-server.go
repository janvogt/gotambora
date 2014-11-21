package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/janvogt/gotambora/coding"
	_ "github.com/lib/pq"
	"log"
	"net/http"
	"os"
	"time"
)

func main() {
	dbUrl := os.Getenv("GOTAMBORA_CODING_SERVER_DATABASE_URL")
	if dbUrl == "" {
		log.Fatal("No data source name set. Please set GOTAMBORA_CODING_SERVER_DATABASE_URL appropriately.")
	}
	coding.Config().DbUrl = dbUrl
	db, err := sql.Open("postgres", dbUrl)
	if err == nil {
		defer db.Close()
		err = db.Ping()
	}
	if err != nil {
		log.Fatal(err)
	}
	coding.Config().Db = db
	mp := os.Getenv("GOTAMBORA_CODING_SERVER_MIGRATIONS_PATH")
	if mp == "" {
		log.Fatal("No migration path set. Please set GOTAMBORA_CODING_SERVER_MIGRATIONS_PATH appropriately.")
	}
	coding.Config().MigrationsPath = mp
	lp := os.Getenv("GOTAMBORA_CODING_SERVER_LISTEN_PORT")
	if lp == "" {
		lp = "80"
	}
	if errs := coding.Migrate(); len(errs) > 0 {
		log.Fatal(errs)
	}
	http.HandleFunc("/", makeDbHandler(testHandler, db))
	log.Printf("tambora-coding starting to listen on localhost:%s ...", lp)
	if err := http.ListenAndServe(fmt.Sprintf(":%s", lp), nil); err != nil {
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
	fmt.Fprintf(rw, "The call took %v to run.\n", t1.Sub(t0))
	if err != nil {
		http.Error(rw, err.Error(), 500)
		return
	}
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
