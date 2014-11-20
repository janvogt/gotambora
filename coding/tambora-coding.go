package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"net/http"
	"os"
)

func main() {
	dbc := os.Getenv("TAMBORA_CODING_DATA_SOURCE_NAME")
	if dbc == "" {
		log.Fatal("No data source name set. Please set TAMBORA_CODING_DATA_SOURCE_NAME appropriately.")
	}
	db, err := sql.Open("postgres", dbc)
	if err == nil {
		defer db.Close()
		err = db.Ping()
	}
	if err != nil {
		log.Fatal(err)
	}
	lp := os.Getenv("TAMBORA_CODING_LISTEN_PORT")
	if lp == "" {
		lp = "80"
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
	rows, err := db.Query("SELECT name FROM name")
	if err != nil {
		http.Error(rw, err.Error(), 500)
	}
	defer rows.Close()
	fmt.Fprintln(rw, "Names:")
	var name string
	for rows.Next() {
		err := rows.Scan(&name)
		if err != nil {
			http.Error(rw, err.Error(), 500)
		}
		fmt.Fprintln(rw, name)
	}
}
