package main

import (
	"flag"
	"fmt"
	"github.com/janvogt/gotambora/coding"
	"github.com/janvogt/gotambora/coding/database"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/vharitonsky/iniflags"
	"log"
	"net/http"
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
	cdb, err := database.NewDB(db, *dbprefix)
	if err != nil {
		log.Fatal(err)
	}
	if *cleandb {
		if err := cdb.Clean(); err != nil {
			log.Fatal(err)
		}
		return
	}
	h, err := coding.NewHandler(cdb)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("tambora-coding starting to listen on localhost:%d ...", *port)
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), h); err != nil {
		log.Fatal(err)
	}
}
