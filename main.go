package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

type Demo struct {
	Time       string
	HttpHost   string
	Schema     string
	HttpStatus string
}

type DS []Demo

type DSS []DS

var (
	tgtTime = flag.Int64("t", 1547928000, "target start time")
)

func CreateData(t int64, targetMinutes int, httpHost string) []Demo {
	var d Demo
	var ds []Demo

	d.HttpHost = httpHost

	now := time.Unix(t, 123456789)
	for i := 0; i < targetMinutes; i++ {
		now = now.Add(1 * time.Minute)
		t := now.String()
		d.Time = strings.Split(t, " +")[0] + "+" + strings.Split(t, "+")[1][:2]

		rand.Seed(time.Now().UnixNano())
		if rand.Intn(2) != 0 {
			switch rand.Intn(2) {
			case 0:
				d.Schema = "http"
			case 1:
				d.Schema = "https"
			}

			rand.Seed(time.Now().UnixNano())
			switch rand.Intn(311) {
			case 0:
				d.HttpStatus = "5xx"
			default:
				rand.Seed(time.Now().UnixNano())
				switch rand.Intn(31) {
				case 0:
					d.HttpStatus = "1xx"
				default:
					rand.Seed(time.Now().UnixNano())
					switch rand.Intn(3) {
					case 0:
						d.HttpStatus = "2xx"
					case 1:
						d.HttpStatus = "3xx"
					default:
						d.HttpStatus = "4xx"
					}
				}
			}
			ds = append(ds, d)
		}
	}

	return ds
}

func InsertToDB(db *sql.DB, ds []Demo) {
	stmt, err := db.Prepare(`INSERT INTO demo(time, http_host, schema, http_status) VALUES ($1, $2, $3, $4)`)
	defer stmt.Close()
	if err != nil {
		panic(err)
	}
	for i := range ds {
		d := ds[i]
		_, err := stmt.Exec(d.Time, d.HttpHost, d.Schema, d.HttpStatus)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("incert record finished")
}

func main() {
	flag.Parse()

	var httpHosts []string = []string{"first.com", "second.com", "third.com"}
	var ds DS
	var dss DSS
	execMin := 120

	for _, v := range httpHosts {
		ds = CreateData(*tgtTime, execMin, v)
		dss = append(dss, ds)
	}

	dbs := make([]*sql.DB, len(dss))
	for i := 0; i < len(dss); i++ {
		db, err := sql.Open("postgres", "user=postgres password=postgres dbname=tutorial sslmode=disable")
		if err != nil {
			panic(err)
		}
		dbs[i] = db
		defer dbs[i].Close()
	}

	wg := &sync.WaitGroup{}
	for i := range dbs {
		wg.Add(1)
		go func(gi int) {
			InsertToDB(dbs[gi], dss[gi])
			wg.Done()
		}(i)
	}
	wg.Wait()
}
