package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	_ "modernc.org/sqlite"
	"runtime"
	"strings"
	"time"
)

func main() {
	//db, _ := connectSqliteDb("bloom")
	db, _ := connectSqliteDb(":memory:")
	db.SetMaxOpenConns(runtime.NumCPU())
	_, _ = db.Exec(`create table if not exists bloom (id integer constraint data_pk primary key autoincrement, num integer not null) strict;`)

	populate(db)
	getLength(db)

	//f, _ := os.Create("profile.pprof")
	//pprof.StartCPUProfile(f)
	//defer pprof.StopCPUProfile()

	fmt.Println()

	queries := []string{"abc", "abcd", "abcde", "abcdef", "abcdefg", "abcdefgh", "abcdefghi", "abcdefghij", "abcdefghijk", "abcdefghijkl", "abcdefghijklm", "abcdefghijklmn", "abcdefghijklmnop", "abcdefghijklmnopq", "abcdefghijklmnopqr", "abcdefghijklmnopqrs", "abcdefghijklmnopqrst", "abcdefghijklmnopqrstu", "abcdefghijklmnopqrstuv", "abcdefghijklmnopqrstuvw", "abcdefghijklmnopqrstuvwx", "abcdefghijklmnopqrstuvwxy", "abcdefghijklmnopqrstuvwxyz"}
	for _, word := range queries {

		fmt.Println(">", word)
		s := time.Now()
		res := dbSearch(Queryise(word), db)
		fmt.Println("serial", len(res), time.Since(s))
		rowAtCount = 0

		s = time.Now()
		res = dbSearch2(Queryise(word), db)
		fmt.Println("serial modified", len(res), time.Since(s))
		fmt.Println("////////////////")
	}
}

func dbSearch(queryBits []uint64, db *sql.DB) []int {
	var results []int
	var res uint64

	if len(queryBits) == 0 {
		return results
	}

	bloomLength := getLength(db)

	// we want to go through the index, stepping though each "shard"
	for i := 0; i < bloomLength; i += BloomSize {
		// preload the res with the result of the first queryBit and if it's not 0 then we continue
		// if it is 0 it means nothing can be a match so we don't need to do anything
		//res = bloomFilter[queryBits[0]+uint64(i)]
		res = getRowAt(db, queryBits[0]+uint64(i)) // have to do this first to ensure res has something before we &

		// we don't need to look at the first queryBit anymore so start at one
		// then go through each long looking to see if we keep a match anywhere
		for j := 1; j < len(queryBits); j++ {
			//res = res & bloomFilter[queryBits[j]+uint64(i)]
			res = res & getRowAt(db, queryBits[j]+uint64(i))

			// if we have 0 meaning no bits set we should bail out because there is nothing more to do here
			// as we cannot have a match even if further queryBits have something set
			if res == 0 {
				break
			}
		}

		// if we have a non 0 value that means at least one bit is set indicating a match
		// so now we need to go through each bit and work out which document it is
		if res != 0 {
			for j := 0; j < DocumentsPerBlock; j++ {
				// determine which bits are still set indicating they have all the bits
				// set for this query which means we have a potential match
				if res&(1<<j) > 0 {
					results = append(results, int(DocumentsPerBlock*(i/BloomSize)+j))
				}
			}
		}
	}

	return results
}

func dbSearch2(queryBits []uint64, db *sql.DB) []int {
	var results []int
	var res uint64

	if len(queryBits) == 0 {
		return results
	}

	bloomLength := getLength(db)

	// we want to go through the index, stepping though each "shard"
	for i := 0; i < bloomLength; i += BloomSize {
		queryRows := []uint64{}
		for j := 0; j < len(queryBits); j++ {
			queryRows = append(queryRows, queryBits[j]+uint64(i))
		}

		rows := getRowsAt(db, queryRows)
		if len(rows) != 0 { // have to do this first to ensure res has something before we &
			res = rows[0]
		}
		for ii := 1; ii < len(rows); ii++ {
			res = res & rows[ii]
			if res == 0 {
				break
			}
		}

		// if we have a non 0 value that means at least one bit is set indicating a match
		// so now we need to go through each bit and work out which document it is
		if res != 0 {
			for j := 0; j < DocumentsPerBlock; j++ {
				// determine which bits are still set indicating they have all the bits
				// set for this query which means we have a potential match
				if res&(1<<j) > 0 {
					results = append(results, int(DocumentsPerBlock*(i/BloomSize)+j))
				}
			}
		}
	}

	return results
}

var length = 0

func getLength(db *sql.DB) int {
	if length != 0 {
		return length
	}

	row := db.QueryRow(`select count(*) from bloom;`)
	var bloomLength int
	_ = row.Scan(&bloomLength)

	length = bloomLength

	return bloomLength
}

var rowAtCount = 0

func getRowAt(db *sql.DB, id uint64) uint64 {
	rowAtCount++
	var res uint64
	row := db.QueryRow(`select num from bloom where id = ?`, id)
	_ = row.Scan(&res)
	return res
}

func getRowsAt(db *sql.DB, ids []uint64) []uint64 {
	rowAtCount++
	var res []uint64

	// Convert ids slice to []interface{} to pass as variadic arguments
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}

	rows, _ := db.Query(`select num from bloom where id in (`+strings.TrimRight(strings.Repeat("?,", len(ids)), ",")+`)`, args...)
	for rows.Next() {
		var r uint64
		_ = rows.Scan(&r)
		res = append(res, r)
	}

	return res
}

func populate(db *sql.DB) {
	tx, _ := db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})

	// add some documents to index, note that its 64 documents per int
	for i := 0; i < BloomSize*1_000; i++ {
		_, err := tx.Exec(`insert into bloom (num) values (?)`, rand.Int())
		if err != nil {
			fmt.Println(err.Error())
		}

		if i%64 == 0 {
			err = tx.Commit()
			if err != nil {
				fmt.Println(err.Error())
			}
			tx, _ = db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelReadCommitted})
		}
	}
	err := tx.Commit()
	if err != nil {
		fmt.Println(err.Error())
	}
}

func connectSqliteDb(name string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	if name != ":memory:" {
		db, err = sql.Open("sqlite", fmt.Sprintf("%s.db?_busy_timeout=5000", name))
		if err != nil {
			return nil, err
		}
	}

	_, err = db.Exec(`pragma journal_mode = wal;
pragma synchronous = normal;
pragma temp_store = memory;
pragma mmap_size = 268435456;
pragma foreign_keys = on;`)

	if err != nil {
		return nil, err
	}

	return db, nil
}
