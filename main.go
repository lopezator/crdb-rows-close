package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"

	_ "github.com/jackc/pgx/v4/stdlib" // force psql driver import
)

func main() {
	// connect to the DB
	db, err := sql.Open("pgx", "postgres://root@localhost:26257/defaultdb?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	// create table and populate a single row
	ctx := context.Background()
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS my_foo_bar_test"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE my_foo_bar_test(first_name string, last_name string)"); err != nil {
		log.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "INSERT INTO my_foo_bar_test(first_name, last_name) VALUES ('foo', 'bar')"); err != nil {
		log.Fatal(err)
	}

	// list on previously created table
	query := fmt.Sprintf("EXPERIMENTAL CHANGEFEED FOR my_foo_bar_test WITH UPDATED, DIFF")
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		log.Fatal(err)
	}
	if err := loopChangefeed(rows); err != nil {
		log.Fatal(err)
	}
}

func loopChangefeed(rows *sql.Rows) error {
	// this rows.Close blocks forever
	defer rows.Close()

	for rows.Next() {
		row := &struct {
			Key   string
			Table string
			Value string
		}{}
		if err := rows.Scan(&row.Table, &row.Key, &row.Value); err != nil {
			log.Fatal(err)
		}
		fmt.Println("key: ", row.Key)
		fmt.Println("table: ", row.Table)
		fmt.Println("value: ", row.Value)

		// after this err, defer gets executed, and blocks forever
		return errors.New("i'm gonna err here")
	}
	return nil
}

