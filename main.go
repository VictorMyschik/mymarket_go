package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/xuri/excelize/v2"
	"strings"
)

const (
	host     = "localhost"
	port     = 5435
	user     = "mymarket"
	password = "root"
	dbname   = "mymarket"
)

func main() {
	db := connectDB()
	defer db.Close()
	custom(db)
	//run(db)
}

func connectDB() *sql.DB {
	creds := fmt.Sprintf("host= %s port= %d user= %s password= %s dbname= %s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", creds)
	if err != nil {
		panic(err)
	}

	return db
}

func custom(db *sql.DB) {
	//var list string = "triovist_ooo-2021-06-20-13-37"
	var list string = "Sheet1"
	file, err := excelize.OpenFile("test.xlsx")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() {
		if err := file.Close(); err != nil {
			fmt.Println(err)
		}
	}()

	rows, err := file.Rows(list)
	if err != nil {
		return
	}
	specification_row := [27]string{
		"SpecificationID",
		"A",
		"B",
		"C",
		"D",
		"E",
		"F",
		"G",
		"H",
		"I",
		"J",
		"K",
		"L",
		"M",
		"N",
		"O",
		"P",
		"Q",
		"R",
		"S",
		"T",
		"U",
		"V",
		"W",
		"X",
		"Y",
		"Z",
	}
	results, cur, max := make([][]string, 0, 64), 0, 0
	var args string
	var blockArgs string
	var header string
	// Header
	for column := range specification_row {
		header = header + "\"" + specification_row[column] + "\", "
	}
	header = strings.TrimSuffix(header, ", ")

	for rows.Next() {
		cur++
		row, err := rows.Columns()
		if err != nil {
			break
		}
		results = append(results, row)

		for key := range specification_row {
			if key == 0 {
				args = "(399, "
			} else if key <= len(row)-1 {
				args = args + "'" + row[key] + "'" + ", "
			} else {
				args = args + "null" + ", "
			}
		}

		args = strings.TrimSuffix(args, ", ")
		args = args + "),"
		blockArgs = blockArgs + args

		if cur == 1300 {
			insert(header, blockArgs, db)
			blockArgs = ""
			cur = 0
		}
	}

	insert(header, blockArgs, db)

	fmt.Println(max)
}

func insert(header string, blockArgs string, db *sql.DB) {
	blockArgs = strings.TrimSuffix(blockArgs, ",")
	var query string = `INSERT INTO mr.mr_specification_row (` + header + `) VALUES ` + blockArgs
	go db.Exec(query)
}
