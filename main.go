package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"

	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	_ "github.com/pingcap/tidb/pkg/parser/test_driver"
)

type colX struct {
	TableNames []string
}

func (v *colX) Enter(in ast.Node) (ast.Node, bool) {
	if name, ok := in.(*ast.TableName); ok {
		v.TableNames = append(v.TableNames, name.Name.O)
	}
	return in, false
}

func (v *colX) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func GetTableNames(p *parser.Parser, sql string) ([]string, error) {
	stmt, _, err := p.ParseSQL(sql)
	if err != nil {
		return nil, err
	}
	x, ok := stmt[0].(*ast.SelectStmt)
	if !ok {
		return nil, fmt.Errorf("not select stmt")
	}

	tableNames := &colX{}
	x.Accept(tableNames)
	return tableNames.TableNames, nil
}

func main() {
	p := parser.New()

	f, err := os.Open("demo.csv")
	if err != nil {
		panic(err)
	}
	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		panic(err)
	}
	fmt.Println(records[1])

	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	for i, row := range records {
		tableNames, err := GetTableNames(p, row[0])
		if err != nil {
			fmt.Println(i, err)
			continue
		}
		fmt.Println(i, tableNames)
	}
}
