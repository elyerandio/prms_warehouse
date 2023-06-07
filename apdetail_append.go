package main

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	_ "github.com/alexbrainman/odbc"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type Invoice struct {
	ACTIV string    `db:"ACTIV"`
	CMPNO int       `db:"CMPNO"`
	PLTNO int       `db:"PLTNO"`
	APRCD string    `db:"APRCD"`
	BNKNM int       `db:"BNKNM"`
	VNDNO int       `db:"VNDNO"`
	INVCN string    `db:"INVC#"`
	USRID string    `db:"USRID"`
	AUDDT time.Time `db:"AUDDT"`
	AUDTM int       `db:"AUDTM"`
	ABTCH int       `db:"ABTCH"`
	SEQNO int       `db:"SEQNO"`
	APAMT float64   `db:"APAMT"`
	FAPAM float64   `db:"FAPAM"`
	DISCT float64   `db:"DISCT"`
	FDSCT float64   `db:"FDSCT"`
	APEGL float64   `db:"APEGL"`
	APDGL float64   `db:"APDGL"`
	CHKNB int       `db:"CHKNB"`
	APTDT time.Time `db:"APTDT"`
	PRDNO string    `db:"PRDNO"`
	COMNT string    `db:"COMNT"`
	MFLAG string    `db:"MFLAG"`
	VOIDF string    `db:"VOIDF"`
	PFLAG string    `db:"PFLAG"`
	TCRCD string    `db:"TCRCD"`
	TEXRT float64   `db:"TEXRT"`
	TRGGL float64   `db:"TRGGL"`
	TUGGL float64   `db:"TUGGL"`
	TUGLA float64   `db:"TUGLA"`
	TRGLA float64   `db:"TRGLA"`
	DUGLA float64   `db:"DUGLA"`
	IEAMT float64   `db:"IEAMT"`
	IEDSC float64   `db:"IEDSC"`
	A1099 int       `db:"A1099"`
	GLCMP int       `db:"GLCMP"`
	A2VTR float64   `db:"A2VTR"`
	A2TGA float64   `db:"A2TGA"`
	A2GPF string    `db:"A2GPF"`
	A2PVT string    `db:"A2PVT"`
	A2FGA float64   `db:"A2FGA"`
	A2PYC string    `db:"A2PYC"`
	A2SPC string    `db:"A2SPC"`
	A2DDT time.Time `db:"A2DDT"`
	A2DPF string    `db:"A2DPF"`
	A2ASC string    `db:"A2ASC"`
	APUGN float64   `db:"APUGN"`
	APULS float64   `db:"APULS"`
	APRGN float64   `db:"APRGN"`
	APRLS float64   `db:"APRLS"`
	APIGL float64   `db:"APIGL"`
}

type Invoice2 struct {
	ACTIV string    `db:"ACTIV"`
	CMPNO int       `db:"CMPNO"`
	PLTNO int       `db:"PLTNO"`
	APRCD string    `db:"APRCD"`
	BNKNM int       `db:"BNKNM"`
	VNDNO int       `db:"VNDNO"`
	INVCN string    `db:"INVCN"`
	USRID string    `db:"USRID"`
	AUDDT time.Time `db:"AUDDT"`
	AUDTM int       `db:"AUDTM"`
	ABTCH int       `db:"ABTCH"`
	SEQNO int       `db:"SEQNO"`
	APAMT float64   `db:"APAMT"`
	FAPAM float64   `db:"FAPAM"`
	DISCT float64   `db:"DISCT"`
	FDSCT float64   `db:"FDSCT"`
	APEGL float64   `db:"APEGL"`
	APDGL float64   `db:"APDGL"`
	CHKNB int       `db:"CHKNB"`
	APTDT time.Time `db:"APTDT"`
	PRDNO string    `db:"PRDNO"`
	COMNT string    `db:"COMNT"`
	MFLAG string    `db:"MFLAG"`
	VOIDF string    `db:"VOIDF"`
	PFLAG string    `db:"PFLAG"`
	TCRCD string    `db:"TCRCD"`
	TEXRT float64   `db:"TEXRT"`
	TRGGL float64   `db:"TRGGL"`
	TUGGL float64   `db:"TUGGL"`
	TUGLA float64   `db:"TUGLA"`
	TRGLA float64   `db:"TRGLA"`
	DUGLA float64   `db:"DUGLA"`
	IEAMT float64   `db:"IEAMT"`
	IEDSC float64   `db:"IEDSC"`
	A1099 int       `db:"A1099"`
	GLCMP int       `db:"GLCMP"`
	A2VTR float64   `db:"A2VTR"`
	A2TGA float64   `db:"A2TGA"`
	A2GPF string    `db:"A2GPF"`
	A2PVT string    `db:"A2PVT"`
	A2FGA float64   `db:"A2FGA"`
	A2PYC string    `db:"A2PYC"`
	A2SPC string    `db:"A2SPC"`
	A2DDT time.Time `db:"A2DDT"`
	A2DPF string    `db:"A2DPF"`
	A2ASC string    `db:"A2ASC"`
	APUGN float64   `db:"APUGN"`
	APULS float64   `db:"APULS"`
	APRGN float64   `db:"APRGN"`
	APRLS float64   `db:"APRLS"`
	APIGL float64   `db:"APIGL"`
}

var (
	dbOdbc     *sqlx.DB
	dbPostgres *sqlx.DB
	dateStart  time.Time
	dateEnd    time.Time
)

func main() {
	var err error
	var odbcConnectStr string
	var pqConnectStr string
	var dbName string

	if len(os.Args) != 4 {
		fmt.Println("apdetail_update - argument count less than 3")
		panic("Argument count less than 3")
	} else {
		odbcConnectStr = os.Args[1]
		pqConnectStr = os.Args[2]
		dbName = os.Args[3]
	}

	dbOdbc, err = sqlx.Open("odbc", odbcConnectStr)
	if err != nil {
		fmt.Println("\napdetail_update - cannot connect", odbcConnectStr)
		panic(err)
	}
	defer dbOdbc.Close()

	dbPostgres, err = sqlx.Open("postgres", pqConnectStr)
	if err != nil {
		fmt.Println("\napdetail_update - cannot connect", pqConnectStr)
		panic(err)
	}
	defer dbPostgres.Close()

	updateAPDetailTable(dbName)
}

func updateAPDetailTable(dbName string) {
	var dbErr *pq.Error

	invoice := Invoice{}
	invoice2 := Invoice2{}
	fields := DBFields(Invoice{})
	fieldsCsv := fieldsCSV(fields)
	// fieldsCsvColons := fieldsCSVColons(fields)

	fields2 := DBFields(Invoice2{})
	fieldsCsv2 := fieldsCSV(fields2)
	fieldsCsvColons2 := fieldsCSVColons(fields2)

	// get the latest aptdt from apapp200 in PostgreSQL
	latestDate := time.Now()
	err := dbPostgres.QueryRow("SELECT max(aptdt) FROM apapp200").Scan(&latestDate)
	if err != nil {
		if errors.As(err, &dbErr) {
			fmt.Printf("apdetail_update max(aptdt) - %s - %#v", dbErr.Code, err)
			panic(err)
		} else {
			fmt.Println("apdetail_update max(aptdt)", err)
			fmt.Println(dbErr.Code)
			panic(err)
		}
	}

	fmt.Printf("\nUploading records with transaction date of %s and newer\n", latestDate.Format("2006-01-02"))
	selectStmt := fmt.Sprintf("SELECT %s FROM RMSMDFL#.APAPP200 WHERE aptdt >= '%s'",
		fieldsCsv, latestDate.Format("2006-01-02"))

	rows, err := dbOdbc.Queryx(selectStmt)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	insertStmt := fmt.Sprintf("INSERT INTO apapp200 (%s) VALUES(%s)", fieldsCsv2, fieldsCsvColons2)
	recCount := 0
	insCount := 0
	dupCount := 0

	fmt.Printf("\nDatabase Name   : %s\n", dbName)
	fmt.Printf("Table Name      : %s\n", "apapp200")
	fmt.Printf("Record #        : %8d", recCount)
	for rows.Next() {
		recCount++
		fmt.Printf("\b\b\b\b\b\b\b\b")
		fmt.Printf("%8d", recCount)
		err = rows.StructScan(&invoice)
		if err != nil {
			panic(err)
		}

		invoice2 = Invoice2(invoice)
		_, err = dbPostgres.NamedExec(insertStmt, invoice2)
		if err != nil {
			if errors.As(err, dbErr) {
				// 23505 = Unique key violation
				// ignore unique key violation, continue to the next record
				if dbErr.Code == "23505" {
					dupCount++
					continue
				} else {
					fmt.Println()
					fmt.Println(err)
					panic(err)
				}
			} else {
				fmt.Println()
				fmt.Println(err)
				panic(err)
			}
		}
		insCount++
	}

	fmt.Println()
	fmt.Printf("Uploaded count  : %d\n", insCount)
	fmt.Printf("Duplicate count : %d\n", dupCount)
}

func DBFields(values interface{}) []string {
	v := reflect.ValueOf(values)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	fields := []string{}
	if v.Kind() == reflect.Struct {
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i).Tag.Get("db")
			// fmt.Printf("i=%d\tfield=%s\n", i, field)
			if field != "" {
				fields = append(fields, field)
			}
		}
		return fields
	}
	if v.Kind() == reflect.Map {
		for _, key := range v.MapKeys() {
			fields = append(fields, key.String())
		}
		return fields
	}

	panic(fmt.Errorf("DBFields requires a struct or a map, found: %s", v.Kind().String()))
}

func fieldsCSV(fields []string) string {
	return strings.Join(fields, ", ")
}

func fieldsCSVColons(fields []string) string {
	var result string

	for i, s := range fields {
		result += fmt.Sprintf(":%s", s)
		if i != len(fields)-1 {
			result += ", "
		}
	}
	return result
}
