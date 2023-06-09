package main

import (
	"errors"
	"fmt"
	"log"
	"time"

	_ "github.com/alexbrainman/odbc"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
)

type InvoiceDetail struct {
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

type InvoiceDetail2 struct {
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

func updateAPDetailTable(dbOdbc, dbPostgre *sqlx.DB, dbName string, logfile *log.Logger) {
	var dbErr *pq.Error

	invoice := InvoiceDetail{}
	invoice2 := InvoiceDetail2{}
	fields := DBFields(InvoiceDetail{})
	fieldsCsv := fieldsCSV(fields)
	// fieldsCsvColons := fieldsCSVColons(fields)

	fields2 := DBFields(InvoiceDetail2{})
	fieldsCsv2 := fieldsCSV(fields2)
	fieldsCsvColons2 := fieldsCSVColons(fields2)

	// get the latest aptdt from apapp200 in PostgreSQL
	latestDate := time.Now()
	err := dbPostgre.QueryRow("SELECT max(aptdt) FROM apapp200").Scan(&latestDate)
	if err != nil {
		if errors.As(err, &dbErr) {
			fmt.Printf("apdetail_update max(aptdt) - %s - %#v", dbErr.Code, err)
			fmt.Println(err)
			panic(err)
		} else {
			fmt.Println("apdetail_update max(aptdt)", err)
			fmt.Println(dbErr.Code)
			fmt.Println(err)
			panic(err)
		}
	}

	fmt.Printf("\nCopying apapp200 records with transaction date of %s and newer\n", latestDate.Format("2006-01-02"))
	logfile.Printf("Copying apapp200 records with transaction date of %s and newer\n", latestDate.Format("2006-01-02"))
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

	fmt.Printf("\nDB Name         : %s\n", dbName)
	fmt.Printf("Table Name      : %s\n", "apapp200")
	fmt.Printf("Record #        : %8d", recCount)
	logfile.Printf("DB Name         : %s\n", dbName)
	logfile.Printf("Table Name      : %s\n", "apapp200")
	for rows.Next() {
		recCount++
		fmt.Printf("\b\b\b\b\b\b\b\b")
		fmt.Printf("%8d", recCount)
		err = rows.StructScan(&invoice)
		if err != nil {
			logfile.Println(err)
			panic(err)
		}

		invoice2 = InvoiceDetail2(invoice)
		_, err = dbPostgre.NamedExec(insertStmt, invoice2)
		if err != nil {
			if errors.As(err, &dbErr) {
				// 23505 = Unique key violation
				// ignore unique key violation, continue to the next record
				if dbErr.Code == "23505" {
					dupCount++
					continue
				} else {
					fmt.Println()
					fmt.Println(err)
					logfile.Println(err)
					panic(err)
				}
			} else {
				fmt.Println()
				fmt.Println(err)
				logfile.Println(err)
				panic(err)
			}
		} else {
			insCount++
		}
	}

	fmt.Println()
	fmt.Printf("Append count    : %8d\n", insCount)
	fmt.Printf("Duplicate count : %8d\n", dupCount)
	logfile.Printf("Record count    : %d\n", recCount)
	logfile.Printf("Append count    : %d\n", insCount)
	logfile.Printf("Duplicate count : %d\n\n", dupCount)
}
