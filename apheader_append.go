package main

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	_ "github.com/alexbrainman/odbc"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	// _ "github.com/lib/pq"
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
	INDSC string    `db:"INDSC"`
	VCHNO int       `db:"VCHNO"`
	OPERD int       `db:"OPERD"`
	BNACT string    `db:"BNACT"`
	APAGL float64   `db:"APAGL"`
	ADAMT float64   `db:"ADAMT"`
	FADAM float64   `db:"FADAM"`
	REMAN float64   `db:"REMAN"`
	FREMN float64   `db:"FREMN"`
	DISCA float64   `db:"DISCA"`
	FDSCA float64   `db:"FDSCA"`
	TDSCT float64   `db:"TDSCT"`
	FTDST float64   `db:"FTDST"`
	TDSCA float64   `db:"TDSCA"`
	FTDSA float64   `db:"FTDSA"`
	DISCP float64   `db:"DISCP"`
	PURCH int       `db:"PURCH"`
	RECVN int       `db:"RECV#"`
	APIDT time.Time `db:"APIDT"`
	APDDT time.Time `db:"APDDT"`
	APCDT time.Time `db:"APCDT"`
	NXSEQ int       `db:"NXSEQ"`
	APLDT time.Time `db:"APLDT"`
	IVENT string    `db:"IVENT"`
	PYHLD string    `db:"PYHLD"`
	PYNXT string    `db:"PYNXT"`
	AFDSC string    `db:"AFDSC"`
	ASDSC string    `db:"ASDSC"`
	ICOMP string    `db:"ICOMP"`
	COMPL string    `db:"COMPL"`
	A1CUR string    `db:"A1CUR"`
	A1OER float64   `db:"A1OER"`
	APMER string    `db:"APMER"`
	A1CER float64   `db:"A1CER"`
	APUGA float64   `db:"APUGA"`
	APRGA float64   `db:"APRGA"`
	INRSN string    `db:"INRSN"`
	TDSCL float64   `db:"TDSCL"`
	FTDSL float64   `db:"FTDSL"`
	ORGIN float64   `db:"ORGIN"`
	FORIN float64   `db:"FORIN"`
	SLPAY string    `db:"SLPAY"`
	VALPH string    `db:"VALPH"`
	A1TXA float64   `db:"A1TXA"`
	A1TXR float64   `db:"A1TXR"`
	A1TGA float64   `db:"A1TGA"`
	A1GPF string    `db:"A1GPF"`
	A1PVT string    `db:"A1PVT"`
	A1FTX float64   `db:"A1FTX"`
	A1FGA float64   `db:"A1FGA"`
	A1PYC string    `db:"A1PYC"`
	A1SPC string    `db:"A1SPC"`
	A1DDT time.Time `db:"A1DDT"`
	A1DPF string    `db:"A1DPF"`
	A1TXP string    `db:"A1TXP"`
	A1ASC string    `db:"A1ASC"`
	A1REL int       `db:"A1REL"`
	APUGN int       `db:"APUGN"`
	APULS int       `db:"APULS"`
	APRGN int       `db:"APRGN"`
	APRLS int       `db:"APRLS"`
	APIGL int       `db:"APIGL"`
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
	INDSC string    `db:"INDSC"`
	VCHNO int       `db:"VCHNO"`
	OPERD int       `db:"OPERD"`
	BNACT string    `db:"BNACT"`
	APAGL float64   `db:"APAGL"`
	ADAMT float64   `db:"ADAMT"`
	FADAM float64   `db:"FADAM"`
	REMAN float64   `db:"REMAN"`
	FREMN float64   `db:"FREMN"`
	DISCA float64   `db:"DISCA"`
	FDSCA float64   `db:"FDSCA"`
	TDSCT float64   `db:"TDSCT"`
	FTDST float64   `db:"FTDST"`
	TDSCA float64   `db:"TDSCA"`
	FTDSA float64   `db:"FTDSA"`
	DISCP float64   `db:"DISCP"`
	PURCH int       `db:"PURCH"`
	RECVN int       `db:"RECVN"`
	APIDT time.Time `db:"APIDT"`
	APDDT time.Time `db:"APDDT"`
	APCDT time.Time `db:"APCDT"`
	NXSEQ int       `db:"NXSEQ"`
	APLDT time.Time `db:"APLDT"`
	IVENT string    `db:"IVENT"`
	PYHLD string    `db:"PYHLD"`
	PYNXT string    `db:"PYNXT"`
	AFDSC string    `db:"AFDSC"`
	ASDSC string    `db:"ASDSC"`
	ICOMP string    `db:"ICOMP"`
	COMPL string    `db:"COMPL"`
	A1CUR string    `db:"A1CUR"`
	A1OER float64   `db:"A1OER"`
	APMER string    `db:"APMER"`
	A1CER float64   `db:"A1CER"`
	APUGA float64   `db:"APUGA"`
	APRGA float64   `db:"APRGA"`
	INRSN string    `db:"INRSN"`
	TDSCL float64   `db:"TDSCL"`
	FTDSL float64   `db:"FTDSL"`
	ORGIN float64   `db:"ORGIN"`
	FORIN float64   `db:"FORIN"`
	SLPAY string    `db:"SLPAY"`
	VALPH string    `db:"VALPH"`
	A1TXA float64   `db:"A1TXA"`
	A1TXR float64   `db:"A1TXR"`
	A1TGA float64   `db:"A1TGA"`
	A1GPF string    `db:"A1GPF"`
	A1PVT string    `db:"A1PVT"`
	A1FTX float64   `db:"A1FTX"`
	A1FGA float64   `db:"A1FGA"`
	A1PYC string    `db:"A1PYC"`
	A1SPC string    `db:"A1SPC"`
	A1DDT time.Time `db:"A1DDT"`
	A1DPF string    `db:"A1DPF"`
	A1TXP string    `db:"A1TXP"`
	A1ASC string    `db:"A1ASC"`
	A1REL int       `db:"A1REL"`
	APUGN int       `db:"APUGN"`
	APULS int       `db:"APULS"`
	APRGN int       `db:"APRGN"`
	APRLS int       `db:"APRLS"`
	APIGL int       `db:"APIGL"`
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
		fmt.Println("apheader_update - argument count less than 3")
		panic("Argument count less than 3")
	} else {
		odbcConnectStr = os.Args[1]
		pqConnectStr = os.Args[2]
		dbName = os.Args[3]
	}

	dbOdbc, err = sqlx.Open("odbc", odbcConnectStr)
	if err != nil {
		fmt.Println("apheader_update - cannot connect", odbcConnectStr)
		panic(err)
	}
	defer dbOdbc.Close()

	dbPostgres, err = sqlx.Open("postgres", pqConnectStr)
	if err != nil {
		fmt.Println("apheader_update - cannot connect", pqConnectStr)
		panic(err)
	}
	defer dbPostgres.Close()

	updateAPHeaderTable(dbName)
}

func updateAPHeaderTable(dbName string) {
	var dbErr *pq.Error

	invoice := Invoice{}
	invoice2 := Invoice2{}
	fields := DBFields(Invoice{})
	fieldsCsv := fieldsCSV(fields)
	// fieldsCsvColons := fieldsCSVColons(fields)

	fields2 := DBFields(Invoice2{})
	fieldsCSV2 := fieldsCSV(fields2)
	fieldsCSVColons2 := fieldsCSVColons(fields2)

	// get the latest aptdt from apapp100 in PostgreSQL
	latestDate := time.Now()
	err := dbPostgres.QueryRow("SELECT max(aptdt) FROM apapp100").Scan(&latestDate)
	if err != nil {
		if errors.As(err, &dbErr) {
			fmt.Printf("apheader_update max(aptdt) - %s - %#v", dbErr.Code, err)
			panic(err)
		} else {
			fmt.Println("apheader_update max(aptdt)", err)
			fmt.Println(dbErr.Code)
			panic(err)
		}
	}

	fmt.Printf("\nUploading apapp100 records with transaction date of %s and newer\n", latestDate.Format("2006-01-02"))
	selectStmt := fmt.Sprintf("SELECT %s FROM RMSMDFL#.APAPP100 WHERE aptdt >= '%s'",
		fieldsCsv, latestDate.Format("2006-01-02"))

	rows, err := dbOdbc.Queryx(selectStmt)
	if err != nil {
		fmt.Println(err)
		panic(err)
	}
	defer rows.Close()

	insertStmt := fmt.Sprintf("INSERT INTO apapp100 (%s) VALUES(%s)", fieldsCSV2, fieldsCSVColons2)
	recCount := 0
	insCount := 0
	dupCount := 0

	fmt.Printf("\nDatabase Name   : %s\n", dbName)
	fmt.Printf("Table Name      : %s\n", "apapp100")
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
			if errors.As(err, &dbErr) {
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
	fmt.Printf("Append count    : %d\n", insCount)
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
