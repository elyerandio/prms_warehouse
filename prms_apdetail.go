package main

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	_ "github.com/alexbrainman/odbc"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
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
	dbOdbc    *sqlx.DB
	dbMysql   *sqlx.DB
	dateStart time.Time
	dateEnd   time.Time
)

func main() {
	var err error
	var odbcConnectStr string
	var mysqlConnectStr string

	if len(os.Args) != 5 {
		panic("Argument count less than 4")
	} else {
		odbcConnectStr = os.Args[1]
		mysqlConnectStr = os.Args[2]
		startDate := os.Args[3]
		endDate := os.Args[4]
		dateStart, dateEnd = checkDates(startDate, endDate)
	}

	dbOdbc, err = sqlx.Open("odbc", odbcConnectStr)
	if err != nil {
		panic(err)
	}
	defer dbOdbc.Close()

	// dbMysql, err = sqlx.Open("mysql", "root:justdoit@/prms?charset=utf8&parseTime=True&loc=Local")
	dbMysql, err = sqlx.Open("mysql", mysqlConnectStr)
	if err != nil {
		panic(err)
	}
	defer dbMysql.Close()

	processAPDetailTable()
}

func checkDates(startDate, endDate string) (time.Time, time.Time) {
	dateStart, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		panic(err)
	}

	dateEnd, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		panic(err)
	}

	return dateStart, dateEnd
}

func processAPDetailTable() {
	invoice := Invoice{}
	invoice2 := Invoice2{}
	fields := DBFields(Invoice{})
	fieldsCsv := fieldsCSV(fields)
	// fieldsCsvColons := fieldsCSVColons(fields)

	fields2 := DBFields(Invoice2{})
	fieldsCsv2 := fieldsCSV(fields2)
	fieldsCsvColons2 := fieldsCSVColons(fields2)

	selectStmt := fmt.Sprintf("SELECT %s FROM RMSMDFL#.APAPP200 WHERE aptdt BETWEEN '%s' AND '%s'",
		fieldsCsv, dateStart.Format("2006-01-02"), dateEnd.Format("2006-01-02"))

	rows, err := dbOdbc.Queryx(selectStmt)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	newAPDetailTable := getNewAPDetailTableName()
	createAPDetailTable(newAPDetailTable)

	insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", newAPDetailTable, fieldsCsv2,
		fieldsCsvColons2)
	recCount := 0
	fmt.Printf("\nTable Name : %s\n", newAPDetailTable)
	fmt.Printf("Record # : %8d", recCount)
	for rows.Next() {
		recCount++
		fmt.Printf("\b\b\b\b\b\b\b\b")
		fmt.Printf("%8d", recCount)
		err = rows.StructScan(&invoice)
		if err != nil {
			panic(err)
		}

		invoice2 = Invoice2(invoice)
		_, err = dbMysql.NamedExec(insertStmt, invoice2)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println()
}

func getNewAPDetailTableName() string {
	newDate := dateEnd.Format("200601")

	newTableName := "apapp200_" + newDate[:6]
	return newTableName
}

func createAPDetailTable(tblName string) {
	var dbErr *mysql.MySQLError

	stmt := `CREATE TABLE ` + tblName + `(
		ACTIV char(1),
		CMPNO int(3),
		PLTNO int(2),
		APRCD char(1),
		BNKNM int(6),
		VNDNO int(6),
		INVCN varchar(10),
		USRID varchar(10),
		AUDDT date,
		AUDTM int(6),
		ABTCH int(5),
		SEQNO int(3),
		APAMT decimal(13,2),
		FAPAM decimal(13,2),
		DISCT decimal(13,2),
		FDSCT decimal(13,2),
		APEGL decimal(15,0),
		APDGL decimal(15,0),
		CHKNB int(6),
		APTDT date,
		PRDNO varchar(15),
		COMNT varchar(20),
		MFLAG char(1),
		VOIDF char(1),
		PFLAG char(1),
		TCRCD char(3),
		TEXRT decimal(11,6),
		TRGGL decimal(15,0),
		TUGGL decimal(15,0),
		TUGLA decimal(13,2),
		TRGLA decimal(13,2),
		DUGLA decimal(13,2),
		IEAMT decimal(13,2),
		IEDSC decimal(13,2),
		A1099 int(2),
		GLCMP int(3),
		A2VTR decimal(5,3),
		A2TGA decimal(13,2),
		A2GPF char(1),
		A2PVT char(4),
		A2FGA decimal(13,2),
		A2PYC varchar(10),
		A2SPC char(1),
		A2DDT date,
		A2DPF char(1),
		A2ASC char(1),
		APUGN decimal(15,0),
		APULS decimal(15,0),
		APRGN decimal(15,0),
		APRLS decimal(15,0),
		APIGL decimal(15,0)
		) ENGINE=InnoDB DEFAULT CHARSET=latin1;
		`
	_, err := dbMysql.Exec(stmt)
	if err != nil {
		if errors.As(err, &dbErr) {
			// table already exists error
			if dbErr.Number == 1050 {
				clearTable(tblName)
			} else {
				panic(err)
			}
		} else {
			panic(err)
		}
	}
}

func clearTable(tblName string) {
	sql := fmt.Sprintf("TRUNCATE TABLE %s", tblName)
	_, err := dbMysql.Exec(sql)
	if err != nil {
		panic(err)
	}
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
