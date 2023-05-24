package main

import (
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/alexbrainman/odbc"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/zenthangplus/goccm"
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
	dbOdbc    *sqlx.DB
	dbMysql   *sqlx.DB
	dateStart time.Time
	dateEnd   time.Time
)

func main() {
	var err error
	var odbcConnectStr string
	var mysqlConnectStr string
	var year string
	var processCnt int

	if len(os.Args) != 5 {
		panic("Argument count less than 4")
	} else {
		odbcConnectStr = os.Args[1]
		mysqlConnectStr = os.Args[2]
		year = os.Args[3]
		processCnt, _ = strconv.Atoi(os.Args[4])
	}

	dbOdbc, err = sqlx.Open("odbc", odbcConnectStr)
	if err != nil {
		panic(err)
	}
	defer dbOdbc.Close()

	// dbMysql, err = sqlx.Open("mysql", "root:justdoit@/prms?charset=utf8&parseTime=True&loc=Local")
	// dbMysql, err = sqlx.Open("mysql", "edpdev:edpdev777@tcp(172.20.0.39:3306)/prms_ap?charset=utf8&parseTime=True&loc=Local")
	dbMysql, err = sqlx.Open("mysql", mysqlConnectStr)
	if err != nil {
		panic(err)
	}
	defer dbMysql.Close()

	processAPHeaderTable(year, processCnt)
}

func processAPHeaderTable(year string, processCnt int) {
	invoice := Invoice{}
	invoice2 := Invoice2{}
	fields := DBFields(Invoice{})
	fieldsCsv := fieldsCSV(fields)
	// fieldsCsvColons := fieldsCSVColons(fields)

	fields2 := DBFields(Invoice2{})
	fieldsCSV2 := fieldsCSV(fields2)
	fieldsCSVColons2 := fieldsCSVColons(fields2)

	// selectStmt := fmt.Sprintf("SELECT %s FROM MDMOD#.APAPL100 WHERE auddt BETWEEN '%s' AND '%s'",
	// fieldsCsv, dateStart.Format("2006-01-02"), dateEnd.Format("2006-01-02"))
	selectStmt := fmt.Sprintf("SELECT %s FROM RMSMDFL#.APAPP100 WHERE YEAR(aptdt) = %s",
		fieldsCsv, year)

	rows, err := dbOdbc.Queryx(selectStmt)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	// newAPHeaderTable := getNewAPHeaderTableName()
	newAPHeaderTable := "apapp100"
	createAPHeaderTable(newAPHeaderTable)

	insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", newAPHeaderTable, fieldsCSV2,
		fieldsCSVColons2)
	recCount := 0
	fmt.Printf("\nTable Name : %s\n", newAPHeaderTable)
	fmt.Printf("Record # : %8d", recCount)
	c := goccm.New(processCnt)
	for rows.Next() {
		recCount++
		fmt.Printf("\b\b\b\b\b\b\b\b")
		fmt.Printf("%8d", recCount)
		err = rows.StructScan(&invoice)
		if err != nil {
			panic(err)
		}

		invoice2 = Invoice2(invoice)
		c.Wait()
		go func(invoice2 Invoice2) {
			_, err = dbMysql.NamedExec(insertStmt, invoice2)
			c.Done()
			if err != nil {
				fmt.Println()
				fmt.Println(err)
				panic(err)
			}
		}(invoice2)
	}

	if c.RunningCount() > 0 {
		c.WaitAllDone()
	}
	fmt.Println()
}

func getNewAPHeaderTableName() string {
	newDate := dateEnd.Format("200601")

	newTableName := "apapp100_" + newDate[:6]
	return newTableName
}

func createAPHeaderTable(tblName string) {
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
		APAMT decimal(13,2),
		FAPAM decimal(13,2),
		DISCT decimal(13,2),
		FDSCT decimal(13,2),
		APEGL decimal(15),
		APDGL decimal(15),
		CHKNB int(6),
		APTDT date,
		PRDNO varchar(15),
		COMNT varchar(20),
		MFLAG char(1),
		VOIDF char(1),
		PFLAG char(1),
		TCRCD char(3),
		TEXRT decimal(11,6),
		TRGGL decimal(15),
		TUGGL decimal(15),
		TUGLA decimal(13,2),
		TRGLA decimal(13,2),
		DUGLA decimal(13,2),
		IEAMT decimal(13,2),
		IEDSC decimal(13,2),
		A1099 int(2),
		GLCMP int(3),
		INDSC varchar(30),
		VCHNO int(6),
		OPERD int(2),
		BNACT varchar(10),
		APAGL decimal(15),
		ADAMT decimal(13,2),
		FADAM decimal(13,2),
		REMAN decimal(13,2),
		FREMN decimal(13,2),
		DISCA decimal(13,2),
		FDSCA decimal(13,2),
		TDSCT decimal(13,2),
		FTDST decimal(13,2),
		TDSCA decimal(13,2),
		FTDSA decimal(13,2),
		DISCP decimal(5,3),
		PURCH int(6),
		RECVN int(6),
		APIDT date,
		APDDT date,
		APCDT date,
		NXSEQ int(3),
		APLDT date,
		IVENT char(1),
		PYHLD char(1),
		PYNXT char(1),
		AFDSC char(1),
		ASDSC char(1),
		ICOMP char(1),
		COMPL char(1),
		A1CUR char(3),
		A1OER decimal(11,6),
		APMER char(1),
		A1CER decimal(11,6),
		APUGA decimal(13,2),
		APRGA decimal(13,2),
		INRSN varchar(25),
		TDSCL decimal(13,2),
		FTDSL decimal(13,2),
		ORGIN decimal(13,2),
		FORIN decimal(13,2),
		SLPAY char(1),
		VALPH char(6),
		A1TXA decimal(13,2),
		A1TXR decimal(5,3),
		A1TGA decimal(13,2),
		A1GPF char(1),
		A1PVT char(4),
		A1FTX decimal(13,2),
		A1FGA decimal(13,2),
		A1PYC varchar(10),
		A1SPC char(1),
		A1DDT date,
		A1DPF char(1),
		A1TXP char(1),
		A1ASC char(1),
		A1REL int(3),
		APUGN decimal(15),
		APULS decimal(15),
		APRGN decimal(15),
		APRLS decimal(15),
		APIGL decimal(15)
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
