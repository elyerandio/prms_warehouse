package main

import (
	"bufio"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/term"
	"gopkg.in/ini.v1"

	_ "github.com/alexbrainman/odbc"
	"github.com/dustin/go-humanize"
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
	dbOdbc      *sqlx.DB
	dbPostgre   *sqlx.DB
	cfg         *ini.File
	invoiceYear int
)

func main() {
	// check if ini file exists
	cfg = &ini.File{}
	if _, err := os.Stat("dp_copy.ini"); err == nil {
		cfg, err = ini.Load("dp_copy.ini")
		if err != nil {
			panic(err)
		}
	} else {
		cfg = nil
	}

	connectAS()
	defer dbOdbc.Close()

	connectPostgre()
	defer dbPostgre.Close()

	// get Invoice Year to copy
	invoiceYear, _ = strconv.Atoi(getInput("\nInvoice Year : "))

	copyAPHeaderTable()
}

func connectAS() {
	var err error

	// get AS400 DSN from ini file
	dsn := ""
	if cfg != nil {
		dsn = cfg.Section("AS/400").Key("dsn").String()
		dsn = strings.TrimSpace(dsn)
	}
	if dsn != "" {
		fmt.Printf("AS400 Server DSN : %s\n", dsn)
	} else {
		dsn = getInput("AS400 Server DSN : ")
	}

	// get AS400 username from ini file
	userAS := ""
	if cfg != nil {
		userAS = cfg.Section("AS/400").Key("user").String()
		userAS = strings.TrimSpace(userAS)
	}
	if userAS != "" {
		fmt.Printf("Username : %s\n", userAS)
	} else {
		userAS = getInput("Username : ")
	}

	// get password and connect to AS400
	pwd := ""
	for {
		pwd, err = getCredentials()
		if err != nil {
			panic(err)
		}

		// connect to AS400
		odbcConnectStr := fmt.Sprintf("DSN=%s; UID=%s; PWD=%s", dsn, userAS, pwd)
		dbOdbc, err = sqlx.Open("odbc", odbcConnectStr)
		err = dbOdbc.Ping()
		if err != nil {
			fmt.Println(err)
		} else {
			break
		}
	}
}

func connectPostgre() {
	var err error

	// get PostgreSQL IP Address from ini file
	postgreIP := ""
	if cfg != nil {
		postgreIP = cfg.Section("PostgreSQL").Key("server_ip").String()
		postgreIP = strings.TrimSpace(postgreIP)
	}
	if postgreIP != "" {
		fmt.Printf("\nPostgreSQL Server IP Addr : %s\n", postgreIP)
	} else {
		postgreIP = getInput("\nPostgreSQL Server IP Addr : ")
	}

	// get PostgreSQL database name from ini file
	dbname := ""
	if cfg != nil {
		dbname = cfg.Section("PostgreSQL").Key("db_name").String()
		dbname = strings.TrimSpace(dbname)
	}
	if dbname != "" {
		fmt.Printf("Database Name : %s\n", dbname)
	} else {
		dbname = getInput("Database Name : ")
	}

	// get PostgreSQL user from ini file
	username := ""
	if cfg != nil {
		username = cfg.Section("PostgreSQL").Key("user").String()
		username = strings.TrimSpace(username)
	}
	if username != "" {
		fmt.Printf("Username : %s\n", username)
	} else {
		username = getInput("Username : ")
	}

	// get password and connect to PostgreSQL
	pwd := ""
	for {
		pwd, err = getCredentials()
		if err != nil {
			panic(err)
		}

		// connect to AS400
		connectStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			postgreIP, 5432, username, pwd, dbname)
		dbPostgre, err = sqlx.Open("postgres", connectStr)
		err = dbOdbc.Ping()
		if err != nil {
			fmt.Println(err)
		} else {
			break
		}
	}
}

func getInput(msg string) string {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print(msg)
	scanner.Scan()

	input := scanner.Text()
	return strings.TrimSpace(input)
}

func getCredentials() (string, error) {
	fmt.Print("Password : ")
	bytePwd, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return "", err
	}
	fmt.Println()
	pwd := string(bytePwd)

	return strings.TrimSpace(pwd), nil
}

func copyAPHeaderTable() {
	invoice := Invoice{}
	invoice2 := Invoice2{}
	fields := DBFields(Invoice{})
	fieldsCsv := fieldsCSV(fields)
	// fieldsCsvColons := fieldsCSVColons(fields)

	fields2 := DBFields(Invoice2{})
	fieldsCSV2 := fieldsCSV(fields2)
	fieldsCSVColons2 := fieldsCSVColons(fields2)

	selectStmt := ""
	if invoiceYear == 0 {
		selectStmt = fmt.Sprintf("SELECT %s FROM RMSMDFL#.APAPP100 WHERE vndno > 9999", fieldsCsv)
	} else {
		selectStmt = fmt.Sprintf("SELECT %s FROM RMSMDFL#.APAPP100 WHERE vndno > 9999 AND YEAR(apidt) = %d",
			fieldsCsv, invoiceYear)
	}

	rows, err := dbOdbc.Queryx(selectStmt)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", "apapp100", fieldsCSV2,
		fieldsCSVColons2)
	readCount := 0
	fmt.Printf("Record read : %13.13s", "0")
	for rows.Next() {
		readCount++
		fmt.Printf("\b\b\b\b\b\b\b\b\b\b\b\b\b")
		fmt.Printf("%13.13s", humanize.Comma(int64(readCount)))
		err = rows.StructScan(&invoice)
		if err != nil {
			panic(err)
		}

		invoice2 = Invoice2(invoice)
		_, err = dbPostgre.NamedExec(insertStmt, invoice2)
		// error code = 23505 duplicate error
		if err, ok := err.(*pq.Error); ok {
			panic(err.Code)
		}
	}
	fmt.Println()
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
