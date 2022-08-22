package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/360EntSecGroup-Skylar/excelize"
	_ "github.com/alexbrainman/odbc"
	_ "github.com/lib/pq"
	"golang.org/x/term"
)

type Invoice struct {
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

func main() {
	dsn := getInput("AS400 Server DSN : ")
	userAS, pwd, err := getCredentials()
	if err != nil {
		panic(err)
	}

	// connect to AS400
	odbcConnectStr := fmt.Sprintf("DSN=%s; UID=%s; PWD=%s", dsn, userAS, pwd)
	dbOdbc, err := sql.Open("odbc", odbcConnectStr)
	// dbOdbc, err := sql.Open("odbc", fmt.Sprintf("DSN=%s; UID=%s; PWD=%s", "MDC", "APC", "APPS7OWNER"))
	if err != nil {
		panic(err)
	}
	err = dbOdbc.Ping()
	if err != nil {
		panic(err)
	}
	defer dbOdbc.Close()

	// get credentials and connect to Postgre
	postgreIP := getInput("\nPostgre Server IP : ")
	user, pwd, err := getCredentials()
	if err != nil {
		panic(err)
	}

	pqConnectStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		// "localhost", 5432, "postgres", "justdoit", "prms")
		postgreIP, 5432, user, pwd, "prms")
	dbPostgres, err := sql.Open("postgres", pqConnectStr)
	if err != nil {
		panic(err)
	}
	err = dbPostgres.Ping()
	if err != nil {
		panic(err)
	}
	dbPostgres.Close()

	inputFile := getInput("\nFile to upload : ")
	if strings.HasSuffix(strings.ToLower(inputFile), ".xlsx") || strings.HasSuffix(strings.ToLower(inputFile), ".xls") {
		uploadExcel(dbPostgres, inputFile)
	}
}

func getInput(msg string) string {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print(msg)
	scanner.Scan()
	return strings.TrimSpace(scanner.Text())
}

func getCredentials() (string, string, error) {
	reader := bufio.NewReader(os.Stdin)

	fmt.Print("Username : ")
	user, err := reader.ReadString('\n')
	if err != nil {
		return "", "", err
	}

	fmt.Print("Password : ")
	bytePwd, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return "", "", err
	}
	fmt.Println()
	pwd := string(bytePwd)

	return strings.TrimSpace(user), strings.TrimSpace(pwd), nil
}

func uploadExcel(db *sql.DB, filename string) {
	f, err := excelize.OpenFile(filename)
	if err != nil {
		panic(err)
	}

	rows, err := f.Rows("Sheet1")
	if err != nil {
		panic(err)
	}

	for rows.Next() {
		row := rows.Columns()

		vendorNum := row[0]
		invoiceNum := row[1]
		amount := row[2]
		customerNum := row[3]
		invoiceDate := row[4]

		fmt.Printf("Vendor# : %s\tInvoice # : %s\tAmount : %s\tCustomer# : %s\tInvoice Date : %s\n",
			vendorNum, invoiceNum, amount, customerNum, invoiceDate)
	}
}
