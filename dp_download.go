package main

import (
	"bufio"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"gopkg.in/ini.v1"
	"io/ioutil"
	"os"
	"strings"
	"time"
)

type Output struct {
	VendorNum     int
	InvoiceDate   time.Time
	InvoiceNum    string
	BranchCode    int
	InvoiceAmount float64
	DiscountTerms int
	Active        string
	HoldPayment   string
	HoldReason    string
}

type PostGre struct {
	db     *sql.DB
	ip     string
	dbname string
	user   string
	pwd    string
}

var (
	dbPostgre            *sql.DB
	pqConnections        []PostGre
	transactionDateStart time.Time
	transactionDateEnd   time.Time
	invoiceDateStart     time.Time
	invoiceDateEnd       time.Time
)

func main() {
	// read ini file
	pqConnections = []PostGre{}
	readIni()
	defer closeConnections()

	getDateParams()
	downloadData()
}

func readIni() {
	// dp_upload.ini is in the Linux server 172.20.0.39
	// cpRemoteIni() copies the remote init file to local pc to be read by the program
	iniFilename := cpRemoteIni()
	defer os.Remove(iniFilename)

	// check if ini file exists
	cfg := &ini.File{}
	if _, err := os.Stat(iniFilename); err == nil {
		cfg, err = ini.Load(iniFilename)
		if err != nil {
			panic(err)
		}
	} else {
		cfg = nil
	}

	getPostgreConnections(cfg)
}

func cpRemoteIni() string {
	remoteAddr := "172.20.0.39:22"
	remoteUsr := "edpdev"
	remotePwd := "edpdev777"
	remoteIni := "/home/edpdev/prms/dp_upload.ini"

	client, err := ssh.Dial("tcp", remoteAddr, &ssh.ClientConfig{
		User: remoteUsr,
		Auth: []ssh.AuthMethod{
			ssh.Password(remotePwd),
		},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
	})
	if err != nil {
		fmt.Println("Failed to connect to the remote server:", err)
		os.Exit(-1)
	}

	// open a SFTP session over the existing ssh connections
	sftp, err := sftp.NewClient(client)

	// open the remote ini file
	iniRemote, err := sftp.Open(remoteIni)
	if err != nil {
		fmt.Println("Failed to open the remove ini file:", err)
		os.Exit(-1)
	}
	defer iniRemote.Close()

	// create the local temporary ini file
	iniLocal, err := createTempFile()

	// copy the file
	iniRemote.WriteTo(iniLocal)
	defer iniLocal.Close()

	return iniLocal.Name()
}

func createTempFile() (*os.File, error) {
	file, err := ioutil.TempFile("", "ini")
	if err != nil {
		return nil, err
	}

	return file, nil
}

func closeConnections() {
	// close Postgre connections
	for _, pq := range pqConnections {
		pq.db.Close()
	}
}

func getPostgreConnections(cfg *ini.File) {
	// read all PostgreSQL sections
	for i := 1; i < 50; i++ {
		sectionName := fmt.Sprintf("PostgreSQL%d", i)
		if cfg.HasSection(sectionName) {
			section := cfg.Section(sectionName)
			ip := strings.TrimSpace(section.Key("server_ip").String())
			dbname := strings.TrimSpace(section.Key("db_name").String())
			user := strings.TrimSpace(section.Key("user").String())
			pwd := strings.TrimSpace(section.Key("pwd").String())

			connString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
				ip, 5432, user, pwd, dbname)

			db, err := sql.Open("postgres", connString)
			if err != nil {
				fmt.Println(err)
				panic(err)
			}

			pqConnections = append(pqConnections, PostGre{
				db:     db,
				ip:     ip,
				dbname: dbname,
				user:   user,
				pwd:    pwd,
			})
		}
	}

	// set dbPostgre to the postgre database that will be updated
	l := len(pqConnections) - 1
	dbPostgre = pqConnections[l].db
}

func getDateParams() {
	transactionDateStart = getDate("Transaction Date Start (MMDDYYYY): ")
	transactionDateEnd = getDate("Transaction Date End (MMDDYYYY) :")

	invoiceDateStart = getDate("Invoice Date Start (MMDDYYYY): ")
	invoiceDateEnd = getDate("Invoice Date End (MMDDYYYY): ")
}

func getInput(msg string) string {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print(msg)
	scanner.Scan()
	return strings.TrimSpace(scanner.Text())
}

func getDate(msg string) time.Time {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print(msg)
		scanner.Scan()
		dateStr := strings.TrimSpace(scanner.Text())

		// conver to time
		dt, err := time.Parse("01022006", dateStr)
		if err != nil {
			fmt.Printf("Invalid date!\n\n")
		} else {
			return dt
		}
	}
}

func downloadData() {
	now := time.Now()
	outfileName := now.Format("Jan 02") + ".txt"
	outfileName = strings.ToUpper(outfileName)
	outfile, err := os.Create(outfileName)
	if err != nil {
		panic(err)
	}
	defer outfile.Close()

	stmt := `SELECT vndno, apidt, invcn, vchno, reman, pyhld, inrsn FROM apapp100 WHERE
		aptdt BETWEEN $1 AND $2 AND apidt BETWEEN $3 AND $4 ORDER BY invcn`

	cnt := 0
	fmt.Printf("\nOutput File : %s\n", outfileName)
	fmt.Printf("Record # : %6d", cnt)
	for _, pq := range pqConnections {
		rows, err := pq.db.Query(stmt, transactionDateStart, transactionDateEnd, invoiceDateStart,
			invoiceDateEnd)
		if err != nil {
			panic(err)
		}
		defer rows.Close()

		o := Output{}
		for rows.Next() {
			err = rows.Scan(&o.VendorNum, &o.InvoiceDate, &o.InvoiceNum, &o.BranchCode, &o.InvoiceAmount,
				&o.HoldPayment, &o.HoldReason)
			if err != nil {
				panic(err)
			}

			if o.InvoiceAmount <= 0 && o.VendorNum <= 9999 {
				continue
			}

			cnt++
			fmt.Printf("\b\b\b\b\b\b%6d", cnt)
			getVendorFields(pq.db, &o)

			fmt.Fprintf(outfile, "%06d%s%-10.10s%06d%20.2f%03d%s%s\n", o.VendorNum,
				o.InvoiceDate.Format("01-02-2006"), o.InvoiceNum, o.BranchCode, o.InvoiceAmount,
				o.DiscountTerms, o.Active, o.HoldPayment)
		}
	}

	fmt.Println("\n\nDownload finished. Press ENTER to continue...")
	fmt.Scanln()
}

func getVendorFields(db *sql.DB, o *Output) {
	stmt := `SELECT activ, distd FROM msvmp100 WHERE vndno=$1`
	activ := ""
	distd := 0

	err := db.QueryRow(stmt, o.VendorNum).Scan(&activ, &distd)
	if err != nil {
		panic(err)
	}

	o.Active = activ
	o.DiscountTerms = distd
}
