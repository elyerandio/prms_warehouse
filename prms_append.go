package main

// prms_append.go
// To compile : go build -o prms_append.exe prms_append.go customer_append.go vendor_append.go apdetail_append.go apheader_append.go

import (
	"bufio"
	// "database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"strings"
	"syscall"
	"time"

	"gopkg.in/ini.v1"

	_ "github.com/alexbrainman/odbc"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/term"
)

var dbOdbc *sqlx.DB
var dbPostgre *sqlx.DB

func main() {
	timeStart := time.Now()

	// opens logfile prms_append.log
	f, err := os.OpenFile("prms_append.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("error opening logfile :%v", err)
	}
	defer f.Close()
	logfile := log.New(f, "", log.Ldate|log.Ltime)

	logfile.Printf("Starting prms_append")

	dsn := "PRMS"
	fmt.Println("Server   :", dsn)
	userAS, pwd, err := getCredentials()

	// connect to AS400
	odbcConnectStr := fmt.Sprintf("DSN=%s; UID=%s; PWD=%s", dsn, userAS, pwd)
	dbOdbc, err := sqlx.Open("odbc", odbcConnectStr)
	if err != nil {
		logfile.Println(err)
		panic(err)
	}
	err = dbOdbc.Ping()
	if err != nil {
		logfile.Println(err)
		panic(err)
	}
	defer dbOdbc.Close()
	log.Println("Connected to", dsn)

	pqConnectStr, dbname := readIni()
	dbPostgre, err := sqlx.Open("postgres", pqConnectStr)
	if err != nil {
		logfile.Println(err)
		panic(err)
	}
	err = dbPostgre.Ping()
	if err != nil {
		logfile.Println(err)
		panic(err)
	}
	defer dbPostgre.Close()
	fmt.Println()
	log.Println("Connected to PostgreSQL (172.20.0.39)")

	// call function to update customer table
	appendCustomerTable(dbOdbc, dbPostgre, dbname, logfile)

	// call function to update vendor table
	appendVendorTable(dbOdbc, dbPostgre, dbname, logfile)

	// call function to update ap detail table
	updateAPDetailTable(dbOdbc, dbPostgre, dbname, logfile)

	// call function to update ap header table
	updateAPHeaderTable(dbOdbc, dbPostgre, dbname, logfile)

	fmt.Println()
	log.Printf("Process done!\n")
	fmt.Printf("Elapsed Time : %v\n", time.Since(timeStart))
	logfile.Printf("Process done!\n")
	logfile.Printf("Elapsed Time : %v\n\n", time.Since(timeStart))
	fmt.Printf("\nPress ENTER to continue...")
	fmt.Scanln()
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

// readIni() - reads the ini file in the remote addr 172.20.0.39:/home/edpdev/prms/dp_upload.ini
// and returns the postgre connection string to the PostgreSQL db to update and dbname
func readIni() (string, string) {

	// dp_upload.ini is in the Linux server 172.20.0.39
	// cpRemoteIni() copies the remote ini file to local pc to be read by the program
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

	ip := ""
	dbname := ""
	user := ""
	pwd := ""
	// read all PostgreSQL sections
	for i := 1; i < 20; i++ {
		sectionName := fmt.Sprintf("PostgreSQL%d", i)
		if cfg.HasSection(sectionName) {
			section := cfg.Section(sectionName)
			ip = strings.TrimSpace(section.Key("server_ip").String())
			dbname = strings.TrimSpace(section.Key("db_name").String())
			user = strings.TrimSpace(section.Key("user").String())
			pwd = strings.TrimSpace(section.Key("pwd").String())

		}
	}

	// set dbPostgre to the postgre database that will be updated (the last PostgreSQL section)
	connString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		ip, 5432, user, pwd, dbname)

	return connString, dbname
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

func fieldsUpdate(fields []string) string {
	var result string

	for i, s := range fields {
		result += fmt.Sprintf("%s=:%s", s, s)
		if i != len(fields)-1 {
			result += ", "
		}
	}

	return result
}
