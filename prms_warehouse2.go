package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	_ "github.com/alexbrainman/odbc"
	_ "github.com/lib/pq"
	"golang.org/x/term"
)

func main() {
	// dsn := getInput("AS400 Server DSN : ")
	// userAS, pwd, err := getCredentials()
	// if err != nil {
	// 	panic(err)
	// }

	var processCnt string

	flag.StringVar(&processCnt, "pcount", "10", "Number of concurrent process to use.")
	flag.Parse()

	timeStart := time.Now()

	dsn := "PRMS"
	// userAS := "APC"
	// pwd := "prmsowner"

	fmt.Println("Server   : PRMS")
	userAS, pwd, err := getCredentials()

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
	log.Println("Connected to PRMS")

	year := getInput("\nYear to copy : ")
	// get credentials and connect to Mysql
	// mysqlIP := getInput("\nMySQL Server IP : ")
	// user, pwd, err := getCredentials()
	// if err != nil {
	// 	panic(err)
	// }

	pqIP := "172.20.0.39"
	user := "edpdev"
	pwd = "edpdev777"

	// dbMysql, err = sqlx.Open("mysql", "root:justdoit@/prms?charset=utf8&parseTime=True&loc=Local")
	// dbMysql, err = sqlx.Open("mysql", "edpdev:edpdev777@tcp(172.20.0.39:3306)/prms_ap?charset=utf8&parseTime=True&loc=Local")
	pqConnectStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=prms_%s sslmode=disable",
		pqIP, "5432", user, pwd, year)
	dbPostgre, err := sql.Open("postgres", pqConnectStr)
	if err != nil {
		panic(err)
	}
	err = dbPostgre.Ping()
	if err != nil {
		panic(err)
	}
	dbPostgre.Close()
	fmt.Println()
	log.Println("Connected to PostgreSQL (172.20.0.39)")

	// call program to create customer table
	cmd := exec.Command("./prms_customer2.exe", odbcConnectStr, pqConnectStr, processCnt)
	cmd.Stdout = os.Stdout
	err = cmd.Run()

	if err != nil {
		panic(err)
	}

	// call program to create vendor table
	cmd = exec.Command("./prms_vendor2.exe", odbcConnectStr, pqConnectStr, processCnt)
	cmd.Stdout = os.Stdout
	err = cmd.Run()

	if err != nil {
		panic(err)
	}

	// call program to create ap detail table
	cmd = exec.Command("./prms_apdetail2.exe", odbcConnectStr, pqConnectStr, year, processCnt)
	cmd.Stdout = os.Stdout
	err = cmd.Run()

	if err != nil {
		panic(err)
	}

	// call program to create ap header table
	cmd = exec.Command("./prms_apheader2.exe", odbcConnectStr, pqConnectStr, year, processCnt)
	cmd.Stdout = os.Stdout
	err = cmd.Run()

	if err != nil {
		panic(err)
	}

	fmt.Println()
	log.Printf("Process done!\n")
	fmt.Printf("Elapsed Time : %v\n", time.Since(timeStart))
	fmt.Printf("Press ENTER to continue...")
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
