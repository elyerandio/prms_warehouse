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
	_ "github.com/go-sql-driver/mysql"
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
	userAS := "APC"
	pwd := "prmsowner"

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
	mysqlIP := "172.20.0.39"
	user := "edpdev"
	pwd = "edpdev777"

	// dbMysql, err = sqlx.Open("mysql", "root:justdoit@/prms?charset=utf8&parseTime=True&loc=Local")
	// dbMysql, err = sqlx.Open("mysql", "edpdev:edpdev777@tcp(172.20.0.39:3306)/prms_ap?charset=utf8&parseTime=True&loc=Local")
	mysqlConnectStr := fmt.Sprintf("%s:%s@tcp(%s:3306)/prms_%s?charset=utf8&parseTime=True&loc=Local",
		user, pwd, mysqlIP, year)
	dbMysql, err := sql.Open("mysql", mysqlConnectStr)
	if err != nil {
		panic(err)
	}
	err = dbMysql.Ping()
	if err != nil {
		panic(err)
	}
	dbMysql.Close()
	fmt.Println()
	log.Println("Connected to MySQL (172.20.0.39)")

	// call program to create customer table
	cmd := exec.Command("./prms_customer.exe", odbcConnectStr, mysqlConnectStr, processCnt)
	cmd.Stdout = os.Stdout
	err = cmd.Run()

	if err != nil {
		panic(err)
	}

	// call program to create vendor table
	cmd = exec.Command("./prms_vendor.exe", odbcConnectStr, mysqlConnectStr, processCnt)
	cmd.Stdout = os.Stdout
	err = cmd.Run()

	if err != nil {
		panic(err)
	}

	// call program to create ap detail table
	cmd = exec.Command("./prms_apdetail.exe", odbcConnectStr, mysqlConnectStr, year, processCnt)
	cmd.Stdout = os.Stdout
	err = cmd.Run()

	if err != nil {
		panic(err)
	}

	// call program to create ap header table
	cmd = exec.Command("./prms_apheader.exe", odbcConnectStr, mysqlConnectStr, year, processCnt)

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
