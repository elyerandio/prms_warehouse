package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"time"

	_ "github.com/alexbrainman/odbc"
)

func main() {
	var sdat time.Time
	var edat time.Time

	// read REFXDAT file in MDMOD# library to get the Start Date & End Date
	dbOdbc, err := sql.Open("odbc", fmt.Sprintf("DSN=%s; UID=%s; PWD=%s", "MDC", "APC", "APPS7OWNER"))
	if err != nil {
		panic(err)
	}

	// retrieve the Start & End Dates
	sql := `SELECT SDAT, EDAT FROM mdmod#.refxdat`
	err = dbOdbc.QueryRow(sql).Scan(&sdat, &edat)
	if err != nil {
		panic(err)
	}

	fmt.Printf("\nStart Date : %v\n", sdat.Format("2006-01-02"))
	fmt.Printf("End Date  : %v\n", edat.Format("2006-01-02"))

	// call program to create customer table
	cmd := exec.Command("prms_customer.exe", sdat.Format("2006-01-02"), edat.Format("2006-01-02"))
	cmd.Stdout = os.Stdout
	err = cmd.Run()

	if err != nil {
		panic(err)
	}

	// call program to create vendor table
	cmd = exec.Command("prms_vendor.exe", sdat.Format("2006-01-02"), edat.Format("2006-01-02"))
	cmd.Stdout = os.Stdout
	err = cmd.Run()

	if err != nil {
		panic(err)
	}

	// call program to create ap header table
	cmd = exec.Command("prms_apheader.exe", sdat.Format("2006-01-02"), edat.Format("2006-01-02"))
	cmd.Stdout = os.Stdout
	err = cmd.Run()

	if err != nil {
		panic(err)
	}
}
