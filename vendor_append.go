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
)

type Vendor struct {
	ACTIV string    `db:"ACTIV"`
	CMPNO int       `db:"CMPNO"`
	PLTNO int       `db:"PLTNO"`
	VNDNO int       `db:"VNDNO"`
	CRPVD int       `db:"CRPVD"`
	VNAME string    `db:"VNAME"`
	VADD1 string    `db:"VADD1"`
	VADD2 string    `db:"VADD2"`
	VADDX string    `db:"VADDX"`
	VADD3 string    `db:"VADD3"`
	VSTAT string    `db:"VSTAT"`
	VZIPC string    `db:"VZIPC"`
	PHONE float64   `db:"PHONE"`
	VPHOA string    `db:"VPHOA"`
	VTLXN string    `db:"VTLXN"`
	VFAXN string    `db:"VFAXN"`
	VTEMP string    `db:"VTEMP"`
	ALPHA string    `db:"ALPHA"`
	BNKN1 int       `db:"BNKN1"`
	VAPGL float64   `db:"VAPGL"`
	VDSGL float64   `db:"VDSGL"`
	VDPGL float64   `db:"VDPGL"`
	VMLDT time.Time `db:"VMLDT"`
	TOTDU float64   `db:"TOTDU"`
	FTOTD float64   `db:"FTOTD"`
	MTDPY float64   `db:"MTDPY"`
	FMTDP float64   `db:"FMTDP"`
	PD2PY float64   `db:"PD2PY"`
	FP2PY float64   `db:"FP2PY"`
	YTDPY float64   `db:"YTDPY"`
	FYTDP float64   `db:"FYTDP"`
	PYDPY float64   `db:"PYDPY"`
	FPYPY float64   `db:"FPYPY"`
	MTDDS float64   `db:"MTDDS"`
	FMTDD float64   `db:"FMTDD"`
	PD2DS float64   `db:"PD2DS"`
	FP2DS float64   `db:"FP2DS"`
	YTDDS float64   `db:"YTDDS"`
	FYTDD float64   `db:"FYTDD"`
	PYDDS float64   `db:"PYDDS"`
	FPYDS float64   `db:"FPYDS"`
	MTDLS float64   `db:"MTDLS"`
	FMTLS float64   `db:"FMTLS"`
	PD2LS float64   `db:"PD2LS"`
	FP2LS float64   `db:"FP2LS"`
	YTDLS float64   `db:"YTDLS"`
	FYTLS float64   `db:"FYTLS"`
	PYDLS float64   `db:"PYDLS"`
	FPYLS float64   `db:"FPYLS"`
	MTDPC float64   `db:"MTDPC"`
	FMTPC float64   `db:"FMTPC"`
	PD2PC float64   `db:"PD2PC"`
	FP2PC float64   `db:"FP2PC"`
	YTDPC float64   `db:"YTDPC"`
	FYTPC float64   `db:"FYTPC"`
	PYDPC float64   `db:"PYDPC"`
	FPYPC float64   `db:"FPYPC"`
	VCMNT string    `db:"VCMNT"`
	VRPCT float64   `db:"VRPCT"`
	F1099 string    `db:"F1099"`
	FEDID string    `db:"FEDID"`
	YTD99 float64   `db:"YTD99"`
	NXT99 float64   `db:"NXT99"`
	LSTCY int       `db:"LSTCY"`
	VDAYS int       `db:"VDAYS"`
	NTMDY int       `db:"NTMDY"`
	VCRCD string    `db:"VCRCD"`
	DISCP float64   `db:"DISCP"`
	DISTD int       `db:"DISTD"`
	VPHLD string    `db:"VPHLD"`
	VFDSC string    `db:"VFDSC"`
	VSDSC string    `db:"VSDSC"`
	VDNPY string    `db:"VDNPY"`
	VCONT string    `db:"VCONT"`
	VCAT1 string    `db:"VCAT1"`
	VCAT2 string    `db:"VCAT2"`
	OPDIV string    `db:"OPDIV"`
	VPNXT string    `db:"VPNXT"`
	RMAD1 string    `db:"RMAD1"`
	RMAD2 string    `db:"RMAD2"`
	RMADX string    `db:"RMADX"`
	RMAD3 string    `db:"RMAD3"`
	RMSTA string    `db:"RMSTA"`
	RMZIP string    `db:"RMZIP"`
	VST01 float64   `db:"VST01"`
	VST02 float64   `db:"VST02"`
	VST03 float64   `db:"VST03"`
	VST04 float64   `db:"VST04"`
	VST05 float64   `db:"VST05"`
	VST06 float64   `db:"VST06"`
	VST07 float64   `db:"VST07"`
	VST08 float64   `db:"VST08"`
	VST09 float64   `db:"VST09"`
	VST10 float64   `db:"VST10"`
	VST11 float64   `db:"VST11"`
	VST12 float64   `db:"VST12"`
	VST13 float64   `db:"VST13"`
	FVT01 float64   `db:"FVT01"`
	FVT02 float64   `db:"FVT02"`
	FVT03 float64   `db:"FVT03"`
	FVT04 float64   `db:"FVT04"`
	FVT05 float64   `db:"FVT05"`
	FVT06 float64   `db:"FVT06"`
	FVT07 float64   `db:"FVT07"`
	FVT08 float64   `db:"FVT08"`
	FVT09 float64   `db:"FVT09"`
	FVT10 float64   `db:"FVT10"`
	FVT11 float64   `db:"FVT11"`
	FVT12 float64   `db:"FVT12"`
	FVT13 float64   `db:"FVT13"`
	VSL01 float64   `db:"VSL01"`
	VSL02 float64   `db:"VSL02"`
	VSL03 float64   `db:"VSL03"`
	VSL04 float64   `db:"VSL04"`
	VSL05 float64   `db:"VSL05"`
	VSL06 float64   `db:"VSL06"`
	VSL07 float64   `db:"VSL07"`
	VSL08 float64   `db:"VSL08"`
	VSL09 float64   `db:"VSL09"`
	VSL10 float64   `db:"VSL10"`
	VSL11 float64   `db:"VSL11"`
	VSL12 float64   `db:"VSL12"`
	VSL13 float64   `db:"VSL13"`
	FVL01 float64   `db:"FVL01"`
	FVL02 float64   `db:"FVL02"`
	FVL03 float64   `db:"FVL03"`
	FVL04 float64   `db:"FVL04"`
	FVL05 float64   `db:"FVL05"`
	FVL06 float64   `db:"FVL06"`
	FVL07 float64   `db:"FVL07"`
	FVL08 float64   `db:"FVL08"`
	FVL09 float64   `db:"FVL09"`
	FVL10 float64   `db:"FVL10"`
	FVL11 float64   `db:"FVL11"`
	FVL12 float64   `db:"FVL12"`
	FVL13 float64   `db:"FVL13"`
	ONORV float64   `db:"ONORV"`
	FONOV float64   `db:"FONOV"`
	VMMIN float64   `db:"VMMIN"`
	VMMAX float64   `db:"VMMAX"`
	VMAVO float64   `db:"VMAVO"`
	VMIMW float64   `db:"VMIMW"`
	VPOHL string    `db:"VPOHL"`
	VMOUT string    `db:"VMOUT"`
	VMVIA string    `db:"VMVIA"`
	VMCAR string    `db:"VMCAR"`
	VMFRT string    `db:"VMFRT"`
	VMFTD string    `db:"VMFTD"`
	VRCBY float64   `db:"VRCBY"`
	VRCBM float64   `db:"VRCBM"`
	VRCBP float64   `db:"VRCBP"`
	VRCAY float64   `db:"VRCAY"`
	FRCAY float64   `db:"FRCAY"`
	VRCAM float64   `db:"VRCAM"`
	FRCAM float64   `db:"FRCAM"`
	VRCAP float64   `db:"VRCAP"`
	FRCAP float64   `db:"FRCAP"`
	VJCBY float64   `db:"VJCBY"`
	VJCBM float64   `db:"VJCBM"`
	VJCBP float64   `db:"VJCBP"`
	VMSCM float64   `db:"VMSCM"`
	VMSCY float64   `db:"VMSCY"`
	VMRVM float64   `db:"VMRVM"`
	VMRVY float64   `db:"VMRVY"`
	VMRWM float64   `db:"VMRWM"`
	VMRWY float64   `db:"VMRWY"`
	VNESY int       `db:"VNESY"`
	VNESM int       `db:"VNESM"`
	VNESP int       `db:"VNESP"`
	VNLSY int       `db:"VNLSY"`
	VNLSM int       `db:"VNLSM"`
	VNLSP int       `db:"VNLSP"`
	VOTSY int       `db:"VOTSY"`
	VOTSM int       `db:"VOTSM"`
	VOTSP int       `db:"VOTSP"`
	VPOCT string    `db:"VPOCT"`
	VSMCT string    `db:"VSMCT"`
	VMLPD int       `db:"VMLPD"`
	VTXB1 string    `db:"VTXB1"`
	VTXB2 string    `db:"VTXB2"`
	VTXB3 string    `db:"VTXB3"`
	VTAX1 string    `db:"VTAX1"`
	VTAX2 string    `db:"VTAX2"`
	VTAX3 string    `db:"VTAX3"`
	VTXL1 string    `db:"VTXL1"`
	VTXL2 string    `db:"VTXL2"`
	VTXL3 string    `db:"VTXL3"`
	V1099 int       `db:"V1099"`
	VDDGL float64   `db:"VDDGL"`
	VDDCM string    `db:"VDDCM"`
	VDDST string    `db:"VDDST"`
	VVRSN string    `db:"VVRSN"`
	VIRSN string    `db:"VIRSN"`
	VMSSD float64   `db:"VMSSD"`
	VACGL float64   `db:"VACGL"`
	VMPYC string    `db:"VMPYC"`
	VMAPR string    `db:"VMAPR"`
	VMPEN float64   `db:"VMPEN"`
	VMFPN float64   `db:"VMFPN"`
	VMTXP string    `db:"VMTXP"`
	WBRCD string    `db:"WBRCD"`
	VMEDI string    `db:"VMEDI"`
	VMED1 string    `db:"VMED1"`
	VMED2 string    `db:"VMED2"`
	VMED3 string    `db:"VMED3"`
	VMED4 float64   `db:"VMED4"`
	VMED5 float64   `db:"VMED5"`
	VMED6 float64   `db:"VMED6"`
	VMDPD string    `db:"VMDPD"`
	VMVSC string    `db:"VMVSC"`
	VMVSE string    `db:"VMVSE"`
	VMCTY string    `db:"VMCTY"`
	VMSEC string    `db:"VMSEC"`
	VMLNG string    `db:"VMLNG"`
}

var (
	dbOdbc    *sqlx.DB
	dbPostgre *sqlx.DB
	dateStart time.Time
	dateEnd   time.Time
)

func main() {
	var err error
	var odbcConnectStr string
	var pqConnectStr string
	var dbName string

	if len(os.Args) != 4 {
		panic("Argument count less than 3")
	} else {
		odbcConnectStr = os.Args[1]
		pqConnectStr = os.Args[2]
		dbName = os.Args[3]
	}

	dbOdbc, err = sqlx.Open("odbc", odbcConnectStr)
	if err != nil {
		panic(err)
	}
	defer dbOdbc.Close()

	// dbMysql, err = sqlx.Open("mysql", "root:justdoit@/prms?charset=utf8&parseTime=True&loc=Local")
	dbPostgre, err = sqlx.Open("postgres", pqConnectStr)
	if err != nil {
		panic(err)
	}
	defer dbPostgre.Close()

	appendVendorTable(dbName)
}

func appendVendorTable(dbName string) {
	var dbError *pq.Error

	vendor := Vendor{}
	fields := DBFields(Vendor{})
	fieldsCSV := fieldsCSV(fields)
	fieldsCSVColons := fieldsCSVColons(fields)
	fieldsUpdate := fieldsUpdate(fields)

	selectStmt := fmt.Sprintf("SELECT %s FROM RMSMDFL#.MSVMP100", fieldsCSV)
	rows, err := dbOdbc.Queryx(selectStmt)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	// newVendorTable := getNewVendorTableName()
	newVendorTable := "msvmp100"

	insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", newVendorTable, fieldsCSV,
		fieldsCSVColons)
	updateStmt := fmt.Sprintf("UPDATE msvmp100 SET %s WHERE vndno=:VNDNO", fieldsUpdate)

	recCount := 0
	updateCount := 0
	insertCount := 0
	fmt.Printf("\nTable Name : %s\n", newVendorTable)
	fmt.Printf("Record # : %8d", recCount)
	for rows.Next() {
		recCount++
		fmt.Printf("\b\b\b\b\b\b\b\b")
		fmt.Printf("%8d", recCount)
		err = rows.StructScan(&vendor)
		if err != nil {
			fmt.Println()
			fmt.Println("Structscan error", err)
			panic(err)
		}

		_, err = dbPostgre.NamedExec(insertStmt, vendor)
		if err != nil {
			if errors.As(err, &dbError) {
				// 23505 = Unique key violation
				// if unique key violation, update the record
				if dbError.Code == "23505" {
					_, err = dbPostgre.NamedExec(updateStmt, vendor)
					if err != nil {
						fmt.Println()
						fmt.Println("Update error:", err)
						panic(err)
					} else {
						updateCount++
					}
				} else {
					fmt.Println()
					fmt.Println("Insert error", dbError.Code, err)
					panic(err)
				}
			} else {
				fmt.Println()
				fmt.Println("Insert error 2", err)
				panic(err)
			}
		} else {
			insertCount++
		}
	}

	fmt.Println()
	fmt.Printf("Append count : %d\n", insertCount)
	fmt.Printf("Update count : %d\n", updateCount)
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
