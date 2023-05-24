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
	dbMysql   *sqlx.DB
	dateStart time.Time
	dateEnd   time.Time
)

func main() {
	var err error
	var odbcConnectStr string
	var mysqlConnectStr string
	var processCnt int

	if len(os.Args) != 4 {
		panic("Argument count less than 3")
	} else {
		odbcConnectStr = os.Args[1]
		mysqlConnectStr = os.Args[2]
		processCnt, _ = strconv.Atoi(os.Args[3])
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

	processVendorTable(processCnt)
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

func processVendorTable(processCnt int) {
	vendor := Vendor{}
	fields := DBFields(Vendor{})
	fieldsCSV := fieldsCSV(fields)
	fieldsCSVColons := fieldsCSVColons(fields)

	selectStmt := fmt.Sprintf("SELECT %s FROM RMSMDFL#.MSVMP100", fieldsCSV)
	rows, err := dbOdbc.Queryx(selectStmt)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	// newVendorTable := getNewVendorTableName()
	newVendorTable := "msvmp100"
	createVendorTable(newVendorTable)

	insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", newVendorTable, fieldsCSV,
		fieldsCSVColons)
	recCount := 0
	fmt.Printf("\nTable Name : %s\n", newVendorTable)
	fmt.Printf("Record # : %8d", recCount)
	c := goccm.New(processCnt)
	for rows.Next() {
		recCount++
		fmt.Printf("\b\b\b\b\b\b\b\b")
		fmt.Printf("%8d", recCount)
		err = rows.StructScan(&vendor)
		if err != nil {
			panic(err)
		}

		c.Wait()
		go func(vendor Vendor) {
			_, err = dbMysql.NamedExec(insertStmt, vendor)
			c.Done()
			if err != nil {
				fmt.Println()
				fmt.Println(err)
				panic(err)
			}
		}(vendor)
	}

	if c.RunningCount() > 0 {
		c.WaitAllDone()
	}
	fmt.Println()
}

func getNewVendorTableName() string {
	newDate := dateEnd.Format("200601")

	newTableName := "msvmp100_" + newDate[:6]
	return newTableName
}

func createVendorTable(tblName string) {
	var dbErr *mysql.MySQLError

	stmt := `CREATE TABLE ` + tblName + `(
	ACTIV char(1),
	CMPNO int(10),
	PLTNO int(10),
	VNDNO int(10),
	CRPVD int(10),
	VNAME varchar(25),
	VADD1 varchar(25),
	VADD2 varchar(25),
	VADDX varchar(25),
	VADD3 varchar(16),
	VSTAT char(2),
	VZIPC varchar(10),
	PHONE decimal(10),
	VPHOA varchar(16),
	VTLXN varchar(13),
	VFAXN varchar(16),
	VTEMP char(1),
	ALPHA varchar(6),
	BNKN1 int(10),
	VAPGL decimal(15),
	VDSGL decimal(15),
	VDPGL decimal(15),
	VMLDT date,
	TOTDU decimal(15,2),
	FTOTD decimal(15,2),
	MTDPY decimal(15,2),
	FMTDP decimal(15,2),
	PD2PY decimal(15,2),
	FP2PY decimal(15,2),
	YTDPY decimal(15,2),
	FYTDP decimal(15,2),
	PYDPY decimal(15,2),
	FPYPY decimal(15,2),
	MTDDS decimal(15,2),
	FMTDD decimal(15,2),
	PD2DS decimal(15,2),
	FP2DS decimal(15,2),
	YTDDS decimal(15,2),
	FYTDD decimal(15,2),
	PYDDS decimal(15,2),
	FPYDS decimal(15,2),
	MTDLS decimal(15,2),
	FMTLS decimal(15,2),
	PD2LS decimal(15,2),
	FP2LS decimal(15,2),
	YTDLS decimal(15,2),
	FYTLS decimal(15,2),
	PYDLS decimal(15,2),
	FPYLS decimal(15,2),
	MTDPC decimal(15,2),
	FMTPC decimal(15,2),
	PD2PC decimal(15,2),
	FP2PC decimal(15,2),
	YTDPC decimal(15,2),
	FYTPC decimal(15,2),
	PYDPC decimal(15,2),
	FPYPC decimal(15,2),
	VCMNT varchar(30),
	VRPCT decimal(5,3),
	F1099 char(1),
	FEDID varchar(11),
	YTD99 decimal(15,2),
	NXT99 decimal(15,2),
	LSTCY int(10),
	VDAYS int(10),
	NTMDY int(10),
	VCRCD char(3),
	DISCP decimal(5,3),
	DISTD int(10),
	VPHLD char(1),
	VFDSC char(1),
	VSDSC char(1),
	VDNPY char(1),
	VCONT varchar(25),
	VCAT1 char(2),
	VCAT2 char(2),
	OPDIV char(3),
	VPNXT char(1),
	RMAD1 varchar(25),
	RMAD2 varchar(25),
	RMADX varchar(25),
	RMAD3 varchar(16),
	RMSTA char(2),
	RMZIP varchar(10),
	VST01 decimal(15,2),
	VST02 decimal(15,2),
	VST03 decimal(15,2),
	VST04 decimal(15,2),
	VST05 decimal(15,2),
	VST06 decimal(15,2),
	VST07 decimal(15,2),
	VST08 decimal(15,2),
	VST09 decimal(15,2),
	VST10 decimal(15,2),
	VST11 decimal(15,2),
	VST12 decimal(15,2),
	VST13 decimal(15,2),
	FVT01 decimal(15,2),
	FVT02 decimal(15,2),
	FVT03 decimal(15,2),
	FVT04 decimal(15,2),
	FVT05 decimal(15,2),
	FVT06 decimal(15,2),
	FVT07 decimal(15,2),
	FVT08 decimal(15,2),
	FVT09 decimal(15,2),
	FVT10 decimal(15,2),
	FVT11 decimal(15,2),
	FVT12 decimal(15,2),
	FVT13 decimal(15,2),
	VSL01 decimal(15,2),
	VSL02 decimal(15,2),
	VSL03 decimal(15,2),
	VSL04 decimal(15,2),
	VSL05 decimal(15,2),
	VSL06 decimal(15,2),
	VSL07 decimal(15,2),
	VSL08 decimal(15,2),
	VSL09 decimal(15,2),
	VSL10 decimal(15,2),
	VSL11 decimal(15,2),
	VSL12 decimal(15,2),
	VSL13 decimal(15,2),
	FVL01 decimal(15,2),
	FVL02 decimal(15,2),
	FVL03 decimal(15,2),
	FVL04 decimal(15,2),
	FVL05 decimal(15,2),
	FVL06 decimal(15,2),
	FVL07 decimal(15,2),
	FVL08 decimal(15,2),
	FVL09 decimal(15,2),
	FVL10 decimal(15,2),
	FVL11 decimal(15,2),
	FVL12 decimal(15,2),
	FVL13 decimal(15,2),
	ONORV decimal(15,2),
	FONOV decimal(15,2),
	VMMIN decimal(15,2),
	VMMAX decimal(15,2),
	VMAVO decimal(15,2),
	VMIMW decimal(9,2),
	VPOHL char(1),
	VMOUT char(1),
	VMVIA varchar(10),
	VMCAR varchar(10),
	VMFRT char(1),
	VMFTD varchar(15),
	VRCBY decimal(13,2),
	VRCBM decimal(13,2),
	VRCBP decimal(13,2),
	VRCAY decimal(13,2),
	FRCAY decimal(13,2),
	VRCAM decimal(13,2),
	FRCAM decimal(13,2),
	VRCAP decimal(13,2),
	FRCAP decimal(13,2),
	VJCBY decimal(13,2),
	VJCBM decimal(13,2),
	VJCBP decimal(13,2),
	VMSCM decimal(13,2),
	VMSCY decimal(13,2),
	VMRVM decimal(13,2),
	VMRVY decimal(13,2),
	VMRWM decimal(13,2),
	VMRWY decimal(13,2),
	VNESY int(10),
	VNESM int(10),
	VNESP int(10),
	VNLSY int(10),
	VNLSM int(10),
	VNLSP int(10),
	VOTSY int(10),
	VOTSM int(10),
	VOTSP int(10),
	VPOCT varchar(25),
	VSMCT varchar(25),
	VMLPD int(10),
	VTXB1 char(1),
	VTXB2 char(1),
	VTXB3 char(1),
	VTAX1 varchar(4),
	VTAX2 varchar(4),
	VTAX3 varchar(4),
	VTXL1 varchar(15),
	VTXL2 varchar(15),
	VTXL3 varchar(15),
	V1099 int(10),
	VDDGL decimal(15),
	VDDCM varchar(20),
	VDDST varchar(8),
	VVRSN varchar(25),
	VIRSN varchar(25),
	VMSSD decimal(5,2),
	VACGL decimal(15),
	VMPYC varchar(10),
	VMAPR varchar(6),
	VMPEN decimal(15,2),
	VMFPN decimal(15,2),
	VMTXP char(1),
	WBRCD varchar(6),
	VMEDI char(1),
	VMED1 varchar(30),
	VMED2 varchar(30),
	VMED3 varchar(30),
	VMED4 decimal(15),
	VMED5 decimal(15),
	VMED6 decimal(15),
	VMDPD char(1),
	VMVSC char(1),
	VMVSE char(1),
	VMCTY char(3),
	VMSEC varchar(16),
	VMLNG char(3)
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
