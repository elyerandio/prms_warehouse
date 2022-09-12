package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/ini.v1"

	_ "github.com/alexbrainman/odbc"
	"github.com/dustin/go-humanize"
	"github.com/jmoiron/sqlx"
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
	APAGL int64     `db:"APAGL"`
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

type Bank struct {
	ACTIV string  `db:"ACTIV"`
	CMPNO float64 `db:"CMPNO"`
	PLTNO float64 `db:"PLTNO"`
	B2BNK float64 `db:"B2BNK"`
	B2PYC string  `db:"B2PYC"`
	B2DES string  `db:"B2DES"`
	B2MDA string  `db:"B2MDA"`
	B2PFM string  `db:"B2PFM"`
	B2TFM string  `db:"B2TFM"`
	B2SPC string  `db:"B2SPC"`
	B2REM string  `db:"B2REM"`
	B2DOM string  `db:"B2DOM"`
	B2STS string  `db:"B2STS"`
	B2MPN float64 `db:"B2MPN"`
	B2APN float64 `db:"B2APN"`
	B2SSF string  `db:"B2SSF"`
	B2VOD float64 `db:"B2VOD"`
	B2ONE string  `db:"B2ONE"`
	B2STB string  `db:"B2STB"`
	B2ISB float64 `db:"B2ISB"`
	B2IRA float64 `db:"B2IRA"`
	B2SEQ string  `db:"B2SEQ"`
	B2DGL string  `db:"B2DGL"`
	B2P01 float64 `db:"B2P01"`
	B2P02 float64 `db:"B2P02"`
	B2P03 float64 `db:"B2P03"`
	B2P04 float64 `db:"B2P04"`
	B2P05 float64 `db:"B2P05"`
	B2P06 float64 `db:"B2P06"`
	B2P07 float64 `db:"B2P07"`
	B2P08 float64 `db:"B2P08"`
	B2P09 float64 `db:"B2P09"`
	B2P10 float64 `db:"B2P10"`
	B2P11 float64 `db:"B2P11"`
	B2P12 float64 `db:"B2P12"`
	B2P13 float64 `db:"B2P13"`
	B2D01 float64 `db:"B2D01"`
	B2D02 float64 `db:"B2D02"`
	B2D03 float64 `db:"B2D03"`
	B2D04 float64 `db:"B2D04"`
	B2D05 float64 `db:"B2D05"`
	B2D06 float64 `db:"B2D06"`
	B2D07 float64 `db:"B2D07"`
	B2D08 float64 `db:"B2D08"`
	B2D09 float64 `db:"B2D09"`
	B2D10 float64 `db:"B2D10"`
	B2D11 float64 `db:"B2D11"`
	B2D12 float64 `db:"B2D12"`
	B2D13 float64 `db:"B2D13"`
	B2MCA float64 `db:"B2MCA"`
}

type MSCURR struct {
	MOBAR string
	MPOAP string
	MBCUR string
	MBCDS string
	MUGLA string
	MRGLA string
	OVCOA string
	OVCPA string
	OVEOA string
	OVEPA string
	MEXOB string
	MEXPO string
}

type APGLAC struct {
	APACC int64
}

type INPUTREC struct {
	vendorNum  string
	invoiceNum string
}

var (
	dbPostgre     *sqlx.DB
	dbOdbc        *sqlx.DB
	userID        string
	inv           Invoice
	vnd           Vendor
	bnk           Bank
	runType       string
	errorFile     *os.File
	errorMsg      string
	errorFound    bool
	lineCount     int
	errorCount    int
	saveCount     int
	inputRec      []INPUTREC
	vendorNum     string
	invoiceNum    string
	strAmount     string
	customerNum   string
	invoiceDate   string
	currTimeStamp time.Time
	mscurr        MSCURR
	apglac        APGLAC
)

func main() {
	var err error

	// check if ini file exists
	cfg := &ini.File{}
	if _, err := os.Stat("dp_upload.ini"); err == nil {
		cfg, err = ini.Load("dp_upload.ini")
		if err != nil {
			panic(err)
		}
	} else {
		cfg = nil
	}

	// get AS400 DSN from ini file
	dsn := ""
	if cfg != nil {
		dsn = cfg.Section("AS/400").Key("dsn").String()
		dsn = strings.TrimSpace(dsn)
	}
	if dsn != "" {
		fmt.Printf("\nAS400 Server DSN : %s\n", dsn)
	} else {
		// if dsn is blank or missing in ini file
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
		// user is blank or missing in ini file
		userAS = getInput("Username : ")
	}

	pwd := ""
	// get credential & connect to AS400
	for {
		pwd, err = getCredentials(userAS)
		if err != nil {
			panic(err)
		}

		// connect to AS400
		odbcConnectStr := fmt.Sprintf("DSN=%s; UID=%s; PWD=%s", dsn, userAS, pwd)
		dbOdbc, err = sqlx.Open("odbc", odbcConnectStr)
		// dbOdbc, err := sql.Open("odbc", fmt.Sprintf("DSN=%s; UID=%s; PWD=%s", "MDC", "APC", "APPS7OWNER"))
		err = dbOdbc.Ping()
		if err != nil {
			fmt.Println(err)
		} else {
			break
		}
	}
	defer dbOdbc.Close()

	// get PostgreSQL IP address from ini file
	postgreIP := ""
	if cfg != nil {
		postgreIP = cfg.Section("PostgreSQL").Key("server_ip").String()
		postgreIP = strings.TrimSpace(postgreIP)
	}
	if postgreIP != "" {
		fmt.Printf("\nPostgreSQL IP : %s\n", postgreIP)
	} else {
		// server_ip is blank or missing in ini file
		postgreIP = getInput("\nPostgreSQL IP : ")
	}

	// get Postgre database name from ini file
	dbname := ""
	if cfg != nil {
		dbname = cfg.Section("PostgreSQL").Key("db_name").String()
		dbname = strings.TrimSpace(dbname)
	}
	if dbname == "" {
		// db_name is blank or missing in ini file
		dbname = getInput("Database Name : ")
	}

	// get Postgre user from ini file
	if cfg != nil {
		userID = cfg.Section("PostgreSQL").Key("user").String()
		userID = strings.TrimSpace(userID)
	}
	if userID != "" {
		fmt.Println("Username :", userID)
	} else {
		// user is blank or missing in ini file
		userID = getInput("Username : ")
	}

	// loop until connected to the Postgre DB successfully
	for {
		pwd, err = getCredentials(userID)
		if err != nil {
			panic(err)
		}

		pqConnectStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			// "localhost", 5432, "postgres", "justdoit", "prms")
			postgreIP, 5432, userID, pwd, dbname)
		dbPostgre, err = sqlx.Open("postgres", pqConnectStr)
		if err != nil {
			fmt.Println(err)
		} else {
			break
		}
	}
	err = dbPostgre.Ping()
	if err != nil {
		panic(err)
	}
	defer dbPostgre.Close()

	// prompt for Trial/Final Run
	runType = getInputRun()

	// initialize the input record struct array, to check for duplicate entries
	inputRec = []INPUTREC{}

	getSystemControl()
	inputFile := getInput("\nFile to upload : ")
	openErrorFile(inputFile)
	uploadCSV(inputFile)

	fmt.Printf("\nInput record count : %d\n", lineCount)
	fmt.Printf("Error count : %d\n", errorCount)
	fmt.Printf("Uploaded count : %d\n", saveCount)
}

func getInput(msg string) string {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Print(msg)
	scanner.Scan()
	return strings.TrimSpace(scanner.Text())
}

func getInputRun() string {
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println()
	for {
		fmt.Print("Trial/Final run [T/F] : ")
		scanner.Scan()
		inputStr := strings.TrimSpace(scanner.Text())
		if inputStr[0] == 'T' || inputStr[0] == 't' {
			return "Trial"
		} else if inputStr[0] == 'F' || inputStr[0] == 'f' {
			return "Final"
		}
	}
}

func getCredentials(user string) (string, error) {
	fmt.Print("Password : ")
	bytePwd, err := term.ReadPassword(int(syscall.Stdin))
	if err != nil {
		return "", err
	}
	fmt.Println()
	pwd := string(bytePwd)

	return strings.TrimSpace(pwd), nil
}

func getSystemControl() {
	stmt := `SELECT WCDTA FROM RMSMDFL#.MSWCP100 WHERE WCKEY=? AND WCPNO=? AND WCPLT=?`
	wcdta := ""

	// get APPERD
	err := dbOdbc.QueryRow(stmt, "APPERD", 777, 0).Scan(&wcdta)
	if err != nil {
		panic(err)
	}

	// get APGLAC -> Default G/L Accounts
	err = dbOdbc.QueryRow(stmt, "APGLAC", 777, 0).Scan(&wcdta)
	if err != nil {
		panic(err)
	}
	apglac = APGLAC{}
	temp := ""
	temp = wcdta[0:15]
	apglac.APACC, _ = strconv.ParseInt(temp, 10, 64)

	// get APCLOS
	err = dbOdbc.QueryRow(stmt, "APCLOS", 777, 0).Scan(&wcdta)
	if err != nil {
		panic(err)
	}

	// get MSCURR
	err = dbOdbc.QueryRow(stmt, "MSCURR", 777, 0).Scan(&wcdta)
	if err != nil {
		panic(err)
	}
	mscurr = MSCURR{}
	mscurr.MOBAR = wcdta[0:1]
	mscurr.MPOAP = wcdta[1:2]
	mscurr.MBCUR = wcdta[2:5]
}

func openErrorFile(inputFile string) {
	var err error

	// extract the filename w/o file extension
	errorFileName := strings.TrimSuffix(inputFile, filepath.Ext(inputFile))

	// add .err file extension
	errorFileName += ".err"

	// create the error file
	errorFile, err = os.Create(errorFileName)
	if err != nil {
		panic(err)
	}

	// write header to the error file
	space := " "
	fmt.Fprintf(errorFile, "%sAP Uploading Error Report%s%s\n", strings.Repeat(" ", 60),
		strings.Repeat(" ", 55), time.Now().Format("01/02/2006"))
	fmt.Fprintf(errorFile, "%s%s Run\n\n", strings.Repeat(" ", 67), runType)
	fmt.Fprintf(errorFile, "%6.6s%4.4s%s%6.6s%s%-20.20s%s%-14.14s%s%-9.9s%s%-12.12s%s\n",
		"LINE #", space, "VENDOR #", space, "INVOICE #", space, "INVOICE AMOUNT", space, "VOUCHER #",
		space, "INVOICE DATE", space, "REMARKS")
	fmt.Fprintf(errorFile, "%s\n", strings.Repeat("-", 150))

	fmt.Println("Error file : ", errorFileName)
}

func uploadCSV(filename string) {
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}

	r := csv.NewReader(f)
	if err != nil {
		panic(err)
	}

	lineCount = 0
	for {
		errorFound = false
		record, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		lineCount++

		inv = Invoice{}
		currTimeStamp = time.Now()

		// get the column data to upload
		vendorNum = strings.TrimSpace(record[0])
		invoiceNum = strings.TrimSpace(record[1])
		strAmount = strings.TrimSpace(record[2])
		customerNum = strings.TrimSpace(record[3])
		invoiceDate = strings.TrimSpace(record[4])

		vDuplicateInInputFile()
		vDuplicateInDB()

		if vVendor() {
			errorFound = true
			printError()
		} else {
			// get vendor details from AS400
			vnd, err = getVendor(vendorNum)
			if err == sql.ErrNoRows {
				errorFound = true
				errorMsg = "Vendor not found."
				printError()
			}
		}

		vInvoiceNumber()
		vAmount()
		vCustomerNumber()

		// validate Invoice Date
		if vInvoiceDate() {
			errorFound = true
			printError()
		}

		// get bank details from AS400
		bnk, err = getBank()
		if err == sql.ErrNoRows {
			errorFound = true
			errorMsg = "Bank not found"
			printError()
		}

		if runType == "Final" && !errorFound {
			saveInvoice()
		}
	}
}

func getVendor(vendorNum string) (Vendor, error) {
	vendor := Vendor{}
	fields := DBFields(Vendor{})
	fieldsCsv := fieldsCSV(fields)
	// fieldsCSVColons := fieldsCSVColons(fields)

	selectStmt := fmt.Sprintf("SELECT %s FROM RMSMDFL#.MSVMP100 WHERE vndno=?", fieldsCsv)
	err := dbOdbc.QueryRowx(selectStmt, vendorNum).StructScan(&vendor)
	if err != nil {
		if err == sql.ErrNoRows {
			return vendor, err
		}

		panic(err)
	}

	return vendor, err
}

func getBank() (Bank, error) {
	bank := Bank{}
	fields := DBFields(Bank{})
	fieldsCsv := fieldsCSV(fields)

	selectStmt := fmt.Sprintf("SELECT %s FROM RMSMDFL#.APBAP200 WHERE B2BNK=? AND B2PYC=?",
		fieldsCsv)
	err := dbOdbc.QueryRowx(selectStmt, vnd.BNKN1, vnd.VMPYC).StructScan(&bank)
	if err != nil {
		if err == sql.ErrNoRows {
			return bank, err
		}

		panic(err)
	}

	return bank, err
}

func saveInvoice() {
	amount, _ := strconv.ParseFloat(strAmount, 64)

	inv.ACTIV = "1"
	inv.CMPNO = 777
	inv.PLTNO = 0
	inv.APRCD = "I"
	inv.BNKNM = vnd.BNKN1
	inv.VNDNO, _ = strconv.Atoi(vendorNum)
	inv.INVCN = invoiceNum
	inv.USRID = userID
	inv.AUDDT = currTimeStamp
	inv.AUDTM, _ = strconv.Atoi(currTimeStamp.Format("150405"))
	inv.ABTCH = 0
	inv.APAMT = amount
	inv.FAPAM = amount
	inv.DISCT = amount * vnd.DISCP / 100
	inv.FDSCT = inv.DISCT
	inv.APEGL = vnd.VDDGL
	inv.APDGL = vnd.VDSGL
	inv.CHKNB = 0
	inv.APTDT = currTimeStamp
	inv.PRDNO = strings.Repeat(" ", 15)
	inv.COMNT = vnd.VDDCM
	inv.MFLAG = "N"
	inv.VOIDF = "N"
	inv.PFLAG = " "
	inv.TCRCD = mscurr.MBCUR
	inv.TEXRT = 1
	inv.TRGGL = 0
	inv.TUGGL = 0
	inv.TUGLA = 0
	inv.TRGLA = 0
	inv.DUGLA = 0
	inv.IEAMT = amount
	inv.IEDSC = inv.FDSCT
	inv.A1099 = vnd.V1099
	inv.GLCMP = 0
	inv.INDSC = strings.Repeat(" ", 30)
	inv.VCHNO, _ = strconv.Atoi(customerNum)
	inv.OPERD = 0
	inv.BNACT = strings.Repeat(" ", 10)
	inv.APAGL = apglac.APACC
	inv.ADAMT = 0
	inv.FADAM = 0
	inv.REMAN = inv.APAMT
	inv.FREMN = inv.FAPAM
	inv.DISCA = inv.DISCT
	inv.FDSCA = inv.DISCT
	inv.TDSCT = 0
	inv.FTDST = 0
	inv.TDSCA = 0
	inv.FTDSA = 0
	inv.DISCP = vnd.DISCP
	inv.PURCH = 0
	inv.RECVN = 0
	inv.APIDT, _ = time.Parse("01022006", invoiceDate)
	inv.APDDT = inv.APIDT.AddDate(0, 0, vnd.VDAYS)
	inv.APCDT = inv.APDDT
	inv.NXSEQ = 1
	inv.APLDT = getMonthEnd(inv.AUDDT)
	inv.IVENT = "Y"
	inv.PYHLD = vnd.VPHLD
	inv.PYNXT = vnd.VPNXT
	inv.AFDSC = vnd.VFDSC
	inv.ASDSC = vnd.VSDSC
	inv.ICOMP = " "
	inv.COMPL = " "
	inv.A1CUR = mscurr.MBCUR
	inv.A1OER = 1
	inv.APMER = "N"
	inv.A1CER = 1
	inv.APUGA = 0
	inv.APRGA = 0
	inv.INRSN = " "
	inv.TDSCL = 0
	inv.FTDSL = 0
	inv.ORGIN = amount
	inv.FORIN = amount
	inv.SLPAY = " "
	inv.VALPH = vnd.ALPHA
	inv.A1TXA = 0
	inv.A1TXR = 0
	inv.A1TGA = 0
	inv.A1GPF = " "
	inv.A1PVT = "    "
	inv.A1FTX = 0
	inv.A1FGA = 0
	inv.A1PYC = vnd.VMPYC
	inv.A1SPC = bnk.B2SPC
	// inv.A1DDT =
	inv.A1DPF = "Y"
	inv.A1TXP = vnd.VMTXP
	inv.A1ASC = " "
	inv.A1REL = 0
	inv.APUGN = 0
	inv.APULS = 0
	inv.APRGN = 0
	inv.APRLS = 0
	inv.APIGL = 0

	// save record into APAPP100 table in  Postgres DB
	fields := DBFields(Invoice{})
	fieldsCsv := fieldsCSV(fields)
	fieldsCsvColons := fieldsCSVColons(fields)
	insertStmt := fmt.Sprintf("INSERT INTO apapp100 (%s) VALUES(%s)", fieldsCsv, fieldsCsvColons)
	_, err := dbPostgre.NamedExec(insertStmt, inv)
	if err != nil {
		panic(err)
	}

	saveCount++
}

func getMonthEnd(currDate time.Time) time.Time {
	return time.Date(currDate.Year(), currDate.Month()+1, 0, 0, 0, 0, 0, time.UTC)
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

func printError() {
	errorCount++
	fmt.Fprintf(errorFile, "%05d", lineCount)
	fmt.Fprintf(errorFile, "%5.5s", " ")
	fmt.Fprintf(errorFile, "%6.6s", vendorNum)
	fmt.Fprintf(errorFile, "%8.8s", " ")
	fmt.Fprintf(errorFile, "%-20.20s", invoiceNum)
	fmt.Fprintf(errorFile, "%9.9s", " ")

	amount, _ := strconv.ParseFloat(strAmount, 64)
	fmt.Fprintf(errorFile, "%14.14s", humanize.FormatFloat("#,###.##", amount))
	fmt.Fprintf(errorFile, "%14.14s", " ")
	fmt.Fprintf(errorFile, "%6.6s", customerNum)
	fmt.Fprintf(errorFile, "%12.12s", " ")
	fmt.Fprintf(errorFile, "%8.8s", invoiceDate)
	fmt.Fprintf(errorFile, "%16.16s", " ")
	fmt.Fprintf(errorFile, "%s\n", errorMsg)
}

// vDuplicateInDB - check if there is a duplicate in the database and in the input file
func vDuplicateInDB() {
	// dtInvoice, _ := time.Parse("01022006", invoiceDate)
	// stmt := `SELECT 1 FROM apapp100 WHERE vndno=$1 AND invcn=$2 AND vchno=$3 AND apidt=$4`
	stmt := `SELECT 1 FROM apapp100 WHERE vndno=$1 AND invcn=$2`

	tmp := 0
	// err := dbPostgre.QueryRow(stmt, vendorNum, invoiceNum, customerNum, dtInvoice).Scan(&tmp)
	err := dbPostgre.QueryRow(stmt, vendorNum, invoiceNum).Scan(&tmp)
	if err != nil {
		if err != sql.ErrNoRows {
			errorFound = true
			errorMsg = err.Error()
			printError()
			return
		}
	} else {
		errorFound = true
		errorMsg = "Duplicate Invoice # in database"
		printError()
		return
	}

	return
}

// vDuplicateInInputFile - check if there is a duplicate in the input file
func vDuplicateInInputFile() {

	for _, inp := range inputRec {
		if inp.vendorNum == vendorNum && inp.invoiceNum == invoiceNum {
			errorFound = true
			errorMsg = "Duplicate Invoice # in upload file."
			printError()
			return
		}
	}

	// save vendor & invoice in the array
	inputRec = append(inputRec, INPUTREC{
		vendorNum:  vendorNum,
		invoiceNum: invoiceNum,
	})

	return
}

func vVendor() bool {
	if strings.TrimSpace(vendorNum) == "" {
		errorMsg = "Missing Vendor #"
		return true
	}

	inv, _ := strconv.Atoi(vendorNum)
	if inv == 0 {
		errorMsg = "Vendor # is 0"
		return true
	}

	return false
}

func vInvoiceNumber() {
	if invoiceNum == "" {
		errorFound = true
		errorMsg = "Missing Invoice #"
		printError()
		return
	}

	if allchars(invoiceNum, '0') {
		errorFound = true
		errorMsg = "Invoice # is 0"
		printError()
		return
	}
}

func vAmount() {
	if strAmount == "" {
		errorFound = true
		errorMsg = "Missing Invoice Amount."
		printError()
		return
	}

	amount, err := strconv.ParseFloat(strAmount, 64)
	if err != nil {
		errorFound = true
		errorMsg = "Invalid Invoice Amount."
		printError()
		return
	}

	if amount == 0 {
		errorFound = true
		errorMsg = "Invoice Amount is 0."
		printError()
		return
	}

	if amount < 0 {
		errorFound = true
		errorMsg = "Negative Invoice Amount."
		printError()
		return
	}
}

func vCustomerNumber() {
	if customerNum == "" {
		errorFound = true
		errorMsg = "Missing Voucher Number"
		printError()
		return
	}

	if allchars(customerNum, '0') {
		errorFound = true
		errorMsg = "Voucher Number is 0"
		printError()
		return
	}
}

func vInvoiceDate() bool {

	if strings.TrimSpace(invoiceDate) == "" {
		errorMsg = "Missing Date"
		return true
	}

	// prepend "0" if length of date is 5
	if len(invoiceDate) == 5 {
		invoiceDate = "0" + invoiceDate
	}

	// date length is not 6
	if len(invoiceDate) != 6 {
		errorMsg = "Invalid Invoice Date"
		return true
	}

	// the current invoice date format is MMDDYY
	// prepend "20" to the year, to make it 4 char length
	newDateFormat := invoiceDate[:4] + "20" + invoiceDate[4:]

	// validate if newDateFormat is a valid date
	_, err := time.Parse("01022006", newDateFormat)
	if err != nil {
		errorMsg = "Invalid Invoice Date"
		return true
	}

	// save the 8 character Invoice Date
	invoiceDate = newDateFormat
	return false
}

func allchars(field string, char byte) bool {
	for i := 0; i < len(field); i++ {
		if field[i] != char {
			return false
		}
	}

	return true
}
