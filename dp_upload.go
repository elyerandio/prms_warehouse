package main

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"gopkg.in/ini.v1"

	_ "github.com/alexbrainman/odbc"
	"github.com/dustin/go-humanize"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
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
	ACTIV string    `db:"activ"`
	CMPNO int       `db:"cmpno"`
	PLTNO int       `db:"pltno"`
	VNDNO int       `db:"vndno"`
	CRPVD int       `db:"crpvd"`
	VNAME string    `db:"vname"`
	VADD1 string    `db:"vadd1"`
	VADD2 string    `db:"vadd2"`
	VADDX string    `db:"vaddx"`
	VADD3 string    `db:"vadd3"`
	VSTAT string    `db:"vstat"`
	VZIPC string    `db:"vzipc"`
	PHONE float64   `db:"phone"`
	VPHOA string    `db:"vphoa"`
	VTLXN string    `db:"vtlxn"`
	VFAXN string    `db:"vfaxn"`
	VTEMP string    `db:"vtemp"`
	ALPHA string    `db:"alpha"`
	BNKN1 int       `db:"bnkn1"`
	VAPGL float64   `db:"vapgl"`
	VDSGL float64   `db:"vdsgl"`
	VDPGL float64   `db:"vdpgl"`
	VMLDT time.Time `db:"vmldt"`
	TOTDU float64   `db:"totdu"`
	FTOTD float64   `db:"ftotd"`
	MTDPY float64   `db:"mtdpy"`
	FMTDP float64   `db:"fmtdp"`
	PD2PY float64   `db:"pd2py"`
	FP2PY float64   `db:"fp2py"`
	YTDPY float64   `db:"ytdpy"`
	FYTDP float64   `db:"fytdp"`
	PYDPY float64   `db:"pydpy"`
	FPYPY float64   `db:"fpypy"`
	MTDDS float64   `db:"mtdds"`
	FMTDD float64   `db:"fmtdd"`
	PD2DS float64   `db:"pd2ds"`
	FP2DS float64   `db:"fp2ds"`
	YTDDS float64   `db:"ytdds"`
	FYTDD float64   `db:"fytdd"`
	PYDDS float64   `db:"pydds"`
	FPYDS float64   `db:"fpyds"`
	MTDLS float64   `db:"mtdls"`
	FMTLS float64   `db:"fmtls"`
	PD2LS float64   `db:"pd2ls"`
	FP2LS float64   `db:"fp2ls"`
	YTDLS float64   `db:"ytdls"`
	FYTLS float64   `db:"fytls"`
	PYDLS float64   `db:"pydls"`
	FPYLS float64   `db:"fpyls"`
	MTDPC float64   `db:"mtdpc"`
	FMTPC float64   `db:"fmtpc"`
	PD2PC float64   `db:"pd2pc"`
	FP2PC float64   `db:"fp2pc"`
	YTDPC float64   `db:"ytdpc"`
	FYTPC float64   `db:"fytpc"`
	PYDPC float64   `db:"pydpc"`
	FPYPC float64   `db:"fpypc"`
	VCMNT string    `db:"vcmnt"`
	VRPCT float64   `db:"vrpct"`
	F1099 string    `db:"f1099"`
	FEDID string    `db:"fedid"`
	YTD99 float64   `db:"ytd99"`
	NXT99 float64   `db:"nxt99"`
	LSTCY int       `db:"lstcy"`
	VDAYS int       `db:"vdays"`
	NTMDY int       `db:"ntmdy"`
	VCRCD string    `db:"vcrcd"`
	DISCP float64   `db:"discp"`
	DISTD int       `db:"distd"`
	VPHLD string    `db:"vphld"`
	VFDSC string    `db:"vfdsc"`
	VSDSC string    `db:"vsdsc"`
	VDNPY string    `db:"vdnpy"`
	VCONT string    `db:"vcont"`
	VCAT1 string    `db:"vcat1"`
	VCAT2 string    `db:"vcat2"`
	OPDIV string    `db:"opdiv"`
	VPNXT string    `db:"vpnxt"`
	RMAD1 string    `db:"rmad1"`
	RMAD2 string    `db:"rmad2"`
	RMADX string    `db:"rmadx"`
	RMAD3 string    `db:"rmad3"`
	RMSTA string    `db:"rmsta"`
	RMZIP string    `db:"rmzip"`
	VST01 float64   `db:"vst01"`
	VST02 float64   `db:"vst02"`
	VST03 float64   `db:"vst03"`
	VST04 float64   `db:"vst04"`
	VST05 float64   `db:"vst05"`
	VST06 float64   `db:"vst06"`
	VST07 float64   `db:"vst07"`
	VST08 float64   `db:"vst08"`
	VST09 float64   `db:"vst09"`
	VST10 float64   `db:"vst10"`
	VST11 float64   `db:"vst11"`
	VST12 float64   `db:"vst12"`
	VST13 float64   `db:"vst13"`
	FVT01 float64   `db:"fvt01"`
	FVT02 float64   `db:"fvt02"`
	FVT03 float64   `db:"fvt03"`
	FVT04 float64   `db:"fvt04"`
	FVT05 float64   `db:"fvt05"`
	FVT06 float64   `db:"fvt06"`
	FVT07 float64   `db:"fvt07"`
	FVT08 float64   `db:"fvt08"`
	FVT09 float64   `db:"fvt09"`
	FVT10 float64   `db:"fvt10"`
	FVT11 float64   `db:"fvt11"`
	FVT12 float64   `db:"fvt12"`
	FVT13 float64   `db:"fvt13"`
	VSL01 float64   `db:"vsl01"`
	VSL02 float64   `db:"vsl02"`
	VSL03 float64   `db:"vsl03"`
	VSL04 float64   `db:"vsl04"`
	VSL05 float64   `db:"vsl05"`
	VSL06 float64   `db:"vsl06"`
	VSL07 float64   `db:"vsl07"`
	VSL08 float64   `db:"vsl08"`
	VSL09 float64   `db:"vsl09"`
	VSL10 float64   `db:"vsl10"`
	VSL11 float64   `db:"vsl11"`
	VSL12 float64   `db:"vsl12"`
	VSL13 float64   `db:"vsl13"`
	FVL01 float64   `db:"fvl01"`
	FVL02 float64   `db:"fvl02"`
	FVL03 float64   `db:"fvl03"`
	FVL04 float64   `db:"fvl04"`
	FVL05 float64   `db:"fvl05"`
	FVL06 float64   `db:"fvl06"`
	FVL07 float64   `db:"fvl07"`
	FVL08 float64   `db:"fvl08"`
	FVL09 float64   `db:"fvl09"`
	FVL10 float64   `db:"fvl10"`
	FVL11 float64   `db:"fvl11"`
	FVL12 float64   `db:"fvl12"`
	FVL13 float64   `db:"fvl13"`
	ONORV float64   `db:"onorv"`
	FONOV float64   `db:"fonov"`
	VMMIN float64   `db:"vmmin"`
	VMMAX float64   `db:"vmmax"`
	VMAVO float64   `db:"vmavo"`
	VMIMW float64   `db:"vmimw"`
	VPOHL string    `db:"vpohl"`
	VMOUT string    `db:"vmout"`
	VMVIA string    `db:"vmvia"`
	VMCAR string    `db:"vmcar"`
	VMFRT string    `db:"vmfrt"`
	VMFTD string    `db:"vmftd"`
	VRCBY float64   `db:"vrcby"`
	VRCBM float64   `db:"vrcbm"`
	VRCBP float64   `db:"vrcbp"`
	VRCAY float64   `db:"vrcay"`
	FRCAY float64   `db:"frcay"`
	VRCAM float64   `db:"vrcam"`
	FRCAM float64   `db:"frcam"`
	VRCAP float64   `db:"vrcap"`
	FRCAP float64   `db:"frcap"`
	VJCBY float64   `db:"vjcby"`
	VJCBM float64   `db:"vjcbm"`
	VJCBP float64   `db:"vjcbp"`
	VMSCM float64   `db:"vmscm"`
	VMSCY float64   `db:"vmscy"`
	VMRVM float64   `db:"vmrvm"`
	VMRVY float64   `db:"vmrvy"`
	VMRWM float64   `db:"vmrwm"`
	VMRWY float64   `db:"vmrwy"`
	VNESY int       `db:"vnesy"`
	VNESM int       `db:"vnesm"`
	VNESP int       `db:"vnesp"`
	VNLSY int       `db:"vnlsy"`
	VNLSM int       `db:"vnlsm"`
	VNLSP int       `db:"vnlsp"`
	VOTSY int       `db:"votsy"`
	VOTSM int       `db:"votsm"`
	VOTSP int       `db:"votsp"`
	VPOCT string    `db:"vpoct"`
	VSMCT string    `db:"vsmct"`
	VMLPD int       `db:"vmlpd"`
	VTXB1 string    `db:"vtxb1"`
	VTXB2 string    `db:"vtxb2"`
	VTXB3 string    `db:"vtxb3"`
	VTAX1 string    `db:"vtax1"`
	VTAX2 string    `db:"vtax2"`
	VTAX3 string    `db:"vtax3"`
	VTXL1 string    `db:"vtxl1"`
	VTXL2 string    `db:"vtxl2"`
	VTXL3 string    `db:"vtxl3"`
	V1099 int       `db:"v1099"`
	VDDGL float64   `db:"vddgl"`
	VDDCM string    `db:"vddcm"`
	VDDST string    `db:"vddst"`
	VVRSN string    `db:"vvrsn"`
	VIRSN string    `db:"virsn"`
	VMSSD float64   `db:"vmssd"`
	VACGL float64   `db:"vacgl"`
	VMPYC string    `db:"vmpyc"`
	VMAPR string    `db:"vmapr"`
	VMPEN float64   `db:"vmpen"`
	VMFPN float64   `db:"vmfpn"`
	VMTXP string    `db:"vmtxp"`
	WBRCD string    `db:"wbrcd"`
	VMEDI string    `db:"vmedi"`
	VMED1 string    `db:"vmed1"`
	VMED2 string    `db:"vmed2"`
	VMED3 string    `db:"vmed3"`
	VMED4 float64   `db:"vmed4"`
	VMED5 float64   `db:"vmed5"`
	VMED6 float64   `db:"vmed6"`
	VMDPD string    `db:"vmdpd"`
	VMVSC string    `db:"vmvsc"`
	VMVSE string    `db:"vmvse"`
	VMCTY string    `db:"vmcty"`
	VMSEC string    `db:"vmsec"`
	VMLNG string    `db:"vmlng"`
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

type POSTGRE struct {
	db     *sqlx.DB
	ip     string
	dbname string
	user   string
	pwd    string
}

var (
	dbPostgre     *sqlx.DB
	dbOdbc        *sqlx.DB
	userAS        string
	inv           Invoice
	vnd           Vendor
	bnk           Bank
	runType       string
	errorFile     *os.File
	passFile      *os.File
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
	pqConnections []POSTGRE
)

func main() {
	// read ini file
	pqConnections = []POSTGRE{}
	readIni()
	defer closeConnections()

	// prompt for Trial/Final Run
	runType = getInputRun()

	// initialize the input record struct array, to check for duplicate entries
	inputRec = []INPUTREC{}

	getSystemControl()
	inputFile := getInput("\nFile to upload : ")
	openSuccessFile(inputFile)
	openErrorFile(inputFile)
	uploadCSV(inputFile)

	fmt.Printf("\n\nInput record count : %d\n", lineCount)
	fmt.Printf("Error count : %d\n", errorCount)
	fmt.Printf("Uploaded count : %d\n", saveCount)
	fmt.Print("\nPress ENTER to continue...")
	fmt.Fprintf(errorFile, "\nError count : %s\n", humanize.Comma(int64(errorCount)))
	if runType == "Final" {
		fmt.Fprintf(errorFile, "\nUploaded count : %s\n", humanize.Comma(int64(saveCount)))
	}
	fmt.Scanln()
}

func readIni() {
	var err error

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
	userAS = ""
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
	if cfg != nil {
		pwd = cfg.Section("AS/400").Key("pwd").String()
		pwd = strings.TrimSpace(pwd)
		if pwd == "" {
			pwd, err = getCredentials(userAS)
			if err != nil {
				panic(err)
			}
		}
	} else {
		pwd, err = getCredentials(userAS)
		if err != nil {
			panic(err)
		}
	}

	// connect to AS400
	odbcConnectStr := fmt.Sprintf("DSN=%s; UID=%s; PWD=%s", dsn, userAS, pwd)
	dbOdbc, err = sqlx.Open("odbc", odbcConnectStr)
	// dbOdbc, err := sql.Open("odbc", fmt.Sprintf("DSN=%s; UID=%s; PWD=%s", "MDC", "APC", "APPS7OWNER"))
	err = dbOdbc.Ping()
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
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
	// close ODBC connection
	dbOdbc.Close()

	// close Postgre connections
	for _, pq := range pqConnections {
		pq.db.Close()
	}
}

func getPostgreConnections(cfg *ini.File) {

	// read all PostgreSQL sections
	for i := 1; i < 20; i++ {
		sectionName := fmt.Sprintf("PostgreSQL%d", i)
		if cfg.HasSection(sectionName) {
			section := cfg.Section(sectionName)
			ip := strings.TrimSpace(section.Key("server_ip").String())
			dbname := strings.TrimSpace(section.Key("db_name").String())
			user := strings.TrimSpace(section.Key("user").String())
			pwd := strings.TrimSpace(section.Key("pwd").String())

			connString := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
				ip, 5432, user, pwd, dbname)

			db, err := sqlx.Connect("postgres", connString)
			if err != nil {
				fmt.Println(err)
				panic(err)
			}

			pqConnections = append(pqConnections, POSTGRE{
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
	if err == sql.ErrNoRows {
		errorFound = true
		errorMsg = "APPERD code not found in MSWCP100"
		printError()
	} else if err != nil {
		panic(err)
	}

	// get APGLAC -> Default G/L Accounts
	err = dbOdbc.QueryRow(stmt, "APGLAC", 777, 0).Scan(&wcdta)
	if err == sql.ErrNoRows {
		errorFound = true
		errorMsg = "APGLAC code not found in MSWCP100"
		printError()
	} else if err != nil {
		panic(err)
	}
	apglac = APGLAC{}
	temp := ""
	temp = wcdta[0:15]
	apglac.APACC, _ = strconv.ParseInt(temp, 10, 64)

	// get APCLOS
	err = dbOdbc.QueryRow(stmt, "APCLOS", 777, 0).Scan(&wcdta)
	if err == sql.ErrNoRows {
		errorFound = true
		errorMsg = "APCLOS code not found in MSWCP100"
		printError()
	} else if err != nil {
		panic(err)
	}

	// get MSCURR
	err = dbOdbc.QueryRow(stmt, "MSCURR", 777, 0).Scan(&wcdta)
	if err == sql.ErrNoRows {
		errorFound = true
		errorMsg = "MSCURR code not found in MSWCP100"
		printError()
	} else if err != nil {
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

	fmt.Println("Error file     : ", errorFileName)
}

func openSuccessFile(inputFile string) {
	var err error

	// extract the filename w/o file extension
	successFileName := strings.TrimSuffix(inputFile, filepath.Ext(inputFile))

	// add .pass file extension
	successFileName += ".pass"

	// create the success file
	passFile, err = os.Create(successFileName)
	if err != nil {
		panic(err)
	}

	// write header to the success file
	space := " "
	fmt.Fprintf(passFile, "%sAP Uploading Output Report%s%s\n", strings.Repeat(" ", 60),
		strings.Repeat(" ", 55), time.Now().Format("01/02/2006"))
	fmt.Fprintf(passFile, "%s%s Run\n\n", strings.Repeat(" ", 67), runType)
	fmt.Fprintf(passFile, "%6.6s%4.4s%s%6.6s%s%-20.20s%s%-14.14s%s%-9.9s%s%-12.12s%s\n",
		"LINE #", space, "VENDOR #", space, "INVOICE #", space, "INVOICE AMOUNT", space, "VOUCHER #",
		space, "INVOICE DATE", space, "REMARKS")
	fmt.Fprintf(passFile, "%s\n", strings.Repeat("-", 150))

	fmt.Println("Success file   : ", successFileName)
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
	fmt.Printf("\nRecord # : %6d", lineCount)
	for {
		errorFound = false
		record, err := r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		lineCount++
		fmt.Printf("\b\b\b\b\b\b%6d", lineCount)

		inv = Invoice{}
		currTimeStamp = time.Now()

		// get the column data to upload
		vendorNum = strings.TrimSpace(record[0])
		invoiceNum = strings.TrimSpace(record[1])
		strAmount = strings.TrimSpace(record[2])
		customerNum = strings.TrimSpace(record[3])
		invoiceDate = strings.TrimSpace(record[4])

		// validate Invoice Date
		if vInvoiceDate() {
			errorFound = true
			printError()
		}

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
			} else {
				if vnd.VDDGL == 0 {
					errorFound = true
					errorMsg = "Missing Vendor G/L Account"
					printError()
				}
			}
		}

		vInvoiceNumber()
		vAmount()
		vCustomerNumber()

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

		if !errorFound {
			printRemark()
		}
	}
}

func getVendor(vendorNum string) (Vendor, error) {
	vendor := Vendor{}
	fields := DBFields(Vendor{})
	fieldsCsv := fieldsCSV(fields)
	// fieldsCSVColons := fieldsCSVColons(fields)

	selectStmt := fmt.Sprintf("SELECT %s FROM MSVMP100 WHERE vndno=$1", fieldsCsv)
	// err := dbOdbc.QueryRowx(selectStmt, vendorNum).StructScan(&vendor)
	err := dbPostgre.QueryRowx(selectStmt, vendorNum).StructScan(&vendor)
	if err != nil {
		if err == sql.ErrNoRows {
			return vendor, err
		}

		fmt.Println()
		fmt.Println(selectStmt)
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
	inv.USRID = userAS
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

func printRemark() {
	fmt.Fprintf(passFile, "%05d", lineCount)
	fmt.Fprintf(passFile, "%5.5s", " ")
	fmt.Fprintf(passFile, "%6.6s", vendorNum)
	fmt.Fprintf(passFile, "%8.8s", " ")
	fmt.Fprintf(passFile, "%-20.20s", invoiceNum)
	fmt.Fprintf(passFile, "%9.9s", " ")

	amount, _ := strconv.ParseFloat(strAmount, 64)
	fmt.Fprintf(passFile, "%14.14s", humanize.FormatFloat("#,###.##", amount))
	fmt.Fprintf(passFile, "%14.14s", " ")
	fmt.Fprintf(passFile, "%6.6s", customerNum)
	fmt.Fprintf(passFile, "%12.12s", " ")
	fmt.Fprintf(passFile, "%8.8s", invoiceDate)
	fmt.Fprintf(passFile, "%16.16s", " ")
	if runType == "Final" {
		fmt.Fprintf(passFile, "Uploaded\n")
	} else {
		fmt.Fprintf(passFile, "Passed\n")
	}
}

// vDuplicateInDB - check if there is a duplicate in the database and in the input file
func vDuplicateInDB() {
	// dtInvoice, _ := time.Parse("01022006", invoiceDate)
	// stmt := `SELECT 1 FROM apapp100 WHERE vndno=$1 AND invcn=$2 AND vchno=$3 AND apidt=$4`
	stmt := `SELECT vchno FROM apapp100 WHERE vndno=$1 AND TRIM(invcn)=$2`

	wg := sync.WaitGroup{}
	for _, pq := range pqConnections {
		wg.Add(1)
		go func(pq POSTGRE, vendorNum string, invoiceNum string) {
			vchno := 0
			err := pq.db.QueryRow(stmt, vendorNum, invoiceNum).Scan(&vchno)
			wg.Done()
			if err != nil {
				if err != sql.ErrNoRows {
					errorFound = true
					errorMsg = err.Error()
					printError()
					return
				}
			} else {
				errorFound = true
				errorMsg = fmt.Sprintf("Duplicate Invoice # (branch=%d) in database %s", vchno, pq.dbname)
				printError()
				return
			}
		}(pq, vendorNum, invoiceNum)
	}

	wg.Wait()
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
