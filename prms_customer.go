package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"syscall"
	"time"

	_ "github.com/alexbrainman/odbc"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"golang.org/x/term"
)

var dbOdbc *sqlx.DB
var dbMysql *sqlx.DB
var dateStart, dateEnd time.Time

type Customer struct {
	ACTIV string    `db:"ACTIV"`
	CMPNO int       `db:"CMPNO"`
	PLTNO int       `db:"PLTNO"`
	CUSNO int       `db:"CUSNO"`
	CNAME string    `db:"CNAME"`
	CADD1 string    `db:"CADD1"`
	CADD2 string    `db:"CADD2"`
	CADDX string    `db:"CADDX"`
	CADD3 string    `db:"CADD3"`
	CSTTE string    `db:"CSTTE"`
	CZIPC string    `db:"CZIPC"`
	CNTAC string    `db:"CNTAC"`
	CPHON int       `db:"CPHON"`
	CPHOA string    `db:"CPHOA"`
	CTLXN string    `db:"CTLXN"`
	CFAXN string    `db:"CFAXN"`
	CCUSN int       `db:"CCUS#"`
	BLLCS int       `db:"BLLCS"`
	CMSDT time.Time `db:"CMSDT"`
	CTYPE string    `db:"CTYPE"`
	CRHLD string    `db:"CRHLD"`
	CRLIM int       `db:"CRLIM"`
	CRDAY int       `db:"CRDAY"`
	FCHFG string    `db:"FCHFG"`
	CMASF string    `db:"CMASF"`
	PSTFG string    `db:"PSTFG"`
	AMTDU float64   `db:"AMTDU"`
	OROPN float64   `db:"OROPN"`
	HIGHB int       `db:"HIGHB"`
	PAYAM float64   `db:"PAYAM"`
	PAYDT time.Time `db:"PAYDT"`
	PURDT time.Time `db:"PURDT"`
	MTDDT float64   `db:"MTDDT"`
	YTDDT float64   `db:"YTDDT"`
	PYTDT float64   `db:"PYTDT"`
	MTDDL float64   `db:"MTDDL"`
	YTDDL float64   `db:"YTDDL"`
	PYTDL float64   `db:"PYTDL"`
	MTDDC float64   `db:"MTD$C"`
	YTDDC float64   `db:"YTD$C"`
	PYTSL float64   `db:"PYTSL"`
	PYTDC float64   `db:"PYTDC"`
	MTDFT float64   `db:"MTDFT"`
	YTDFT float64   `db:"YTDFT"`
	PYTFT float64   `db:"PYTFT"`
	MTDTA float64   `db:"MTDTA"`
	YTDTA float64   `db:"YTDTA"`
	PYTTA float64   `db:"PYTTA"`
	CST01 float64   `db:"CST01"`
	CST02 float64   `db:"CST02"`
	CST03 float64   `db:"CST03"`
	CST04 float64   `db:"CST04"`
	CST05 float64   `db:"CST05"`
	CST06 float64   `db:"CST06"`
	CST07 float64   `db:"CST07"`
	CST08 float64   `db:"CST08"`
	CST09 float64   `db:"CST09"`
	CST10 float64   `db:"CST10"`
	CST11 float64   `db:"CST11"`
	CST12 float64   `db:"CST12"`
	CST13 float64   `db:"CST13"`
	CSL01 float64   `db:"CSL01"`
	CSL02 float64   `db:"CSL02"`
	CSL03 float64   `db:"CSL03"`
	CSL04 float64   `db:"CSL04"`
	CSL05 float64   `db:"CSL05"`
	CSL06 float64   `db:"CSL06"`
	CSL07 float64   `db:"CSL07"`
	CSL08 float64   `db:"CSL08"`
	CSL09 float64   `db:"CSL09"`
	CSL10 float64   `db:"CSL10"`
	CSL11 float64   `db:"CSL11"`
	CSL12 float64   `db:"CSL12"`
	CSL13 float64   `db:"CSL13"`
	CMSWK float64   `db:"CMSWK"`
	DSCDE int       `db:"DSCDE"`
	DSCTF string    `db:"DSCTF"`
	DSPCT float64   `db:"DSPCT"`
	CMAGL int       `db:"CMAGL"`
	CMDGL int       `db:"CMDGL"`
	CMBGL int       `db:"CMBGL"`
	CMSGL int       `db:"CMSGL"`
	CPCUS int       `db:"CPCUS"`
	TXBLE string    `db:"TXBLE"`
	TXBL2 string    `db:"TXBL2"`
	TXBL3 string    `db:"TXBL3"`
	TAX01 string    `db:"TAX01"`
	TAX02 string    `db:"TAX02"`
	TAX03 string    `db:"TAX03"`
	REGON int       `db:"REGON"`
	SALNO int       `db:"SALNO"`
	SHVIA string    `db:"SHVIA"`
	DFTWH string    `db:"DFTWH"`
	CMTRM string    `db:"CMTRM"`
	CASHO string    `db:"CASHO"`
	CTEXT string    `db:"CTEXT"`
	ARDAY int       `db:"ARDAY"`
	AVPAY int       `db:"AVPAY"`
	AVINV int       `db:"AVINV"`
	AVBAL int       `db:"AVBAL"`
	DIVSN string    `db:"DIVSN"`
	CTYP1 string    `db:"CTYP1"`
	CTYP2 string    `db:"CTYP2"`
	BACKM string    `db:"BACKM"`
	ALPHA string    `db:"ALPHA"`
	CCOMM float64   `db:"CCOMM"`
	AGING string    `db:"AGING"`
	MDCDC float64   `db:"MDC$C"`
	YDCDC float64   `db:"YDC$C"`
	PYCSL float64   `db:"PYCSL"`
	PYCDC float64   `db:"PYCDC"`
	MDCFT float64   `db:"MDCFT"`
	YDCFT float64   `db:"YDCFT"`
	PYCFT float64   `db:"PYCFT"`
	MTDCT float64   `db:"MTDCT"`
	YTDCT float64   `db:"YTDCT"`
	PYTCT float64   `db:"PYTCT"`
	CROPN float64   `db:"CROPN"`
	CMRGL int       `db:"CMRGL"`
	CFCDM float64   `db:"CFC$M"`
	CFCDY float64   `db:"CFC$Y"`
	CFCPY float64   `db:"CFCPY"`
	CFCPD float64   `db:"CFCPD"`
	CFFCM float64   `db:"CFFCM"`
	CFFCY float64   `db:"CFFCY"`
	PYCFC float64   `db:"PYCFC"`
	CFCTM float64   `db:"CFCTM"`
	CFCTY float64   `db:"CFCTY"`
	CFPTY float64   `db:"CFPTY"`
	CFOPN float64   `db:"CFOPN"`
	CFC01 float64   `db:"CFC01"`
	CFC02 float64   `db:"CFC02"`
	CFC03 float64   `db:"CFC03"`
	CFC04 float64   `db:"CFC04"`
	CFC05 float64   `db:"CFC05"`
	CFC06 float64   `db:"CFC06"`
	CFC07 float64   `db:"CFC07"`
	CFC08 float64   `db:"CFC08"`
	CFC09 float64   `db:"CFC09"`
	CFC10 float64   `db:"CFC10"`
	CFC11 float64   `db:"CFC11"`
	CFC12 float64   `db:"CFC12"`
	CFC13 float64   `db:"CFC13"`
	CFL01 float64   `db:"CFL01"`
	CFL02 float64   `db:"CFL02"`
	CFL03 float64   `db:"CFL03"`
	CFL04 float64   `db:"CFL04"`
	CFL05 float64   `db:"CFL05"`
	CFL06 float64   `db:"CFL06"`
	CFL07 float64   `db:"CFL07"`
	CFL08 float64   `db:"CFL08"`
	CFL09 float64   `db:"CFL09"`
	CFL10 float64   `db:"CFL10"`
	CFL11 float64   `db:"CFL11"`
	CFL12 float64   `db:"CFL12"`
	CFL13 float64   `db:"CFL13"`
	CMT01 float64   `db:"CMT01"`
	CMT02 float64   `db:"CMT02"`
	CMT03 float64   `db:"CMT03"`
	CMT04 float64   `db:"CMT04"`
	CMT05 float64   `db:"CMT05"`
	CMT06 float64   `db:"CMT06"`
	CMT07 float64   `db:"CMT07"`
	CMT08 float64   `db:"CMT08"`
	CMT09 float64   `db:"CMT09"`
	CMT10 float64   `db:"CMT10"`
	CMT11 float64   `db:"CMT11"`
	CMT12 float64   `db:"CMT12"`
	CMT13 float64   `db:"CMT13"`
	CML01 float64   `db:"CML01"`
	CML02 float64   `db:"CML02"`
	CML03 float64   `db:"CML03"`
	CML04 float64   `db:"CML04"`
	CML05 float64   `db:"CML05"`
	CML06 float64   `db:"CML06"`
	CML07 float64   `db:"CML07"`
	CML08 float64   `db:"CML08"`
	CML09 float64   `db:"CML09"`
	CML10 float64   `db:"CML10"`
	CML11 float64   `db:"CML11"`
	CML12 float64   `db:"CML12"`
	CML13 float64   `db:"CML13"`
	CMBUY int       `db:"CMBUY"`
	CMPPL string    `db:"CMPPL"`
	CMRPO string    `db:"CMRPO"`
	TXLI1 string    `db:"TXLI1"`
	TXLI2 string    `db:"TXLI2"`
	TXLI3 string    `db:"TXLI3"`
	CCRCD string    `db:"CCRCD"`
	FAMDU float64   `db:"FAMDU"`
	FOROP float64   `db:"FOROP"`
	FHIGH int       `db:"FHIGH"`
	FPYAM float64   `db:"FPYAM"`
	FMTDT float64   `db:"FMTDT"`
	FYTDT float64   `db:"FYTDT"`
	FPYDT float64   `db:"FPYDT"`
	FMTDL float64   `db:"FMTDL"`
	FYTDL float64   `db:"FYTDL"`
	FPYDL float64   `db:"FPYDL"`
	FMTDC float64   `db:"FMT$C"`
	FYTDC float64   `db:"FYT$C"`
	FPYSL float64   `db:"FPYSL"`
	FPYDC float64   `db:"FPYDC"`
	FMTFT float64   `db:"FMTFT"`
	FYTFT float64   `db:"FYTFT"`
	FPYFT float64   `db:"FPYFT"`
	FCT01 float64   `db:"FCT01"`
	FCT02 float64   `db:"FCT02"`
	FCT03 float64   `db:"FCT03"`
	FCT04 float64   `db:"FCT04"`
	FCT05 float64   `db:"FCT05"`
	FCT06 float64   `db:"FCT06"`
	FCT07 float64   `db:"FCT07"`
	FCT08 float64   `db:"FCT08"`
	FCT09 float64   `db:"FCT09"`
	FCT10 float64   `db:"FCT10"`
	FCT11 float64   `db:"FCT11"`
	FCT12 float64   `db:"FCT12"`
	FCT13 float64   `db:"FCT13"`
	FCL01 float64   `db:"FCL01"`
	FCL02 float64   `db:"FCL02"`
	FCL03 float64   `db:"FCL03"`
	FCL04 float64   `db:"FCL04"`
	FCL05 float64   `db:"FCL05"`
	FCL06 float64   `db:"FCL06"`
	FCL07 float64   `db:"FCL07"`
	FCL08 float64   `db:"FCL08"`
	FCL09 float64   `db:"FCL09"`
	FCL10 float64   `db:"FCL10"`
	FCL11 float64   `db:"FCL11"`
	FCL12 float64   `db:"FCL12"`
	FCL13 float64   `db:"FCL13"`
	FCMWK float64   `db:"FCMWK"`
	FAVPY int       `db:"FAVPY"`
	FAVIN int       `db:"FAVIN"`
	FAVBL int       `db:"FAVBL"`
	CDFOT string    `db:"CDFOT"`
	CMOR1 string    `db:"CMOR1"`
	CMOR2 string    `db:"CMOR2"`
	CQOCF string    `db:"CQOCF"`
	FDTWK float64   `db:"FDTWK"`
	CDTWK float64   `db:"CDTWK"`
	FDLWK float64   `db:"FDLWK"`
	CDLWK float64   `db:"CDLWK"`
	FFTWK float64   `db:"FFTWK"`
	CFTWK float64   `db:"CFTWK"`
	CTAWK float64   `db:"CTAWK"`
	FDSWK float64   `db:"FDSWK"`
	CDSWK float64   `db:"CDSWK"`
	FDFWK float64   `db:"FDFWK"`
	CDFWK float64   `db:"CDFWK"`
	FDXWK float64   `db:"FDXWK"`
	CDXWK float64   `db:"CDXWK"`
	CPSEC time.Time `db:"CPSEC"`
	CMDAD float64   `db:"CMDAD"`
	CMDFQ string    `db:"CMDFQ"`
	CMSLF string    `db:"CMSLF"`
	CMSPD int       `db:"CMSPD"`
	CMSBS int       `db:"CMSBS"`
	CMAFR int       `db:"CMAFR"`
	CMPFR int       `db:"CMPFR"`
	CMPCO string    `db:"CMPCO"`
	CMPRA int       `db:"CMPRA"`
	CMEDI string    `db:"CMEDI"`
	CMCT1 int       `db:"CMCT1"`
	CMCT2 int       `db:"CMCT2"`
	CMCT3 int       `db:"CMCT3"`
	CMCT4 int       `db:"CMCT4"`
	CMCT5 int       `db:"CMCT5"`
	CMCT6 int       `db:"CMCT6"`
	CMSAG float64   `db:"CMSAG"`
	CMFGS float64   `db:"CMFGS"`
	CMFGC float64   `db:"CMFGC"`
	CMFGE float64   `db:"CMFGE"`
	CMCRB time.Time `db:"CMCRB"`
	CMPYB time.Time `db:"CMPYB"`
	CMRAN string    `db:"CMRAN"`
	CMCDR string    `db:"CMCDR"`
	CMCEC time.Time `db:"CMCEC"`
	CMAOC time.Time `db:"CMAOC"`
	CMAAR int       `db:"CMAAR"`
	CMCOG int       `db:"CMCOG"`
	IVEDI string    `db:"IVEDI"`
	CMCDE string    `db:"CMCDE"`
	CMDUN string    `db:"CMDUN"`
	CMDBS string    `db:"CMDBS"`
	CMMSG string    `db:"CMMSG"`
	CMLTR string    `db:"CMLTR"`
	CMMGR string    `db:"CMMGR"`
	CMCCR string    `db:"CMCCR"`
	CMCRC time.Time `db:"CMCRC"`
	CMPCR string    `db:"CMPCR"`
	CMPYD time.Time `db:"CMPYD"`
	CMAGY string    `db:"CMAGY"`
	CMRSK string    `db:"CMRSK"`
	CMEDT time.Time `db:"CMEDT"`
	CMODT time.Time `db:"CMODT"`
	CMCN1 string    `db:"CMCN1"`
	CMPH1 string    `db:"CMPH1"`
	CMCN2 string    `db:"CMCN2"`
	CMPH2 string    `db:"CMPH2"`
	CMDFC string    `db:"CMDFC"`
	CMAD1 string    `db:"CMAD1"`
	CMAD2 string    `db:"CMAD2"`
	CMAD3 string    `db:"CMAD3"`
	CMCTY string    `db:"CMCTY"`
	CMDST string    `db:"CMDST"`
	CMDZP string    `db:"CMDZP"`
	CMCTR string    `db:"CMCTR"`
	CMSEC string    `db:"CMSEC"`
	CMSUP string    `db:"CMSUP"`
	CMTXI string    `db:"CMTXI"`
	CMFRC string    `db:"CMFRC"`
	CMCAR string    `db:"CMCAR"`
	CMMOD string    `db:"CMMOD"`
	CMLNG string    `db:"CMLNG"`
}

type Customer2 struct {
	ACTIV string    `db:"ACTIV"`
	CMPNO int       `db:"CMPNO"`
	PLTNO int       `db:"PLTNO"`
	CUSNO int       `db:"CUSNO"`
	CNAME string    `db:"CNAME"`
	CADD1 string    `db:"CADD1"`
	CADD2 string    `db:"CADD2"`
	CADDX string    `db:"CADDX"`
	CADD3 string    `db:"CADD3"`
	CSTTE string    `db:"CSTTE"`
	CZIPC string    `db:"CZIPC"`
	CNTAC string    `db:"CNTAC"`
	CPHON int       `db:"CPHON"`
	CPHOA string    `db:"CPHOA"`
	CTLXN string    `db:"CTLXN"`
	CFAXN string    `db:"CFAXN"`
	CCUSN int       `db:"CCUSN"`
	BLLCS int       `db:"BLLCS"`
	CMSDT time.Time `db:"CMSDT"`
	CTYPE string    `db:"CTYPE"`
	CRHLD string    `db:"CRHLD"`
	CRLIM int       `db:"CRLIM"`
	CRDAY int       `db:"CRDAY"`
	FCHFG string    `db:"FCHFG"`
	CMASF string    `db:"CMASF"`
	PSTFG string    `db:"PSTFG"`
	AMTDU float64   `db:"AMTDU"`
	OROPN float64   `db:"OROPN"`
	HIGHB int       `db:"HIGHB"`
	PAYAM float64   `db:"PAYAM"`
	PAYDT time.Time `db:"PAYDT"`
	PURDT time.Time `db:"PURDT"`
	MTDDT float64   `db:"MTDDT"`
	YTDDT float64   `db:"YTDDT"`
	PYTDT float64   `db:"PYTDT"`
	MTDDL float64   `db:"MTDDL"`
	YTDDL float64   `db:"YTDDL"`
	PYTDL float64   `db:"PYTDL"`
	MTDDC float64   `db:"MTDDC"`
	YTDDC float64   `db:"YTDDC"`
	PYTSL float64   `db:"PYTSL"`
	PYTDC float64   `db:"PYTDC"`
	MTDFT float64   `db:"MTDFT"`
	YTDFT float64   `db:"YTDFT"`
	PYTFT float64   `db:"PYTFT"`
	MTDTA float64   `db:"MTDTA"`
	YTDTA float64   `db:"YTDTA"`
	PYTTA float64   `db:"PYTTA"`
	CST01 float64   `db:"CST01"`
	CST02 float64   `db:"CST02"`
	CST03 float64   `db:"CST03"`
	CST04 float64   `db:"CST04"`
	CST05 float64   `db:"CST05"`
	CST06 float64   `db:"CST06"`
	CST07 float64   `db:"CST07"`
	CST08 float64   `db:"CST08"`
	CST09 float64   `db:"CST09"`
	CST10 float64   `db:"CST10"`
	CST11 float64   `db:"CST11"`
	CST12 float64   `db:"CST12"`
	CST13 float64   `db:"CST13"`
	CSL01 float64   `db:"CSL01"`
	CSL02 float64   `db:"CSL02"`
	CSL03 float64   `db:"CSL03"`
	CSL04 float64   `db:"CSL04"`
	CSL05 float64   `db:"CSL05"`
	CSL06 float64   `db:"CSL06"`
	CSL07 float64   `db:"CSL07"`
	CSL08 float64   `db:"CSL08"`
	CSL09 float64   `db:"CSL09"`
	CSL10 float64   `db:"CSL10"`
	CSL11 float64   `db:"CSL11"`
	CSL12 float64   `db:"CSL12"`
	CSL13 float64   `db:"CSL13"`
	CMSWK float64   `db:"CMSWK"`
	DSCDE int       `db:"DSCDE"`
	DSCTF string    `db:"DSCTF"`
	DSPCT float64   `db:"DSPCT"`
	CMAGL int       `db:"CMAGL"`
	CMDGL int       `db:"CMDGL"`
	CMBGL int       `db:"CMBGL"`
	CMSGL int       `db:"CMSGL"`
	CPCUS int       `db:"CPCUS"`
	TXBLE string    `db:"TXBLE"`
	TXBL2 string    `db:"TXBL2"`
	TXBL3 string    `db:"TXBL3"`
	TAX01 string    `db:"TAX01"`
	TAX02 string    `db:"TAX02"`
	TAX03 string    `db:"TAX03"`
	REGON int       `db:"REGON"`
	SALNO int       `db:"SALNO"`
	SHVIA string    `db:"SHVIA"`
	DFTWH string    `db:"DFTWH"`
	CMTRM string    `db:"CMTRM"`
	CASHO string    `db:"CASHO"`
	CTEXT string    `db:"CTEXT"`
	ARDAY int       `db:"ARDAY"`
	AVPAY int       `db:"AVPAY"`
	AVINV int       `db:"AVINV"`
	AVBAL int       `db:"AVBAL"`
	DIVSN string    `db:"DIVSN"`
	CTYP1 string    `db:"CTYP1"`
	CTYP2 string    `db:"CTYP2"`
	BACKM string    `db:"BACKM"`
	ALPHA string    `db:"ALPHA"`
	CCOMM float64   `db:"CCOMM"`
	AGING string    `db:"AGING"`
	MDCDC float64   `db:"MDCDC"`
	YDCDC float64   `db:"YDCDC"`
	PYCSL float64   `db:"PYCSL"`
	PYCDC float64   `db:"PYCDC"`
	MDCFT float64   `db:"MDCFT"`
	YDCFT float64   `db:"YDCFT"`
	PYCFT float64   `db:"PYCFT"`
	MTDCT float64   `db:"MTDCT"`
	YTDCT float64   `db:"YTDCT"`
	PYTCT float64   `db:"PYTCT"`
	CROPN float64   `db:"CROPN"`
	CMRGL int       `db:"CMRGL"`
	CFCDM float64   `db:"CFCDM"`
	CFCDY float64   `db:"CFCDY"`
	CFCPY float64   `db:"CFCPY"`
	CFCPD float64   `db:"CFCPD"`
	CFFCM float64   `db:"CFFCM"`
	CFFCY float64   `db:"CFFCY"`
	PYCFC float64   `db:"PYCFC"`
	CFCTM float64   `db:"CFCTM"`
	CFCTY float64   `db:"CFCTY"`
	CFPTY float64   `db:"CFPTY"`
	CFOPN float64   `db:"CFOPN"`
	CFC01 float64   `db:"CFC01"`
	CFC02 float64   `db:"CFC02"`
	CFC03 float64   `db:"CFC03"`
	CFC04 float64   `db:"CFC04"`
	CFC05 float64   `db:"CFC05"`
	CFC06 float64   `db:"CFC06"`
	CFC07 float64   `db:"CFC07"`
	CFC08 float64   `db:"CFC08"`
	CFC09 float64   `db:"CFC09"`
	CFC10 float64   `db:"CFC10"`
	CFC11 float64   `db:"CFC11"`
	CFC12 float64   `db:"CFC12"`
	CFC13 float64   `db:"CFC13"`
	CFL01 float64   `db:"CFL01"`
	CFL02 float64   `db:"CFL02"`
	CFL03 float64   `db:"CFL03"`
	CFL04 float64   `db:"CFL04"`
	CFL05 float64   `db:"CFL05"`
	CFL06 float64   `db:"CFL06"`
	CFL07 float64   `db:"CFL07"`
	CFL08 float64   `db:"CFL08"`
	CFL09 float64   `db:"CFL09"`
	CFL10 float64   `db:"CFL10"`
	CFL11 float64   `db:"CFL11"`
	CFL12 float64   `db:"CFL12"`
	CFL13 float64   `db:"CFL13"`
	CMT01 float64   `db:"CMT01"`
	CMT02 float64   `db:"CMT02"`
	CMT03 float64   `db:"CMT03"`
	CMT04 float64   `db:"CMT04"`
	CMT05 float64   `db:"CMT05"`
	CMT06 float64   `db:"CMT06"`
	CMT07 float64   `db:"CMT07"`
	CMT08 float64   `db:"CMT08"`
	CMT09 float64   `db:"CMT09"`
	CMT10 float64   `db:"CMT10"`
	CMT11 float64   `db:"CMT11"`
	CMT12 float64   `db:"CMT12"`
	CMT13 float64   `db:"CMT13"`
	CML01 float64   `db:"CML01"`
	CML02 float64   `db:"CML02"`
	CML03 float64   `db:"CML03"`
	CML04 float64   `db:"CML04"`
	CML05 float64   `db:"CML05"`
	CML06 float64   `db:"CML06"`
	CML07 float64   `db:"CML07"`
	CML08 float64   `db:"CML08"`
	CML09 float64   `db:"CML09"`
	CML10 float64   `db:"CML10"`
	CML11 float64   `db:"CML11"`
	CML12 float64   `db:"CML12"`
	CML13 float64   `db:"CML13"`
	CMBUY int       `db:"CMBUY"`
	CMPPL string    `db:"CMPPL"`
	CMRPO string    `db:"CMRPO"`
	TXLI1 string    `db:"TXLI1"`
	TXLI2 string    `db:"TXLI2"`
	TXLI3 string    `db:"TXLI3"`
	CCRCD string    `db:"CCRCD"`
	FAMDU float64   `db:"FAMDU"`
	FOROP float64   `db:"FOROP"`
	FHIGH int       `db:"FHIGH"`
	FPYAM float64   `db:"FPYAM"`
	FMTDT float64   `db:"FMTDT"`
	FYTDT float64   `db:"FYTDT"`
	FPYDT float64   `db:"FPYDT"`
	FMTDL float64   `db:"FMTDL"`
	FYTDL float64   `db:"FYTDL"`
	FPYDL float64   `db:"FPYDL"`
	FMTDC float64   `db:"FMTDC"`
	FYTDC float64   `db:"FYTDC"`
	FPYSL float64   `db:"FPYSL"`
	FPYDC float64   `db:"FPYDC"`
	FMTFT float64   `db:"FMTFT"`
	FYTFT float64   `db:"FYTFT"`
	FPYFT float64   `db:"FPYFT"`
	FCT01 float64   `db:"FCT01"`
	FCT02 float64   `db:"FCT02"`
	FCT03 float64   `db:"FCT03"`
	FCT04 float64   `db:"FCT04"`
	FCT05 float64   `db:"FCT05"`
	FCT06 float64   `db:"FCT06"`
	FCT07 float64   `db:"FCT07"`
	FCT08 float64   `db:"FCT08"`
	FCT09 float64   `db:"FCT09"`
	FCT10 float64   `db:"FCT10"`
	FCT11 float64   `db:"FCT11"`
	FCT12 float64   `db:"FCT12"`
	FCT13 float64   `db:"FCT13"`
	FCL01 float64   `db:"FCL01"`
	FCL02 float64   `db:"FCL02"`
	FCL03 float64   `db:"FCL03"`
	FCL04 float64   `db:"FCL04"`
	FCL05 float64   `db:"FCL05"`
	FCL06 float64   `db:"FCL06"`
	FCL07 float64   `db:"FCL07"`
	FCL08 float64   `db:"FCL08"`
	FCL09 float64   `db:"FCL09"`
	FCL10 float64   `db:"FCL10"`
	FCL11 float64   `db:"FCL11"`
	FCL12 float64   `db:"FCL12"`
	FCL13 float64   `db:"FCL13"`
	FCMWK float64   `db:"FCMWK"`
	FAVPY int       `db:"FAVPY"`
	FAVIN int       `db:"FAVIN"`
	FAVBL int       `db:"FAVBL"`
	CDFOT string    `db:"CDFOT"`
	CMOR1 string    `db:"CMOR1"`
	CMOR2 string    `db:"CMOR2"`
	CQOCF string    `db:"CQOCF"`
	FDTWK float64   `db:"FDTWK"`
	CDTWK float64   `db:"CDTWK"`
	FDLWK float64   `db:"FDLWK"`
	CDLWK float64   `db:"CDLWK"`
	FFTWK float64   `db:"FFTWK"`
	CFTWK float64   `db:"CFTWK"`
	CTAWK float64   `db:"CTAWK"`
	FDSWK float64   `db:"FDSWK"`
	CDSWK float64   `db:"CDSWK"`
	FDFWK float64   `db:"FDFWK"`
	CDFWK float64   `db:"CDFWK"`
	FDXWK float64   `db:"FDXWK"`
	CDXWK float64   `db:"CDXWK"`
	CPSEC time.Time `db:"CPSEC"`
	CMDAD float64   `db:"CMDAD"`
	CMDFQ string    `db:"CMDFQ"`
	CMSLF string    `db:"CMSLF"`
	CMSPD int       `db:"CMSPD"`
	CMSBS int       `db:"CMSBS"`
	CMAFR int       `db:"CMAFR"`
	CMPFR int       `db:"CMPFR"`
	CMPCO string    `db:"CMPCO"`
	CMPRA int       `db:"CMPRA"`
	CMEDI string    `db:"CMEDI"`
	CMCT1 int       `db:"CMCT1"`
	CMCT2 int       `db:"CMCT2"`
	CMCT3 int       `db:"CMCT3"`
	CMCT4 int       `db:"CMCT4"`
	CMCT5 int       `db:"CMCT5"`
	CMCT6 int       `db:"CMCT6"`
	CMSAG float64   `db:"CMSAG"`
	CMFGS float64   `db:"CMFGS"`
	CMFGC float64   `db:"CMFGC"`
	CMFGE float64   `db:"CMFGE"`
	CMCRB time.Time `db:"CMCRB"`
	CMPYB time.Time `db:"CMPYB"`
	CMRAN string    `db:"CMRAN"`
	CMCDR string    `db:"CMCDR"`
	CMCEC time.Time `db:"CMCEC"`
	CMAOC time.Time `db:"CMAOC"`
	CMAAR int       `db:"CMAAR"`
	CMCOG int       `db:"CMCOG"`
	IVEDI string    `db:"IVEDI"`
	CMCDE string    `db:"CMCDE"`
	CMDUN string    `db:"CMDUN"`
	CMDBS string    `db:"CMDBS"`
	CMMSG string    `db:"CMMSG"`
	CMLTR string    `db:"CMLTR"`
	CMMGR string    `db:"CMMGR"`
	CMCCR string    `db:"CMCCR"`
	CMCRC time.Time `db:"CMCRC"`
	CMPCR string    `db:"CMPCR"`
	CMPYD time.Time `db:"CMPYD"`
	CMAGY string    `db:"CMAGY"`
	CMRSK string    `db:"CMRSK"`
	CMEDT time.Time `db:"CMEDT"`
	CMODT time.Time `db:"CMODT"`
	CMCN1 string    `db:"CMCN1"`
	CMPH1 string    `db:"CMPH1"`
	CMCN2 string    `db:"CMCN2"`
	CMPH2 string    `db:"CMPH2"`
	CMDFC string    `db:"CMDFC"`
	CMAD1 string    `db:"CMAD1"`
	CMAD2 string    `db:"CMAD2"`
	CMAD3 string    `db:"CMAD3"`
	CMCTY string    `db:"CMCTY"`
	CMDST string    `db:"CMDST"`
	CMDZP string    `db:"CMDZP"`
	CMCTR string    `db:"CMCTR"`
	CMSEC string    `db:"CMSEC"`
	CMSUP string    `db:"CMSUP"`
	CMTXI string    `db:"CMTXI"`
	CMFRC string    `db:"CMFRC"`
	CMCAR string    `db:"CMCAR"`
	CMMOD string    `db:"CMMOD"`
	CMLNG string    `db:"CMLNG"`
}

func main() {
	var err error
	var (
		odbcConnectStr  string
		mysqlConnectStr string
		startDate       string
		endDate         string
	)

	if len(os.Args) == 1 {
		odbcConnectStr, mysqlConnectStr, startDate, endDate = getInputs()
		dateStart, dateEnd = checkDates(startDate, endDate)
	} else if len(os.Args) != 5 {
		panic("Argument count less than 4")
	} else {
		odbcConnectStr = os.Args[1]
		mysqlConnectStr = os.Args[2]
		startDate = os.Args[3]
		endDate = os.Args[4]
		dateStart, dateEnd = checkDates(startDate, endDate)
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

	processCustomerTable()
}

func getInputs() (string, string, string, string) {
	startDate := ""
	endDate := ""

	// get credentials to AS400
	dsn := getInput("AS400 Server DSN : ")
	userAS, pwd, err := getCredentials()
	if err != nil {
		panic(err)
	}

	odbcConnectStr := fmt.Sprintf("DSN=%s; UID=%s; PWD=%s", dsn, userAS, pwd)

	// get credentials to Mysql
	mysqlIP := getInput("\nMySQL Server IP : ")
	user, pwd, err := getCredentials()
	if err != nil {
		panic(err)
	}
	mysqlConnectStr := fmt.Sprintf("%s:%s@tcp(%s:3306)/prms_ap?charset=utf8&parseTime=True&loc=Local",
		user, pwd, mysqlIP)

	// read REFXDAT file in MDMOD# library to get the Start Date & End Date
	sql := `SELECT SDAT, EDAT FROM mdmod#.refxdat`
	err = dbOdbc.QueryRow(sql).Scan(&startDate, &endDate)
	if err != nil {
		panic(err)
	}
	return odbcConnectStr, mysqlConnectStr, startDate, endDate
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

func processCustomerTable() {
	cust := Customer{}
	cust2 := Customer2{}
	fields := DBFields(Customer{})
	fieldsCsv := fieldsCSV(fields)
	// fieldsCsvColons := fieldsCSVColons(fields)

	fields2 := DBFields(Customer2{})
	fieldsCsv2 := fieldsCSV(fields2)
	fieldsCsvColons2 := fieldsCSVColons(fields2)

	selectStmt := fmt.Sprintf("SELECT %s FROM RMSMDFL#.MSCMP100", fieldsCsv)
	rows, err := dbOdbc.Queryx(selectStmt)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	newCustomerTable := getNewCustomerTableName()
	createCustomerTable(newCustomerTable)

	insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", newCustomerTable, fieldsCsv2,
		fieldsCsvColons2)
	recCount := 0
	fmt.Printf("\nTable Name : %s\n", newCustomerTable)
	fmt.Printf("Record # : %8d", recCount)
	for rows.Next() {
		recCount++
		fmt.Printf("\b\b\b\b\b\b\b\b")
		fmt.Printf("%8d", recCount)
		err = rows.StructScan(&cust)
		if err != nil {
			panic(err)
		}

		cust2 = Customer2(cust)
		_, err = dbMysql.NamedExec(insertStmt, cust2)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println()
}

func getNewCustomerTableName() string {
	newDate := dateEnd.Format("200601")

	newTableName := "mscmp100_" + newDate[:6]
	return newTableName
}

func createCustomerTable(tlbName string) {
	var dbErr *mysql.MySQLError

	stmt := `CREATE TABLE ` + tlbName + ` (
		ACTIV char(1),
		CMPNO int(10),
		PLTNO int(10),
		CUSNO int(10),
		CNAME varchar(25),
		CADD1 varchar(25),
		CADD2 varchar(25),
		CADDX varchar(25),
		CADD3 varchar(16),
		CSTTE varchar(2),
		CZIPC varchar(10),
		CNTAC varchar(25),
		CPHON decimal(10),
		CPHOA varchar(16),
		CTLXN varchar(13),
		CFAXN varchar(16),
		CCUSN int(10),
		BLLCS int(10),
		CMSDT date,
		CTYPE varchar(2),
		CRHLD char(1),
		CRLIM decimal(13),
		CRDAY int(10),
		FCHFG char(1),
		CMASF char(1),
		PSTFG char(1),
		AMTDU decimal(15,2),
		OROPN decimal(13,2),
		HIGHB decimal(13),
		PAYAM decimal(15,2),
		PAYDT date,
		PURDT date,
		MTDDT decimal(13,2),
		YTDDT decimal(15,2),
		PYTDT decimal(15,2),
		MTDDL decimal(13,2),
		YTDDL decimal(15,2),
		PYTDL decimal(15,2),
		MTDDC decimal(13,2),
		YTDDC decimal(15,2),
		PYTSL decimal(15,2),
		PYTDC decimal(15,2),
		MTDFT decimal(15,2),
		YTDFT decimal(15,2),
		PYTFT decimal(15,2),
		MTDTA decimal(15,2),
		YTDTA decimal(15,2),
		PYTTA decimal(15,2),
		CST01 decimal(13,2),
		CST02 decimal(13,2),
		CST03 decimal(13,2),
		CST04 decimal(13,2),
		CST05 decimal(13,2),
		CST06 decimal(13,2),
		CST07 decimal(13,2),
		CST08 decimal(13,2),
		CST09 decimal(13,2),
		CST10 decimal(13,2),
		CST11 decimal(13,2),
		CST12 decimal(13,2),
		CST13 decimal(13,2),
		CSL01 decimal(13,2),
		CSL02 decimal(13,2),
		CSL03 decimal(13,2),
		CSL04 decimal(13,2),
		CSL05 decimal(13,2),
		CSL06 decimal(13,2),
		CSL07 decimal(13,2),
		CSL08 decimal(13,2),
		CSL09 decimal(13,2),
		CSL10 decimal(13,2),
		CSL11 decimal(13,2),
		CSL12 decimal(13,2),
		CSL13 decimal(13,2),
		CMSWK decimal(13,2),
		DSCDE int(10),
		DSCTF char(1),
		DSPCT decimal(5,3),
		CMAGL decimal(15),
		CMDGL decimal(15),
		CMBGL decimal(15),
		CMSGL decimal(15),
		CPCUS int(10),
		TXBLE char(1),
		TXBL2 char(1),
		TXBL3 char(1),
		TAX01 varchar(4),
		TAX02 varchar(4),
		TAX03 varchar(4),
		REGON int(10),
		SALNO int(10),
		SHVIA varchar(10),
		DFTWH varchar(2),
		CMTRM varchar(2),
		CASHO char(1),
		CTEXT char(1),
		ARDAY int(10),
		AVPAY decimal(13),
		AVINV decimal(13),
		AVBAL decimal(13),
		DIVSN varchar(2),
		CTYP1 varchar(2),
		CTYP2 varchar(2),
		BACKM char(1),
		ALPHA varchar(6),
		CCOMM decimal(5,3),
		AGING char(1),
		MDCDC decimal(13,2),
		YDCDC decimal(15,2),
		PYCSL decimal(15,2),
		PYCDC decimal(15,2),
		MDCFT decimal(15,2),
		YDCFT decimal(15,2),
		PYCFT decimal(15,2),
		MTDCT decimal(15,2),
		YTDCT decimal(15,2),
		PYTCT decimal(15,2),
		CROPN decimal(13,2),
		CMRGL decimal(15),
		CFCDM decimal(13,2),
		CFCDY decimal(15,2),
		CFCPY decimal(15,2),
		CFCPD decimal(15,2),
		CFFCM decimal(15,2),
		CFFCY decimal(15,2),
		PYCFC decimal(15,2),
		CFCTM decimal(15,2),
		CFCTY decimal(15,2),
		CFPTY decimal(15,2),
		CFOPN decimal(15,2),
		CFC01 decimal(15,2),
		CFC02 decimal(15,2),
		CFC03 decimal(15,2),
		CFC04 decimal(15,2),
		CFC05 decimal(15,2),
		CFC06 decimal(15,2),
		CFC07 decimal(15,2),
		CFC08 decimal(15,2),
		CFC09 decimal(15,2),
		CFC10 decimal(15,2),
		CFC11 decimal(15,2),
		CFC12 decimal(15,2),
		CFC13 decimal(15,2),
		CFL01 decimal(15,2),
		CFL02 decimal(15,2),
		CFL03 decimal(15,2),
		CFL04 decimal(15,2),
		CFL05 decimal(15,2),
		CFL06 decimal(15,2),
		CFL07 decimal(15,2),
		CFL08 decimal(15,2),
		CFL09 decimal(15,2),
		CFL10 decimal(15,2),
		CFL11 decimal(15,2),
		CFL12 decimal(15,2),
		CFL13 decimal(15,2),
		CMT01 decimal(13,2),
		CMT02 decimal(13,2),
		CMT03 decimal(13,2),
		CMT04 decimal(13,2),
		CMT05 decimal(13,2),
		CMT06 decimal(13,2),
		CMT07 decimal(13,2),
		CMT08 decimal(13,2),
		CMT09 decimal(13,2),
		CMT10 decimal(13,2),
		CMT11 decimal(13,2),
		CMT12 decimal(13,2),
		CMT13 decimal(13,2),
		CML01 decimal(13,2),
		CML02 decimal(13,2),
		CML03 decimal(13,2),
		CML04 decimal(13,2),
		CML05 decimal(13,2),
		CML06 decimal(13,2),
		CML07 decimal(13,2),
		CML08 decimal(13,2),
		CML09 decimal(13,2),
		CML10 decimal(13,2),
		CML11 decimal(13,2),
		CML12 decimal(13,2),
		CML13 decimal(13,2),
		CMBUY int(10),
		CMPPL char(1),
		CMRPO char(1),
		TXLI1 varchar(15),
		TXLI2 varchar(15),
		TXLI3 varchar(15),
		CCRCD varchar(3),
		FAMDU decimal(15,2),
		FOROP decimal(13,2),
		FHIGH decimal(13),
		FPYAM decimal(15,2),
		FMTDT decimal(13,2),
		FYTDT decimal(15,2),
		FPYDT decimal(15,2),
		FMTDL decimal(13,2),
		FYTDL decimal(15,2),
		FPYDL decimal(15,2),
		FMTDC decimal(13,2),
		FYTDC decimal(15,2),
		FPYSL decimal(15,2),
		FPYDC decimal(15,2),
		FMTFT decimal(15,2),
		FYTFT decimal(15,2),
		FPYFT decimal(15,2),
		FCT01 decimal(13,2),
		FCT02 decimal(13,2),
		FCT03 decimal(13,2),
		FCT04 decimal(13,2),
		FCT05 decimal(13,2),
		FCT06 decimal(13,2),
		FCT07 decimal(13,2),
		FCT08 decimal(13,2),
		FCT09 decimal(13,2),
		FCT10 decimal(13,2),
		FCT11 decimal(13,2),
		FCT12 decimal(13,2),
		FCT13 decimal(13,2),
		FCL01 decimal(13,2),
		FCL02 decimal(13,2),
		FCL03 decimal(13,2),
		FCL04 decimal(13,2),
		FCL05 decimal(13,2),
		FCL06 decimal(13,2),
		FCL07 decimal(13,2),
		FCL08 decimal(13,2),
		FCL09 decimal(13,2),
		FCL10 decimal(13,2),
		FCL11 decimal(13,2),
		FCL12 decimal(13,2),
		FCL13 decimal(13,2),
		FCMWK decimal(13,2),
		FAVPY decimal(13),
		FAVIN decimal(13),
		FAVBL decimal(13),
		CDFOT varchar(3),
		CMOR1 varchar(2),
		CMOR2 varchar(2),
		CQOCF char(1),
		FDTWK decimal(13,2),
		CDTWK decimal(13,2),
		FDLWK decimal(13,2),
		CDLWK decimal(13,2),
		FFTWK decimal(13,2),
		CFTWK decimal(13,2),
		CTAWK decimal(13,2),
		FDSWK decimal(13,2),
		CDSWK decimal(13,2),
		FDFWK decimal(13,2),
		CDFWK decimal(13,2),
		FDXWK decimal(13,2),
		CDXWK decimal(13,2),
		CPSEC date,
		CMDAD decimal(15,2),
		CMDFQ char(1),
		CMSLF char(1),
		CMSPD int(10),
		CMSBS int(10),
		CMAFR int(10),
		CMPFR int(10),
		CMPCO varchar(3),
		CMPRA int(10),
		CMEDI char(1),
		CMCT1 int(10),
		CMCT2 int(10),
		CMCT3 int(10),
		CMCT4 int(10),
		CMCT5 int(10),
		CMCT6 int(10),
		CMSAG decimal(15),
		CMFGS decimal(15),
		CMFGC decimal(15),
		CMFGE decimal(15),
		CMCRB date,
		CMPYB date,
		CMRAN varchar(11),
		CMCDR varchar(3),
		CMCEC date,
		CMAOC date,
		CMAAR decimal(15),
		CMCOG decimal(15),
		IVEDI char(1),
		CMCDE varchar(2),
		CMDUN char(1),
		CMDBS char(1),
		CMMSG char(1),
		CMLTR char(1),
		CMMGR varchar(4),
		CMCCR varchar(4),
		CMCRC date,
		CMPCR varchar(4),
		CMPYD date,
		CMAGY varchar(20),
		CMRSK varchar(3),
		CMEDT date,
		CMODT date,
		CMCN1 varchar(25),
		CMPH1 varchar(16),
		CMCN2 varchar(25),
		CMPH2 varchar(16),
		CMDFC char(1),
		CMAD1 varchar(30),
		CMAD2 varchar(30),
		CMAD3 varchar(30),
		CMCTY varchar(25),
		CMDST varchar(2),
		CMDZP varchar(10),
		CMCTR varchar(25),
		CMSEC varchar(16),
		CMSUP varchar(8),
		CMTXI varchar(11),
		CMFRC varchar(2),
		CMCAR varchar(4),
		CMMOD char(1),
		CMLNG varchar(3)
	) ENGINE=InnoDB DEFAULT CHARSET=latin1;
	`

	_, err := dbMysql.Exec(stmt)
	if err != nil {
		if errors.As(err, &dbErr) {
			// table already exists error
			if dbErr.Number == 1050 {
				clearTable(tlbName)
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
