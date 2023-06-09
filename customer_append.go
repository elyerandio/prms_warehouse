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
	_ "github.com/lib/pq"
)

var dbOdbc *sqlx.DB
var dbPostgre *sqlx.DB
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
		odbcConnectStr string
		pqConnectStr   string
		dbName         string
	)

	if len(os.Args) != 4 {
		panic("Argument count less than 3")
	} else {
		odbcConnectStr = os.Args[1]
		pqConnectStr = os.Args[2]
		dbName = os.Args[3]
	}

	// connect to AS400
	dbOdbc, err = sqlx.Open("odbc", odbcConnectStr)
	if err != nil {
		panic(err)
	}
	defer dbOdbc.Close()

	dbPostgre, err = sqlx.Open("postgres", pqConnectStr)
	if err != nil {
		panic(err)
	}
	defer dbPostgre.Close()

	appendCustomerTable(dbName)
}

func appendCustomerTable(dbName string) {
	var dbErr *pq.Error

	cust := Customer{}
	cust2 := Customer2{}
	fields := DBFields(Customer{})
	fieldsCsv := fieldsCSV(fields)
	// fieldsCsvColons := fieldsCSVColons(fields)

	fields2 := DBFields(Customer2{})
	fieldsCsv2 := fieldsCSV(fields2)
	fieldsCsvColons2 := fieldsCSVColons(fields2)
	fieldsUpdate := fieldsUpdate(fields2)

	selectStmt := fmt.Sprintf("SELECT %s FROM RMSMDFL#.MSCMP100", fieldsCsv)
	rows, err := dbOdbc.Queryx(selectStmt)
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	newCustomerTable := "mscmp100"

	insertStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", newCustomerTable, fieldsCsv2,
		fieldsCsvColons2)
	updateStmt := fmt.Sprintf("UPDATE mscmp100 SET %s WHERE cusno=:CUSNO", fieldsUpdate)

	recCount := 0
	updateCount := 0
	insertCount := 0

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
		_, err = dbPostgre.NamedExec(insertStmt, cust2)
		if err != nil {
			if errors.As(err, &dbErr) {
				// 23505 = Unique key violation
				// if unique key violation, update the record
				if dbErr.Code == "23505" {
					_, err = dbPostgre.NamedExec(updateStmt, cust2)
					if err != nil {
						fmt.Println()
						fmt.Println("Update error:", err)
						panic(err)
					} else {
						updateCount++
					}
				} else {
					fmt.Println()
					fmt.Println("Insert error:", err)
					panic(err)
				}
			} else {
				fmt.Println()
				fmt.Println(err)
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
