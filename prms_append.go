package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"gopkg.in/ini.v1"

	_ "github.com/alexbrainman/odbc"
	_ "github.com/lib/pq"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
	"golang.org/x/term"
)

func main() {
	timeStart := time.Now()

	dsn := "PRMS"

	fmt.Println("Server   : PRMS")
	userAS, pwd, err := getCredentials()

	// connect to AS400
	odbcConnectStr := fmt.Sprintf("DSN=%s; UID=%s; PWD=%s", dsn, userAS, pwd)
	dbOdbc, err := sql.Open("odbc", odbcConnectStr)
	if err != nil {
		panic(err)
	}
	err = dbOdbc.Ping()
	if err != nil {
		panic(err)
	}
	defer dbOdbc.Close()
	log.Println("Connected to PRMS")

	pqConnectStr, dbname := readIni()
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

	// call program to update customer table
	// cmd := exec.Command("./customer_update.exe", odbcConnectStr, pqConnectStr)
	// cmd.Stdout = os.Stdout
	// err = cmd.Run()

	// if err != nil {
	// 	panic(err)
	// }

	// call program to update vendor table
	cmd := exec.Command("./vendor_append.exe", odbcConnectStr, pqConnectStr, dbname)
	cmd.Stdout = os.Stdout
	err = cmd.Run()

	if err != nil {
		panic(err)
	}

	// call program to update ap detail table
	// cmd := exec.Command("./apdetail_append.exe", odbcConnectStr, pqConnectStr, dbname)
	// cmd.Stdout = os.Stdout
	// err = cmd.Run()

	// if err != nil {
	// 	panic(err)
	// }

	// call program to update ap header table
	// cmd = exec.Command("./apheader_append.exe", odbcConnectStr, pqConnectStr, dbname)
	// cmd.Stdout = os.Stdout
	// err = cmd.Run()

	// if err != nil {
	// 	panic(err)
	// }

	fmt.Println()
	log.Printf("Process done!\n")
	fmt.Printf("Elapsed Time : %v\n", time.Since(timeStart))
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
