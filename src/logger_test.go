package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path"
	"testing"
)

var testLogPath = "/Users/jianweili/go/src/github.com/blastbao/gotwopc/log.csv"

var lgr *logger

func InitLogger() {
	lgr = newLogger(testLogPath)
}

func TestWriteOp(t *testing.T) {
	InitLogger()
	entry := logEntry{
		txId: "xxxxxx",
		state: Started,
		op: PutOp,
		key: "test key",
	}
	lgr.writeOp(entry.txId, entry.state, entry.op, entry.key)
}

func TestRead(t *testing.T) {
	InitLogger()
	entries, err := lgr.read()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(entries)
}

func TestFileOperator(t *testing.T) {

	err := os.MkdirAll(path.Dir(testLogPath), 0)
	file, err := os.OpenFile(testLogPath, os.O_APPEND|os.O_WRONLY, 0777)
	if err != nil {
		log.Fatalln("newLogger:", err)
	}

	n, err := file.Write([]byte("hello world"))
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(n)


	l := csv.NewWriter(file)

	err = l.Write([]string{"xxx", "xxxxx"})
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println("ok")
}
