package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"path"
)

// 日志条目
type Entry struct {
	Id  string				// 事务ID
	State TxState		// 事务状态
	Op    Operation	// 操作
	Key   string			// key
}

func ParseTx(e Entry) *Tx {
	t := &Tx{}
	t.Id = e.Id
	t.State = e.State
	t.Key = e.Key
	t.Op = e.Op
	return t

}

type logRequest struct {
	record []string		// 数据格式(csv): 事务ID, 事务状态, 操作, key
	done   chan int
}

type logger struct {
	path      string			// 文件路径
	file      *os.File			// 文件句柄
	csvWriter *csv.Writer		// csv 封装
	requests  chan *logRequest	// 请求管道
}

func newLogger(logFilePath string) *logger {

	err := os.MkdirAll(path.Dir(logFilePath), 0777)
	file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0777)
	if err != nil {
		log.Fatalln("newLogger:", err)
	}

	l := &logger{
		logFilePath,
		file,
		csv.NewWriter(file),
		make(chan *logRequest),
	}

	go l.loggingLoop()

	return l
}

func (l *logger) loggingLoop() {
	for {
		// 读取请求
		req := <-l.requests

		fmt.Println(req)

		// 写文件
		err := l.csvWriter.Write(req.record)
		if err != nil {
			log.Fatalln("logger.write fatal:", err)
		}
		// flush
		l.csvWriter.Flush()
		// 磁盘 sync
		err = l.file.Sync()
		if err != nil {
			log.Fatalln("logger.write fatal:", err)
		}
		// 回复
		req.done <- 1

		//log.Println("[logger][loggingLoop] ok.")
	}
}


func (l *logger) writeSpecial(directive string) {
	l.writeOp(directive, NoState, NoOp, "")
}

func (l *logger) writeState(txId string, state TxState) {
	l.writeOp(txId, state, NoOp, "")
}

func (l *logger) writeOp(txId string, state TxState, op Operation, key string) {
	record := []string{txId, state.String(), op.String(), key}
	done := make(chan int)
	l.requests <- &logRequest{record, done}
	<-done
	//fmt.Println("[logger][writeOp] ok .")
}


func (l *logger) read() (entries []Entry, err error) {
	entries = make([]Entry, 0)
	file, err := os.OpenFile(l.path, os.O_RDONLY, 0)
	if err != nil {
		if os.IsNotExist(err) {
			err = nil
		}
		return
	}
	r := csv.NewReader(file)
	records, err := r.ReadAll()
	if err != nil {
		return
	}

	for _, record := range records {
		entries = append(entries, Entry{record[0], ParseTxState(record[1]), ParseOperation(record[2]), record[3]})
	}
	return
}
