package main

import (
	"os"
	"log"
	"testing"
)

func Init() {
	log.SetPrefix("C  ")
	log.SetFlags(0)
	log.SetOutput(NewConditionalWriter())
}

func InitLog() {
	log.SetPrefix("C  ")
	log.SetFlags(0)
	log.SetOutput(NewConditionalWriter())
}

func Clear() {
	// Clean out data from old runs
	err := os.RemoveAll("data")
	if err != nil {
		log.Fatal("RemoveAll(data) failed: ", err)
	}
	err = os.RemoveAll("logs")
	if err != nil {
		log.Fatal("RemoveAll(logs) failed: ", err)
	}
}

func TestMain1(t *testing.T) {

}