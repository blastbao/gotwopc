package main

import (
	"log"
	"sync"
	"testing"
)



func TestCmds_Run(t *testing.T) {
	client := NewMasterClient(MasterAddr)
	rsp, err := client.Ping("whatever")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(*rsp)
}

func TestCmds_Put(t *testing.T) {

	client := NewMasterClient(MasterAddr)
	err := client.PutTest("foo", "bar", MasterDontDie, make([]ReplicaDeath, 4))
	if err != nil {
		log.Fatal(err)
	}

	ReplicaCount := 3

	// every replica should have the value
	var wg sync.WaitGroup
	wg.Add(ReplicaCount)
	for i := 0; i < ReplicaCount; i++ {
		go func(i int) {
			val, err := client.GetTest("foo", i)
			if err != nil {
				log.Fatal(err)
			}
			wg.Done()
			log.Println(*val)
		}(i)
	}
	wg.Wait()


	err = client.DelTest("foo", MasterDontDie, make([]ReplicaDeath, 4))
	if err != nil {
		log.Fatal(err)
	}

	// no replica should have the value
	wg.Add(ReplicaCount)
	for i := 0; i < ReplicaCount; i++ {
		go func(i int) {
			_, err := client.GetTest("foo", i)
			if err == nil {
				log.Fatal("Del failed.")
			}
			wg.Done()
			if err != nil {
				log.Fatal(err)
			}
		}(i)
	}
	wg.Wait()
}

func TestReplicaShouldAbortIfPutOnLockedKey(t *testing.T) {

	client, err := NewReplicaClient(GetReplicaHost(0))
	if err != nil {
		log.Fatal(err)
	}

	ok, err := client.TryPut("foo", "bar1", "tx1", ReplicaDontDie)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(*ok)

	ok, err = client.TryPut("foo", "bar2", "tx2", ReplicaDontDie)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(*ok)

	ok, err = client.Commit("tx1", ReplicaDontDie)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(*ok)

}



func Test_Status(t *testing.T) {
	client := NewMasterClient(MasterAddr)
	err := client.PutTest("foo", "bar", MasterDontDie, make([]ReplicaDeath, 4))
	if err != nil {
		log.Fatal(err)
	}
	ok, err := client.Status("tx1")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(*ok)
}



func TestReplicaShouldErrOnUnknownTxCommit(t *testing.T) {
	client, err := NewReplicaClient(GetReplicaHost(0))
	if err != nil {
		log.Fatal(err)
	}
	ok, err := client.Commit("tx1", ReplicaDontDie)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(*ok)
}


func TestReplicaShouldAbortIfDelOnLockedKey(t *testing.T) {

	client, err := NewReplicaClient(GetReplicaHost(0))
	if err != nil {
		log.Fatal(err)
	}

	ok, err := client.TryPut("foo", "bar1", "tx1", ReplicaDontDie)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(*ok)

	ok, err = client.TryDel("foo", "tx2", ReplicaDontDie)
	if err != nil {
		log.Fatal(err)
	}
	log.Println(*ok)
}