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
	err := client.PutTest("foo", "bar")
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


	err = client.DelTest("foo")
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

	ok, err := client.TryPut("foo", "bar1", "tx1")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(ok)

	ok, err = client.TryPut("foo", "bar2", "tx2")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(ok)

	success, err := client.Commit("tx1")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(success)

}



func Test_Status(t *testing.T) {
	client := NewMasterClient(MasterAddr)
	err := client.PutTest("foo", "bar")
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
	ok, err := client.Commit("tx1")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(ok)
}


func TestReplicaShouldAbortIfDelOnLockedKey(t *testing.T) {

	client, err := NewReplicaClient(GetReplicaHost(0))
	if err != nil {
		log.Fatal(err)
	}

	ok, err := client.TryPut("foo", "bar1", "tx1")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(ok)

	ok, err = client.TryDel("foo", "tx2")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(ok)
}

func TestReplica_TryPut(t *testing.T) {
	client, err := NewReplicaClient(GetReplicaHost(0))
	if err != nil {
		log.Fatal(err)
	}
	ok, err := client.TryPut("foo", "bar2", "tx1")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(ok)
}

func TestReplica_Get(t *testing.T) {
	client, err := NewReplicaClient(GetReplicaHost(0))
	if err != nil {
		log.Fatal(err)
	}
	val,err := client.Get("foo")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(*val)
}

func TestReplica_Abort(t *testing.T) {
	client, err := NewReplicaClient(GetReplicaHost(0))
	if err != nil {
		log.Fatal(err)
	}

	ok, err := client.Abort("tx1")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(ok)
}

func TestReplica_Commit(t *testing.T) {
	client, err := NewReplicaClient(GetReplicaHost(0))
	if err != nil {
		log.Fatal(err)
	}

	ok, err := client.Commit("tx1")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(ok)
}

func Test_Put(t *testing.T) {
	client := NewMasterClient(MasterAddr)

	err := client.Put("foo", "xxxxxxxxx")
	if err != nil {
		log.Fatal(err)
	}
}

func Test_Get(t *testing.T) {
	client := NewMasterClient(MasterAddr)

	val,err := client.Get("TestGetKe2y22223222212")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(*val)
}
