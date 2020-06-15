package main

import (
	"fmt"
	"sync"
	"time"
)

const MasterAddr = "localhost:7175"
const ReplicaPortStart = 7176

func GetReplicaHost(replicaNum int) string {
	return fmt.Sprintf("localhost:%v", ReplicaPortStart+replicaNum)
}

func StartMaster(replicaNum int) {
	var addrs []string
	for i:=0;i<replicaNum;i++ {
		addrs = append(addrs, GetReplicaHost(i))
	}
	opt := &Option{
		Port: 7175,
		LogPath: "logs/master.txt",
		Replicas: addrs,
	}
	master := NewMaster(opt)
	master.run()
}

func StartReplicas(replicaNum int) {
	var wg sync.WaitGroup
	for i:=0;i<replicaNum;i++ {
		wg.Add(1)
		go func(i int) {
			replica := NewReplica(i)
			replica.run()
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func main() {
	go StartMaster(3)
	go StartReplicas(3)
	time.Sleep(time.Hour)
}
