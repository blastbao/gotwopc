package main


import (
	"fmt"
	flag "github.com/ogier/pflag"
	"log"
	"sync"
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

func StartReplica(id int) {
	replica := NewReplica(id)
	replica.run()
}

//func main() {
//	go StartMaster(3)
//	go StartReplicas(3)
//	time.Sleep(time.Hour)
//}

//
// master:
// 	 gb && ./my2pc -m -n 3
//
// replica:
//	 gb && ./my2pc -r -i 0
// 	 gb && ./my2pc -r -i 1
//	 gb && ./my2pc -r -i 2

func main() {

	isMaster 		:= flag.BoolP("master", "m", false, "start the master process")
	replicaCount 	:= flag.IntP("replicaCount", "n", 0, "replica count for master")
	isReplica 		:= flag.BoolP("replica", "r", false, "start a replica process")
	replicaId 	    := flag.IntP("replicaIndex", "i", 0, "replica index to run, starting at 0")
	flag.Parse()

	log.SetFlags(0) //log.Ltime | log.Lmicroseconds)
	log.Println(fmt.Sprintf("isMaster=%v, replicaCount=%d, isReplica=%v, replicaId=%d", *isMaster, *replicaCount, *isReplica, *replicaId))

	switch {
	case *isMaster:
		log.SetPrefix("[Master] ")
		StartMaster(*replicaCount)
	case *isReplica:
		log.SetPrefix(fmt.Sprintf("[Replica-%d]", *replicaId))
		StartReplica(*replicaId)
	default:
		flag.Usage()
	}
}