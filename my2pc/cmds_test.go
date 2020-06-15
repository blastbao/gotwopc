package main

import (
	"io"
	"log"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"testing"
)

func TestCmds_Run(t *testing.T) {
	StartMaster()
	//StartReplicas(true)
}

const ReplicaCount = 4
var masterCmd *exec.Cmd
var replicas = [ReplicaCount]*exec.Cmd{}

func StartMaster() {
	//masterCmd = ExecCmd("./my2pc", "-m", "-n", strconv.Itoa(ReplicaCount))
	client := NewMasterClient(MasterPort)
	rsp, err := client.Ping("whatever")
	if err != nil {
		log.Fatal(err)
	}
	log.Println(*rsp)
}

func StartReplicas(shouldRestart bool) {
	var wg sync.WaitGroup
	for i := 0; i < ReplicaCount; i++ {
		wg.Add(1)
		go func(i int) {
			startReplica(i, shouldRestart)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func startReplica(n int, shouldRestart bool) {

	replicas[n] = ExecCmd("./my2pc", "-r", "-i", strconv.Itoa(n))
	client := NewReplicaClient(GetReplicaHost(n))
	if shouldRestart {
		go func(cmd *exec.Cmd) {
			if cmd != nil {
				cmd.Wait()
				if replicas[n] != nil {
					startReplica(n, shouldRestart)
				}
			}
		}(replicas[n])
	}
	_, err := client.Ping("whatever")
	if err != nil {
		log.Fatal(err)
	}
}


func ExecCmd(path string, args ...string) *exec.Cmd {
	cmd := exec.Command(path, args...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatal(err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		log.Fatal(err)
	}
	err = cmd.Start()
	if err != nil {
		log.Fatal(err)
	}

	go io.Copy(os.Stdout, stdout)
	go io.Copy(os.Stderr, stderr)

	return cmd
}
