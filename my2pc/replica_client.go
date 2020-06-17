package main

import (
	"errors"

	log "github.com/sirupsen/logrus"
	"net"
	"net/rpc"
)

type ReplicaClient struct {
	host      string
	rpcClient *rpc.Client
}

func NewReplicaClient(host string) (*ReplicaClient,error) {
	client := &ReplicaClient{
		host,
		nil,
	}
	return client, client.tryConnect()
}

func (c *ReplicaClient) tryConnect() (err error) {

	if c == nil {
		return errors.New("c == nil")
	}

	if c.rpcClient != nil {
		return
	}

	rpcClient, err := rpc.DialHTTP("tcp", c.host)
	if err != nil {
		return
	}
	c.rpcClient = rpcClient
	return
}

func (c *ReplicaClient) call(serviceMethod string, args interface{}, reply interface{}) (err error) {
	err = c.rpcClient.Call(serviceMethod, args, reply)
	_, isNetOpError := err.(*net.OpError)
	if err == rpc.ErrShutdown || isNetOpError {
		c.rpcClient = nil
	}
	return
}

func (c *ReplicaClient) TryPut(key string, value string, txid string) (success bool, err error) {

	logger := log.WithFields(log.Fields{"key":key, "value":value})

	if err = c.tryConnect(); err != nil {
		logger.WithError(err).Error("[ReplicaClient][TryPut] c.tryConnect() error.")
		return
	}

	var reply ReplicaActionResult
	err = c.call("Replica.TryPut", &TxPutArgs{ key, value, txid}, &reply)
	if err != nil {
		logger.WithError(err).Error("[ReplicaClient][TryPut] call Replica.TryPut error.")
		return
	}

	success = reply.Success
	logger.WithField("success", success).Info("[ReplicaClient][TryPut] ok.")
	return
}

func (c *ReplicaClient) TryDel(key string, txid string) (success bool, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaActionResult
	err = c.call("Replica.TryDel", &TxDelArgs{ key, txid}, &reply)
	if err != nil {
		log.Println("ReplicaClient.TryDel:", err)
		return
	}

	success = reply.Success
	return
}

func (c *ReplicaClient) Commit(txid string) (Success bool, err error) {
	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaActionResult
	err = c.call("Replica.Commit", &CommitArgs{ txid }, &reply)
	if err != nil {
		log.Println("ReplicaClient.Commit:", err)
		return
	}

	Success = reply.Success
	return
}

func (c *ReplicaClient) Abort(txid string) (success bool, err error) {

	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaActionResult
	err = c.call("Replica.Abort", &AbortArgs{ txid }, &reply)
	if err != nil {
		log.Println("ReplicaClient.Abort:", err)
		return
	}

	success = reply.Success
	return
}

func (c *ReplicaClient) Get(key string) (Value *string, err error) {

	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaGetResult
	err = c.call("Replica.Get", &ReplicaKeyArgs{ key }, &reply)
	if err != nil {
		log.Println("ReplicaClient.Get:", err)
		return
	}

	Value = &reply.Value

	return
}

func (c *ReplicaClient) Ping(key string) (Value *string, err error) {

	if err = c.tryConnect(); err != nil {
		return
	}

	var reply ReplicaGetResult
	err = c.call("Replica.Ping", &ReplicaKeyArgs{ key }, &reply)
	if err != nil {
		log.Println("ReplicaClient.Ping:", err)
		return
	}

	Value = &reply.Value

	return
}
