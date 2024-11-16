package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.RWMutex

	kvs map[string]string

	appendClientHistory map[string]*clientHistory
}

type clientHistory struct {
	opIndex uint
	value   string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	reply.Value = kv.kvs[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.modifyReqHandledBefore(args.ClientMeta.EndName,
		args.ClientMeta.OpIndex) {
		return
	}

	kv.kvs[args.Key] = args.Value
	kv.addModifyHistory(args.ClientMeta.EndName,
		args.ClientMeta.OpIndex, "")
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.modifyReqHandledBefore(args.ClientMeta.EndName,
		args.ClientMeta.OpIndex) {
		reply.Value = kv.getLastAppendHistoryValue(args.ClientMeta.EndName)
		return
	}

	originalValue := kv.kvs[args.Key]
	kv.kvs[args.Key] = originalValue + args.Value

	kv.addModifyHistory(args.ClientMeta.EndName, args.ClientMeta.OpIndex,
		originalValue)

	reply.Value = originalValue
}

func (kv *KVServer) modifyReqHandledBefore(clientName string, opIndex uint) bool {
	appendHistory := kv.appendClientHistory[clientName]
	if appendHistory == nil {
		return false
	}
	return appendHistory.opIndex >= opIndex
}

func (kv *KVServer) getLastAppendHistoryValue(clientName string) string {
	return kv.appendClientHistory[clientName].value
}

func (kv *KVServer) addModifyHistory(
	clientName string, opIndex uint, value string) {
	kv.appendClientHistory[clientName] = &clientHistory{
		opIndex: opIndex,
		value:   value,
	}
}

func StartKVServer() *KVServer {
	return &KVServer{
		mu:                  sync.RWMutex{},
		kvs:                 make(map[string]string),
		appendClientHistory: make(map[string]*clientHistory),
	}
}
