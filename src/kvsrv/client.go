package kvsrv

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"os"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd

	clientName string

	opIndex uint

	mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	clientName := newClientName()

	return &Clerk{
		server:     server,
		clientName: clientName,
		opIndex:    0,
		mu:         sync.Mutex{},
	}
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	req := GetArgs{key}
	reply := GetReply{}

	ck.callSrv("KVServer.Get", &req, &reply)

	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	nextOpIndex := ck.nextOpIndex()

	req := PutAppendArgs{ClientMeta: ClientMeta{ck.clientName, nextOpIndex},
		Key: key, Value: value}
	reply := PutAppendReply{}

	ck.callSrv(fmt.Sprintf("KVServer.%s", op), &req, &reply)

	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

func (ck *Clerk) callSrv(rpcName string,
	args interface{}, reply interface{}) {
	for {
		if ok := ck.server.Call(rpcName, args, reply); ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) nextOpIndex() uint {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	ck.opIndex++

	return ck.opIndex
}

func newClientName() string {
	return fmt.Sprintf("%d-%x", os.Getpid(), nrand())
}
