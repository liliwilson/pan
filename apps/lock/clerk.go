package lock

import (
	"pan/panapi"
	"pan/panapi/rpc"
)

type Clerk struct{}

func MakeClerk(session panapi.IPNSession, lockSuffix rpc.Ppath) *Clerk {
	ck := &Clerk{}
	return ck
}

// Acquire the lock for a given path; this locks all children as well
func (ck *Clerk) Acquire(path rpc.Ppath) {}

// Release the lock for a given path; requires having the lock already
func (ck *Clerk) Release(path rpc.Ppath) {}
