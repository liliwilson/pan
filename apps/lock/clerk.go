package lock

import (
	"pan/panapi"
	"pan/panapi/rpc"
	"strconv"
)

type Clerk struct {
	session     panapi.IPNSession
	lockDir     rpc.Ppath
	lockSuffix  rpc.Ppath
	currentFile rpc.Ppath
}

func MakeClerk(session panapi.IPNSession, lockDir rpc.Ppath, lockSuffix rpc.Ppath) *Clerk {
	ck := &Clerk{session: session, lockDir: lockDir, lockSuffix: lockSuffix}
	return ck
}

// Returns true iff candidate is the child with the smallest sequence number
func isSmallestSequence(children []rpc.Ppath, candidate rpc.Ppath) bool {
	candidateSeq := candidate.GetSeqNumber()
	for _, child := range children {
		childSeq := child.GetSeqNumber()
		if childSeq < candidateSeq {
			return false
		}
	}
	return true
}

// Returns the name of the node that a client should be watching
func (ck *Clerk) watchNode(children []rpc.Ppath) rpc.Ppath {
	myNum := ck.currentFile.GetSeqNumber()
	current := -1
	for _, child := range children {
		childNum := child.GetSeqNumber()
		if childNum >= myNum {
			continue
		} else {
			current = max(current, childNum)
		}
	}
	return ck.lockDir + ck.lockSuffix + rpc.Ppath(strconv.Itoa(current))
}

// Acquire the lock for the fs
func (ck *Clerk) Acquire() {
	lockPrefix := ck.lockDir + ck.lockSuffix
	fname, _ := ck.session.Create(lockPrefix, "", rpc.Flag{Sequential: true, Ephemeral: true})
	ck.currentFile = fname
	for {
		children, _ := ck.session.GetChildren(ck.lockDir, rpc.Watch{})
		if isSmallestSequence(children, ck.currentFile) {
			return
		}
		nodeToWatch := ck.watchNode(children)
		ch_wait := make(chan struct{})
		exists, _ := ck.session.Exists(
			nodeToWatch,
			rpc.Watch{
				ShouldWatch: true,
				Callback: func(_ rpc.WatchArgs) {
					ch_wait <- struct{}{}
				},
			},
		)
		if exists {
			<-ch_wait
		}

	}
}

// Release the lock for the fs
func (ck *Clerk) Release() {
	ck.session.Delete(ck.currentFile, 1)
	ck.currentFile = ""
}
