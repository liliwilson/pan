package pan

import (
	"fmt"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type PanServer struct {
	me    int
	dead  int32 // set by Kill(); required for tester
	rsm   *rsm.RSM
	peers []*labrpc.ClientEnd

	// Your definitions here
}

func (pn *PanServer) DoOp(req any) any {
	return ""
}

func (pn *PanServer) Restore([]byte) {}

func (pn *PanServer) Snapshot() []byte {
	return make([]byte, 0)
}

func (pn *PanServer) Create(args *Args, reply *Reply) {
	fmt.Println("got an rpc!")
	for i := 0; i < len(pn.peers); i++ {
		if i == pn.me {
			continue
		}
		args := Args{}
		reply := Reply{}
		pn.peers[i].Call("PanServer.TalkToSomeoneElse", &args, &reply)
	}
}

func (pn *PanServer) TalkToSomeoneElse(args *Args, reply *Reply) {
	fmt.Println("Got message from peer")
}

func (pn *PanServer) Kill() {
	atomic.StoreInt32(&pn.dead, 1)
}

func (pn *PanServer) killed() bool {
	z := atomic.LoadInt32(&pn.dead)
	return z == 1
}

// Must return quickly
func StartPanServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	pn := &PanServer{me: me, peers: servers}
	pn.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, pn)

	return []tester.IService{pn, pn.rsm.Raft()}
}
