package pan

import (
	// "fmt"

	"pan/panapi"
	"6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
}

type Pan struct{}

type Args struct {}
type Reply struct {}

func (ck *Clerk) Create(path panapi.Ppath, data string, flags int) {
	args := Args{}
	reply := Reply{}
	ok := true
	for ok {
		ok = ck.clnt.Call(ck.servers[0], "PanServer.Create", &args, &reply)
	}
}

func (ck *Clerk) Delete(path panapi.Ppath, version panapi.Pversion) {}

func (ck *Clerk) Exists(path panapi.Ppath, watch chan int) bool {
	return false
}

func (ck *Clerk) GetData(path panapi.Ppath, watch chan int) string {
	return ""
}

func (ck *Clerk) SetData(path panapi.Ppath, data string, version panapi.Pversion) {}

func (ck *Clerk) GetChildren(path panapi.Ppath, watch chan int) []panapi.Ppath {
	return make([]panapi.Ppath, 0)
}

func (ck *Clerk) Sync(path panapi.Ppath) {}

func MakeClerk(clnt *tester.Clnt, servers []string) panapi.IPNClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}
