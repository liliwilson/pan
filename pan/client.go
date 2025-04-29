package pan

import (
	// "fmt"

	"6.5840/tester1"
	"pan/panapi"
	"pan/panapi/rpc"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
}

type Pan struct{}

type Args struct{}
type Reply struct{}

// Create a new znode with flags; return the name of the new znode
func (ck *Clerk) Create(path panapi.Ppath, data string, flags panapi.Flag) (panapi.Ppath, rpc.Err) {
	args := Args{}
	reply := Reply{}
	ck.clnt.Call(ck.servers[0], "PanServer.Create", &args, &reply)
	return path, rpc.OK
}

// Deletes the given znode if it is at the expected version
func (ck *Clerk) Delete(path panapi.Ppath, version panapi.Pversion) rpc.Err {
	return rpc.OK
}

// Returns true iff the znode at path exists
func (ck *Clerk) Exists(path panapi.Ppath, watch bool) (bool, rpc.Err) {
	return false, rpc.OK
}

// Returns the data and version information about znode
func (ck *Clerk) GetData(path panapi.Ppath, watch bool) (string, panapi.Pversion, rpc.Err) {
	return "", 0, rpc.OK
}

// Writes data to path iff version number is correct
func (ck *Clerk) SetData(path panapi.Ppath, data string, version panapi.Pversion) rpc.Err {
	return rpc.OK
}

// Returns an alphabetically sorted list of child znodes
func (ck *Clerk) GetChildren(path panapi.Ppath, watch bool) ([]panapi.Ppath, rpc.Err) {
		return make([]panapi.Ppath, 0), rpc.OK
}

// Waits for all updates pending at the start of the operation to propogate to the server that client is connected to
func (ck *Clerk) Sync(path panapi.Ppath) rpc.Err {
	return rpc.OK
}

func MakeClerk(clnt *tester.Clnt, servers []string) panapi.IPNClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}
