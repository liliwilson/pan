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
func (ck *Clerk) Create(path rpc.Ppath, data string, flags rpc.Flag) (rpc.Ppath, rpc.Err) {
	args := rpc.CreateArgs{Path: path, Data: data, Flags: flags}
	reply := rpc.CreateReply{}
	ck.clnt.Call(ck.servers[0], "PanServer.Create", &args, &reply)
	return reply.ZNodeName, reply.Err
}

// Deletes the given znode if it is at the expected version
func (ck *Clerk) Delete(path rpc.Ppath, version rpc.Pversion) rpc.Err {
	args := rpc.DeleteArgs{Path: path, Version: version}
	reply := rpc.DeleteReply{}
	ck.clnt.Call(ck.servers[0], "PanServer.Delete", &args, &reply)
	return reply.Err
}

// Returns true iff the znode at path exists
func (ck *Clerk) Exists(path rpc.Ppath, watch bool) (bool, rpc.Err) {
	args := rpc.ExistsArgs{Path: path, Watch: watch}
	reply := rpc.ExistsReply{}
	ck.clnt.Call(ck.servers[0], "PanServer.Exists", &args, &reply)
	return reply.Result, reply.Err
}

// Returns the data and version information about znode
func (ck *Clerk) GetData(path rpc.Ppath, watch bool) (string, rpc.Pversion, rpc.Err) {
	args := rpc.GetDataArgs{Path: path, Watch: watch}
	reply := rpc.GetDataReply{}
	ck.clnt.Call(ck.servers[0], "PanServer.GetData", &args, &reply)
	return reply.Data, reply.Version, reply.Err
}

// Writes data to path iff version number is correct
func (ck *Clerk) SetData(path rpc.Ppath, data string, version rpc.Pversion) rpc.Err {
	args := rpc.SetDataArgs{Path: path, Data: data, Version: version}
	reply := rpc.SetDataReply{}
	ck.clnt.Call(ck.servers[0], "PanServer.SetData", &args, &reply)
	return reply.Err
}

// Returns an alphabetically sorted list of child znodes
func (ck *Clerk) GetChildren(path rpc.Ppath, watch bool) ([]rpc.Ppath, rpc.Err) {
	args := rpc.GetChildrenArgs{Path: path, Watch: watch}
	reply := rpc.GetChildrenReply{}
	ck.clnt.Call(ck.servers[0], "PanServer.GetChildren", &args, &reply)
	return reply.Children, reply.Err
}

// Waits for all updates pending at the start of the operation to propogate to the server that client is connected to
func (ck *Clerk) Sync(path rpc.Ppath) rpc.Err {
	return rpc.OK
}

func (ck *Clerk) EndSession() {}

func MakeClerk(clnt *tester.Clnt, servers []string) panapi.IPNClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}
