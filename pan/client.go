package pan

import (
	// "fmt"

	"pan/panapi"
	"pan/panapi/rpc"
	"time"

	tester "6.5840/tester1"
	"github.com/google/uuid"
)

type Session struct {
	clnt              *tester.Clnt
	servers           []string
	id                string
	keepAliveInterval time.Duration
}

type Pan struct{}

type Args struct{}
type Reply struct{}

// Create a new znode with flags; return the name of the new znode
func (ck *Session) Create(path rpc.Ppath, data string, flags rpc.Flag) (rpc.Ppath, rpc.Err) {
	args := rpc.CreateArgs{SessionId: ck.id, Path: path, Data: data, Flags: flags}
	reply := rpc.CreateReply{}
	ck.clnt.Call(ck.servers[0], "PanServer.Create", &args, &reply)
	return reply.ZNodeName, reply.Err
}

// Deletes the given znode if it is at the expected version
func (ck *Session) Delete(path rpc.Ppath, version rpc.Pversion) rpc.Err {
	args := rpc.DeleteArgs{SessionId: ck.id, Path: path, Version: version}
	reply := rpc.DeleteReply{}
	ck.clnt.Call(ck.servers[0], "PanServer.Delete", &args, &reply)
	return reply.Err
}

// Returns true iff the znode at path exists
func (ck *Session) Exists(path rpc.Ppath, watch bool) (bool, rpc.Err) {
	args := rpc.ExistsArgs{SessionId: ck.id, Path: path, Watch: watch}
	reply := rpc.ExistsReply{}
	ck.clnt.Call(ck.servers[0], "PanServer.Exists", &args, &reply)
	return reply.Result, reply.Err
}

// Returns the data and version information about znode
func (ck *Session) GetData(path rpc.Ppath, watch bool) (string, rpc.Pversion, rpc.Err) {
	args := rpc.GetDataArgs{SessionId: ck.id, Path: path, Watch: watch}
	reply := rpc.GetDataReply{}
	ck.clnt.Call(ck.servers[0], "PanServer.GetData", &args, &reply)
	return reply.Data, reply.Version, reply.Err
}

// Writes data to path iff version number is correct
func (ck *Session) SetData(path rpc.Ppath, data string, version rpc.Pversion) rpc.Err {
	args := rpc.SetDataArgs{SessionId: ck.id, Path: path, Data: data, Version: version}
	reply := rpc.SetDataReply{}
	ck.clnt.Call(ck.servers[0], "PanServer.SetData", &args, &reply)
	return reply.Err
}

// Returns an alphabetically sorted list of child znodes
func (ck *Session) GetChildren(path rpc.Ppath, watch bool) ([]rpc.Ppath, rpc.Err) {
	args := rpc.GetChildrenArgs{SessionId: ck.id, Path: path, Watch: watch}
	reply := rpc.GetChildrenReply{}
	ck.clnt.Call(ck.servers[0], "PanServer.GetChildren", &args, &reply)
	return reply.Children, reply.Err
}

// Waits for all updates pending at the start of the operation to propogate to the server that client is connected to
func (ck *Session) Sync(path rpc.Ppath) rpc.Err {
	return rpc.OK
}

// End the current session
func (ck *Session) EndSession() {
	args := rpc.SessionArgs{SessionId: ck.id}
	reply := rpc.SessionReply{}
	ck.clnt.Call(ck.servers[0], "PanServer.EndSession", &args, &reply)
}

// Private function to maintain the session with keepalive messages.
func (ck *Session) maintainSession() {
	args := rpc.SessionArgs{SessionId: ck.id}
	reply := rpc.SessionReply{}

	for {
		time.Sleep(ck.keepAliveInterval)
		ck.clnt.Call(ck.servers[0], "PanServer.KeepAlive", &args, &reply)
		if reply.Err == rpc.ErrSessionClosed {
			break
		}
	}
}

func MakeSession(clnt *tester.Clnt, servers []string) panapi.IPNSession {
	ck := &Session{clnt: clnt, servers: servers, id: uuid.New().String(), keepAliveInterval: time.Second} // TODO change the keepalive interval

	// Notify the server of a new session
	args := rpc.SessionArgs{SessionId: ck.id}
	reply := rpc.SessionReply{}
	ck.clnt.Call(ck.servers[0], "PanServer.StartSession", &args, &reply)

	go ck.maintainSession()

	return ck
}
