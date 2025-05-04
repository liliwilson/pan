package pan

import (
	// "fmt"

	// "fmt"
	"pan/panapi"
	"pan/panapi/rpc"
	"sync"
	"time"

	tester "6.5840/tester1"
)

type Session struct {
	clnt              *tester.Clnt
	servers           []string
	id                int
	keepAliveInterval time.Duration
	leader            int

	mu sync.Mutex
}

type Pan struct{}

type Args struct{}
type Reply struct{}

func (ck *Session) getLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	return ck.leader
}

func (ck *Session) incrementLeader() int {
	ck.mu.Lock()
	ck.leader = (ck.leader + 1) % len(ck.servers)
	defer ck.mu.Unlock()
	return ck.leader
}

// Create a new znode with flags; return the name of the new znode
func (ck *Session) Create(path rpc.Ppath, data string, flags rpc.Flag) (rpc.Ppath, rpc.Err) {
	args := rpc.CreateArgs{SessionId: ck.id, Path: path, Data: data, Flags: flags}
	reply := rpc.CreateReply{}

	for {
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.Create", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			// fmt.Printf("client with session id %d created %v, with error %v\n", ck.id, reply.ZNodeName, reply.Err)
			return reply.ZNodeName, reply.Err
		}
		ck.incrementLeader()
		time.Sleep(100 * time.Millisecond)
	}
}

// Deletes the given znode if it is at the expected version
func (ck *Session) Delete(path rpc.Ppath, version rpc.Pversion) rpc.Err {
	args := rpc.DeleteArgs{SessionId: ck.id, Path: path, Version: version}
	reply := rpc.DeleteReply{}

	for {
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.Delete", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Err
		}
		ck.incrementLeader()
		time.Sleep(100 * time.Millisecond)
	}
}

// Returns true iff the znode at path exists
func (ck *Session) Exists(path rpc.Ppath, watch rpc.Watch) (bool, rpc.Err) {
	args := rpc.ExistsArgs{SessionId: ck.id, Path: path, Watch: watch}
	reply := rpc.ExistsReply{}

	for {
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.Exists", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Result, reply.Err
		}
		ck.incrementLeader()
		time.Sleep(100 * time.Millisecond)
	}
}

// Returns the data and version information about znode
func (ck *Session) GetData(path rpc.Ppath, watch rpc.Watch) (string, rpc.Pversion, rpc.Err) {
	args := rpc.GetDataArgs{SessionId: ck.id, Path: path, Watch: watch}
	reply := rpc.GetDataReply{}

	for {
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.GetData", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Data, reply.Version, reply.Err
		}
		ck.incrementLeader()
		time.Sleep(100 * time.Millisecond)
	}
}

// Writes data to path iff version number is correct
func (ck *Session) SetData(path rpc.Ppath, data string, version rpc.Pversion) rpc.Err {
	args := rpc.SetDataArgs{SessionId: ck.id, Path: path, Data: data, Version: version}
	reply := rpc.SetDataReply{}

	for {
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.SetData", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Err
		}
		ck.incrementLeader()
		time.Sleep(100 * time.Millisecond)
	}
}

// Returns an alphabetically sorted list of child znodes
func (ck *Session) GetChildren(path rpc.Ppath, watch rpc.Watch) ([]rpc.Ppath, rpc.Err) {
	args := rpc.GetChildrenArgs{SessionId: ck.id, Path: path, Watch: watch}
	reply := rpc.GetChildrenReply{}

	for {
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.GetChildren", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.Children, reply.Err
		}
		ck.incrementLeader()
		time.Sleep(100 * time.Millisecond)
	}
}

// Waits for all updates pending at the start of the operation to propogate to the server that client is connected to
func (ck *Session) Sync(path rpc.Ppath) rpc.Err {
	return rpc.OK
}

// End the current session
func (ck *Session) EndSession() {
	args := rpc.EndSessionArgs{SessionId: ck.id}
	reply := rpc.EndSessionReply{}

	for {
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.EndSession", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return
		}
		ck.incrementLeader()
		time.Sleep(100 * time.Millisecond)
	}
}

// Private function to maintain the session with keepalive messages.
func (ck *Session) maintainSession() {
	args := rpc.KeepAliveArgs{SessionId: ck.id}
	reply := rpc.KeepAliveReply{}

	for {
		time.Sleep(ck.keepAliveInterval)
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.KeepAlive", &args, &reply)
		if reply.Err == rpc.ErrSessionClosed {
			break
		}

		if !ok || reply.Err == rpc.ErrWrongLeader {
			ck.incrementLeader()
		}
	}
}

func MakeSession(clnt *tester.Clnt, servers []string) panapi.IPNSession {
	ck := &Session{clnt: clnt, servers: servers, keepAliveInterval: 100 * time.Millisecond}

	// Notify the server of a new session
	args := rpc.StartSessionArgs{}
	reply := rpc.StartSessionReply{}
	for {
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.StartSession", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			ck.id = reply.SessionId
			break
		}
		ck.incrementLeader()
		time.Sleep(100 * time.Millisecond)
	}

	go ck.maintainSession()

	return ck
}
