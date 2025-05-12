package pan

import (
	// "fmt"

	"pan/panapi"
	"pan/panapi/rpc"
	"strconv"
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

	var oldSeqNum int
	if flags.Sequential {
		oldSeqNum, _ = ck.getHighestSequence(path)
	}

	for {
		reply := rpc.CreateReply{}
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.Create", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			// If the znode already exists, but we created it, return OK. This may come up in crash cases.
			if reply.Err == rpc.ErrOnCreate && reply.CreatedBy == ck.id {
				DebugPrint(dCreate, "S%d created znode %v (already created) %v", ck.id, reply.ZNodeName, PrintFlags(flags))
				return reply.ZNodeName, rpc.OK
			}

			DebugPrint(dCreate, "S%d created znode %v %v", ck.id, reply.ZNodeName, PrintFlags(flags))
			return reply.ZNodeName, reply.Err
		}

		// If we did not get a response, figure out if our znode was actually created.
		// If it was, return. Otherwise, retry.
		if !ok {
			if flags.Sequential {
				newSeqNum, _ := ck.getHighestSequence(path)
				if newSeqNum > oldSeqNum {
					updatedPath := path + rpc.Ppath(strconv.Itoa(newSeqNum))
					DebugPrint(dCreate, "S%d created znode %v %v", ck.id, updatedPath, PrintFlags(flags))
					return updatedPath, reply.Err
				}
			}
		}

		ck.incrementLeader()
		time.Sleep(100 * time.Millisecond)
	}
}

// Helper function for getting the highest sequence number of one of our sequential znodes with a given path.
func (ck *Session) getHighestSequence(path rpc.Ppath) (int, rpc.Err) {
	args := rpc.GetHighestSeqArgs{SessionId: ck.id, Path: path}

	for {
		reply := rpc.GetHighestSeqReply{}
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.GetHighestSequence", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			return reply.SeqNum, reply.Err
		}
		ck.incrementLeader()
		time.Sleep(100 * time.Millisecond)
	}
}

// Deletes the given znode if it is at the expected version
func (ck *Session) Delete(path rpc.Ppath, version rpc.Pversion) rpc.Err {
	args := rpc.DeleteArgs{SessionId: ck.id, Path: path, Version: version}

	for {
		reply := rpc.DeleteReply{}
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.Delete", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			DebugPrint(dDelete, "S%d deleted znode %v", ck.id, args.Path)
			return reply.Err
		}

		ck.incrementLeader()
		time.Sleep(100 * time.Millisecond)
	}
}

// Returns true iff the znode at path exists
func (ck *Session) Exists(path rpc.Ppath, watch rpc.Watch) (bool, rpc.Err) {
	args := rpc.ExistsArgs{SessionId: ck.id, Path: path, Watch: watch}

	for {
		reply := rpc.ExistsReply{}
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.Exists", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			if watch.ShouldWatch {
				DebugPrint(dWatch, "S%d setting an exists watch on %v with id %d", ck.id, path, reply.WatchId)
				go ck.WatchWait(reply.WatchId, watch.Callback)
			}

			DebugPrint(dExists, "S%d called exists on znode %v, got result %v and watchId %d", ck.id, args.Path, reply.Result, reply.WatchId)
			return reply.Result, reply.Err
		}
		ck.incrementLeader()
		time.Sleep(100 * time.Millisecond)
	}
}

// Wait on a watchId. Called by Exists, GetData, and GetChildren
func (ck *Session) WatchWait(watchId int, watchCallback func(rpc.WatchArgs)) {
	args := rpc.WatchWaitArgs{SessionId: ck.id, WatchId: watchId}

	for {
		reply := rpc.WatchWaitReply{}
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.WatchWait", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			DebugPrint(dWatch, "S%d received a watch trigger on watch %d", ck.id, watchId)
			// Call the watch callback
			watchCallback(reply.WatchEvent)
			return
		}
		ck.incrementLeader()
		time.Sleep(100 * time.Millisecond)
	}

}

// Returns the data and version information about znode
func (ck *Session) GetData(path rpc.Ppath, watch rpc.Watch) (string, rpc.Pversion, rpc.Err) {
	args := rpc.GetDataArgs{SessionId: ck.id, Path: path, Watch: watch}

	for {
		reply := rpc.GetDataReply{}
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.GetData", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			if watch.ShouldWatch {
				DebugPrint(dWatch, "S%d setting a data watch on %v with id %d", ck.id, path, reply.WatchId)
				go ck.WatchWait(reply.WatchId, watch.Callback)
			}

			DebugPrint(dGetData, "S%d called get on %v, got data %v, version %v, error %v with watchId %d", ck.id, path, reply.Data, reply.Version, reply.Err, reply.WatchId)
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

	leader := ck.getLeader()
	ok := ck.clnt.Call(ck.servers[leader], "PanServer.SetData", &args, &reply)
	if ok && reply.Err != rpc.ErrWrongLeader {
		DebugPrint(dSetData, "S%d called setData on %v with  data %v and version %v, got error %v", ck.id, path, data, version, reply.Err)
		return reply.Err
	}

	for {
		reply := rpc.SetDataReply{}
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.SetData", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			if reply.Err == rpc.ErrVersion {
				DebugPrint(dSetData, "S%d called setData on %v with  data %v and version %v, got error %v", ck.id, path, data, version, rpc.ErrMaybe)
				return rpc.ErrMaybe
			}
			DebugPrint(dSetData, "S%d called setData on %v with  data %v and version %v, got error %v", ck.id, data, version, reply.Err)
			return reply.Err
		}
		ck.incrementLeader()
		time.Sleep(100 * time.Millisecond)
	}
}

// Returns an alphabetically sorted list of child znodes
func (ck *Session) GetChildren(path rpc.Ppath, watch rpc.Watch) ([]rpc.Ppath, rpc.Err) {
	args := rpc.GetChildrenArgs{SessionId: ck.id, Path: path, Watch: watch}

	for {
		reply := rpc.GetChildrenReply{}
		leader := ck.getLeader()
		ok := ck.clnt.Call(ck.servers[leader], "PanServer.GetChildren", &args, &reply)
		if ok && reply.Err != rpc.ErrWrongLeader {
			if watch.ShouldWatch {
				DebugPrint(dWatch, "S%d setting a children watch on %v with id %d", ck.id, path, reply.WatchId)
				go ck.WatchWait(reply.WatchId, watch.Callback)
			}

			DebugPrint(dGetChildren, "S%d called getChildren on %v with got children %v and error %v with watchId %d", ck.id, path, reply.Children, reply.Err, reply.WatchId)
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

	for {
		reply := rpc.EndSessionReply{}
		leader := ck.getLeader()
		DebugPrint(dEndSession, "S%d ended by client", ck.id)
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

	for {
		reply := rpc.KeepAliveReply{}
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
	for {
		reply := rpc.StartSessionReply{}
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
