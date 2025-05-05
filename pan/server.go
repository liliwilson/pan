package pan

import (
	// "fmt"
	"pan/panapi/rpc"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvraft1/rsm"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type Key struct {
	sessionId      int
	sequencePrefix string
}

type ZNode struct {
	name     string
	data     string
	version  rpc.Pversion
	children []*ZNode

	creatorId       int
	sequenceNums    map[string]int
	sessionToSeqNum map[Key]int
}

// Insert a node into a child's znode list at the correct spot.
// Returns the new node object and a bool indicating success/failure of the operation.
// Failure only occurs if a child with the given name already exists.
func (zn *ZNode) addChild(name string, data string, sequential bool, creatorId int) (*ZNode, bool) {
	// If sequential, find the name
	childName := name
	if sequential {
		seqNum := zn.sequenceNums[name]
		childName += strconv.Itoa(seqNum)
		zn.sequenceNums[name] = seqNum + 1

		zn.sessionToSeqNum[Key{creatorId, name}] = seqNum
	}

	// Check if the child already exists, and if not, find where to insert it to maintain sorted order
	child, idx := zn.findChild(childName)
	if child != nil {
		return child, false
	}

	childZNode := ZNode{name: childName, data: data, version: 1, creatorId: creatorId, sequenceNums: make(map[string]int), sessionToSeqNum: make(map[Key]int)}

	zn.children = append(zn.children, &ZNode{})
	copy(zn.children[idx+1:], zn.children[idx:])
	zn.children[idx] = &childZNode

	return &childZNode, true
}

// Check if a znode has a direct child with a given name.
// Returns that znode if it exists, else nil.
// Also returns the index of the found child node. If the child node does not exist,
// returns the index that the child node should be placed at to maintain sorted order.
func (zn *ZNode) findChild(name string) (*ZNode, int) {
	children := zn.children

	// the go sort package uses binary search
	idx := sort.Search(len(children), func(i int) bool {
		return children[i].name >= name
	})

	if idx < len(children) && children[idx].name == name {
		return children[idx], idx
	} else {
		// this child doesn't exist, but would be at idx if it did
		return nil, idx
	}
}

// Given a name and a version number, removes the removes a znode child if the version number is up to date.
// Checks the version number if checkVersion is set, otherwise, deletes unconditionally (used by ephemeral znodes).
// Returns one of three rpc Error values: ErrNoFile, ErrVersion, or OK.
func (zn *ZNode) removeChild(name string, version rpc.Pversion, checkVersion bool) rpc.Err {
	children := zn.children
	child, idx := zn.findChild(name)

	if child == nil {
		return rpc.ErrNoFile
	}

	if checkVersion && child.version != version {
		return rpc.ErrVersion
	}

	zn.children = append(children[:idx], children[idx+1:]...)
	return rpc.OK
}

// Traverse the tree with root zn to find a znode.
// Returns the znode if it's there, otherwise nil.
func (zn *ZNode) lookup(path []string) *ZNode {
	node, idx := zn.lookupPrefix(path)
	if node != nil && idx == -1 {
		return node
	}
	return nil
}

// Traverse the tree and find a prefix match on the path.
// Returns the last matching znode and the index of the first element the path string that was not found.
// If there is a full path match, returns -1 as the index.
// If the node is not found, returns nil.
func (zn *ZNode) lookupPrefix(path []string) (*ZNode, int) {
	if len(path) == 0 || path[0] != zn.name {
		return nil, -1
	}

	znode := zn
	var i int
	var child *ZNode
	for i = 1; i < len(path); i++ {
		child, _ = znode.findChild(path[i])
		if child == nil {
			return znode, i
		}
		znode = child
	}

	return znode, -1
}

type Watch struct {
	sessionId int
	watchId   int
}

type Watchlist struct {
	watchType string
	watches   map[rpc.Ppath][]*Watch
}

// For a given watchlist, return a list of watches that should be fired based on the path
func (watchlist *Watchlist) fire(path rpc.Ppath) map[Watch]*rpc.WatchArgs {
	fired := make(map[Watch]*rpc.WatchArgs)

	// Check if we have a watch on this node
	if watches, exists := watchlist.watches[path]; exists {
		for _, watch := range watches {
			fired[*watch] = &rpc.WatchArgs{EventType: watchlist.watchType, Path: path}
		}

		delete(watchlist.watches, path)
	}

	return fired
}

// For a given watchlist, append the watch at the provided path to the list.
func (watchlist *Watchlist) append(path rpc.Ppath, watch *Watch) {
	watchlist.watches[path] = append(watchlist.watches[path], watch)
}

// Initialize watchlists for a new PanServer
func (pn *PanServer) initializeWatchlists() {
	pn.dataWatches = Watchlist{watchType: rpc.NodeDataChanged, watches: make(map[rpc.Ppath][]*Watch)}
	pn.createWatches = Watchlist{watchType: rpc.NodeCreated, watches: make(map[rpc.Ppath][]*Watch)}
	pn.deleteWatches = Watchlist{watchType: rpc.NodeDeleted, watches: make(map[rpc.Ppath][]*Watch)}
	pn.childWatches = Watchlist{watchType: rpc.NodeChildrenChanged, watches: make(map[rpc.Ppath][]*Watch)}
	pn.firedWatches = make(map[Watch]*rpc.WatchArgs)
}

// Return the next watch Id to be assigned by the server
func (pn *PanServer) getWatchId() int {
	id := pn.nextWatchId
	pn.nextWatchId++
	return id
}

// Given a map of fired watches produced by watchlist.fire(), add them to the PanServer's firedWatches map
func (pn *PanServer) addFiredWatches(fired map[Watch]*rpc.WatchArgs) {
	for w, wargs := range fired {
		pn.firedWatches[w] = wargs
	}
	pn.watchCond.Broadcast()
}

type PanServer struct {
	me    int
	dead  int32 // set by Kill(); required for tester
	rsm   *rsm.RSM
	peers []*labrpc.ClientEnd

	// ZK data structures
	mu        sync.Mutex
	rootZNode *ZNode

	// Session data
	sessions       map[int]time.Time // map session ID to timeout
	sessionCounter int
	ephemeralNodes map[int][]rpc.Ppath // map session IDs to list of ephemeral znode paths

	// Watches data
	nextWatchId   int
	dataWatches   Watchlist
	createWatches Watchlist
	deleteWatches Watchlist
	childWatches  Watchlist
	firedWatches  map[Watch]*rpc.WatchArgs
	watchCond     *sync.Cond
}

type TimestampedRequest struct {
	Timestamp int64 // int64 instead of time.Time to appease raft lab encoding
	Request   any
}

func (pn *PanServer) DoOp(tsReq any) any {
	switch tsReq.(type) {
	case TimestampedRequest:
		tsReq := tsReq.(TimestampedRequest)
		timestamp := time.UnixMicro(tsReq.Timestamp)
		req := tsReq.Request
		switch req.(type) {
		case rpc.StartSessionArgs:
			req := req.(rpc.StartSessionArgs)
			reply := rpc.StartSessionReply{}
			pn.applyStartSession(&req, &reply, timestamp)
			return &reply
		case rpc.EndSessionArgs:
			req := req.(rpc.EndSessionArgs)
			reply := rpc.EndSessionReply{}
			pn.applyEndSession(&req, &reply)
			return &reply
		case rpc.KeepAliveArgs:
			req := req.(rpc.KeepAliveArgs)
			reply := rpc.KeepAliveReply{}
			pn.applyKeepAlive(&req, &reply, timestamp)
			return &reply
		case rpc.CreateArgs:
			req := req.(rpc.CreateArgs)
			reply := rpc.CreateReply{}
			pn.applyCreate(&req, &reply, timestamp)
			return &reply
		case rpc.ExistsArgs:
			req := req.(rpc.ExistsArgs)
			reply := rpc.ExistsReply{}
			pn.applyExists(&req, &reply, timestamp)
			return &reply
		case rpc.GetDataArgs:
			req := req.(rpc.GetDataArgs)
			reply := rpc.GetDataReply{}
			pn.applyGetData(&req, &reply, timestamp)
			return &reply
		case rpc.SetDataArgs:
			req := req.(rpc.SetDataArgs)
			reply := rpc.SetDataReply{}
			pn.applySetData(&req, &reply, timestamp)
			return &reply
		case rpc.GetChildrenArgs:
			req := req.(rpc.GetChildrenArgs)
			reply := rpc.GetChildrenReply{}
			pn.applyGetChildren(&req, &reply, timestamp)
			return &reply
		case rpc.DeleteArgs:
			req := req.(rpc.DeleteArgs)
			reply := rpc.DeleteReply{}
			pn.applyDelete(&req, &reply, timestamp)
			return &reply
		case rpc.GetHighestSeqArgs:
			req := req.(rpc.GetHighestSeqArgs)
			reply := rpc.GetHighestSeqReply{}
			pn.applyGetHighestSequence(&req, &reply, timestamp)
			return &reply
		}
	}

	return ""
}

func (pn *PanServer) Restore([]byte) {}

func (pn *PanServer) Snapshot() []byte {
	return make([]byte, 0)
}

// Sets a new session timeout, given that we last heard from the client at the given timestamp.
func newSessionTimeout(timestamp time.Time) time.Time {
	timeout := 5 * time.Second
	return timestamp.Add(timeout)
}

// Given a session id, returns true iff the session is still live.
// Resets the session timeout if the session is live.
func (pn *PanServer) checkSession(sessionId int, timestamp time.Time) bool {
	timeout, ok := pn.sessions[sessionId]

	for session, timeout := range pn.sessions {
		if session != sessionId && timestamp.After(timeout) {
			pn.cleanupSession(session)
		}
	}

	if !ok || timestamp.After(timeout) {
		pn.cleanupSession(sessionId)
		return false
	}

	pn.sessions[sessionId] = newSessionTimeout(timestamp)
	return true
}

// Cleans up a session that has ended, removing it from session list
// and deleting ephemeral nodes.
func (pn *PanServer) cleanupSession(sessionId int) {
	ephemeralNodes := pn.ephemeralNodes[sessionId]
	for _, path := range ephemeralNodes {
		path := path.ParsePath()

		parentPath := path[:len(path)-1]
		parentNode := pn.rootZNode.lookup(parentPath)

		if parentNode != nil {
			parentNode.removeChild(path[len(path)-1], 0, false)

			// Fire child watches on the parent
			pn.addFiredWatches(pn.childWatches.fire(rpc.MakePpath(parentPath)))
			// Fire delete watches on the child
			pn.addFiredWatches(pn.deleteWatches.fire(rpc.MakePpath(path)))
		}
	}

	delete(pn.sessions, sessionId)
	delete(pn.ephemeralNodes, sessionId)
}

// Returns the highest sequence number of a node with a given path owned by the current session.
func (pn *PanServer) GetHighestSequence(args *rpc.GetHighestSeqArgs, reply *rpc.GetHighestSeqReply) {
	tsReq := TimestampedRequest{Timestamp: time.Now().UnixMicro(), Request: *args}
	err, res := pn.rsm.Submit(tsReq)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		*reply = *(res.(*rpc.GetHighestSeqReply))
	}
}

func (pn *PanServer) applyGetHighestSequence(args *rpc.GetHighestSeqArgs, reply *rpc.GetHighestSeqReply, timestamp time.Time) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	path := args.Path.ParsePath()

	// You shouldn't be trying to create the root
	if len(path) <= 1 {
		reply.Err = rpc.ErrOnCreate
		return
	}

	parentNode := pn.rootZNode.lookup(path[:len(path)-1])
	if parentNode == nil {
		reply.SeqNum = -1
		reply.Err = rpc.ErrNoFile
		return
	}

	name := path[len(path)-1]
	seqNum, exists := parentNode.sessionToSeqNum[Key{args.SessionId, name}]
	if !exists {
		reply.SeqNum = -1
		reply.Err = rpc.ErrNoFile
		return
	}

	reply.SeqNum = seqNum
	reply.Err = rpc.OK
}

// Start a session for a given client. Returns a session ID.
func (pn *PanServer) StartSession(args *rpc.StartSessionArgs, reply *rpc.StartSessionReply) {
	tsReq := TimestampedRequest{Timestamp: time.Now().UnixMicro(), Request: *args}
	err, res := pn.rsm.Submit(tsReq)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		*reply = *(res.(*rpc.StartSessionReply))
	}
}

func (pn *PanServer) applyStartSession(args *rpc.StartSessionArgs, reply *rpc.StartSessionReply, timestamp time.Time) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	sessionId := pn.sessionCounter
	pn.sessionCounter++
	pn.sessions[sessionId] = newSessionTimeout(timestamp)

	reply.Err = rpc.OK
	reply.SessionId = sessionId
}

// Create a znode.
func (pn *PanServer) Create(args *rpc.CreateArgs, reply *rpc.CreateReply) {
	tsReq := TimestampedRequest{Timestamp: time.Now().UnixMicro(), Request: *args}
	err, res := pn.rsm.Submit(tsReq)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		*reply = *(res.(*rpc.CreateReply))
	}
}

func (pn *PanServer) applyCreate(args *rpc.CreateArgs, reply *rpc.CreateReply, timestamp time.Time) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	if !pn.checkSession(args.SessionId, timestamp) {
		reply.Err = rpc.ErrSessionClosed
		return
	}

	path := args.Path.ParsePath()

	znode, idx := pn.rootZNode.lookupPrefix(path)
	createdPath := rpc.MakePpath(path[:idx])

	if idx == -1 {
		reply.CreatedBy = znode.creatorId
		reply.Err = rpc.ErrOnCreate
	} else {
		for ; idx < len(path); idx++ {
			// fire the child watches, since we'll be adding a child to this path
			pn.addFiredWatches(pn.childWatches.fire(createdPath))

			// ignore the success/failure flag from addChild because already existing child should have been caught by lookupPrefix
			if idx == len(path)-1 {
				znode, _ = znode.addChild(path[idx], args.Data, args.Flags.Sequential, args.SessionId)
			} else {
				znode, _ = znode.addChild(path[idx], "", false, args.SessionId)
			}

			createdPath = createdPath.Add("/" + znode.name)

			// fire the create watches with the updated path name, since we just created this node
			pn.addFiredWatches(pn.createWatches.fire(createdPath))
		}

		if args.Flags.Ephemeral {
			pn.ephemeralNodes[args.SessionId] = append(pn.ephemeralNodes[args.SessionId], createdPath)
		}

		reply.ZNodeName = createdPath
		reply.CreatedBy = znode.creatorId
		reply.Err = rpc.OK
	}
}

// Check if a given znode exists.
func (pn *PanServer) Exists(args *rpc.ExistsArgs, reply *rpc.ExistsReply) {
	tsReq := TimestampedRequest{Timestamp: time.Now().UnixMicro(), Request: *args}
	err, res := pn.rsm.Submit(tsReq)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		*reply = *(res.(*rpc.ExistsReply))
	}
}

func (pn *PanServer) applyExists(args *rpc.ExistsArgs, reply *rpc.ExistsReply, timestamp time.Time) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	if !pn.checkSession(args.SessionId, timestamp) {
		reply.Err = rpc.ErrSessionClosed
		return
	}

	path := args.Path.ParsePath()
	zn := pn.rootZNode.lookup(path)

	// if we found a znode, lookup returns true
	reply.Result = zn != nil
	reply.Err = rpc.OK

	// add a watch if the flag is set
	if args.Watch.ShouldWatch {
		watchId := pn.getWatchId()
		reply.WatchId = watchId

		// TODO is this right
		if reply.Result {
			pn.deleteWatches.append(args.Path, &Watch{watchId: watchId, sessionId: args.SessionId})
		} else {
			pn.createWatches.append(args.Path, &Watch{watchId: watchId, sessionId: args.SessionId})
		}
	}
}

// Get the data for a given znode.
func (pn *PanServer) GetData(args *rpc.GetDataArgs, reply *rpc.GetDataReply) {
	tsReq := TimestampedRequest{Timestamp: time.Now().UnixMicro(), Request: *args}
	err, res := pn.rsm.Submit(tsReq)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		*reply = *(res.(*rpc.GetDataReply))
	}
}

func (pn *PanServer) applyGetData(args *rpc.GetDataArgs, reply *rpc.GetDataReply, timestamp time.Time) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	if !pn.checkSession(args.SessionId, timestamp) {
		reply.Err = rpc.ErrSessionClosed
		return
	}

	path := args.Path.ParsePath()
	zn := pn.rootZNode.lookup(path)

	if zn != nil {
		reply.Data = zn.data
		reply.Version = zn.version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoFile
	}

	// add a watch if the flag is set
	if args.Watch.ShouldWatch {
		watchId := pn.getWatchId()
		reply.WatchId = watchId
		pn.dataWatches.append(args.Path, &Watch{watchId: watchId, sessionId: args.SessionId})
	}
}

// Set the data for a given znode.
func (pn *PanServer) SetData(args *rpc.SetDataArgs, reply *rpc.SetDataReply) {
	tsReq := TimestampedRequest{Timestamp: time.Now().UnixMicro(), Request: *args}
	err, res := pn.rsm.Submit(tsReq)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		*reply = *(res.(*rpc.SetDataReply))
	}
}

func (pn *PanServer) applySetData(args *rpc.SetDataArgs, reply *rpc.SetDataReply, timestamp time.Time) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	if !pn.checkSession(args.SessionId, timestamp) {
		reply.Err = rpc.ErrSessionClosed
		return
	}

	path := args.Path.ParsePath()
	zn := pn.rootZNode.lookup(path)

	if zn != nil {
		if zn.version == args.Version {
			zn.data = args.Data
			zn.version++

			// Fire the data watches
			pn.addFiredWatches(pn.dataWatches.fire(args.Path))

			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		reply.Err = rpc.ErrNoFile
	}
}

// Get the children for a given znode.
func (pn *PanServer) GetChildren(args *rpc.GetChildrenArgs, reply *rpc.GetChildrenReply) {
	tsReq := TimestampedRequest{Timestamp: time.Now().UnixMicro(), Request: *args}
	err, res := pn.rsm.Submit(tsReq)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		*reply = *(res.(*rpc.GetChildrenReply))
	}
}

func (pn *PanServer) applyGetChildren(args *rpc.GetChildrenArgs, reply *rpc.GetChildrenReply, timestamp time.Time) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	if !pn.checkSession(args.SessionId, timestamp) {
		reply.Err = rpc.ErrSessionClosed
		return
	}

	path := args.Path.ParsePath()
	zn := pn.rootZNode.lookup(path)

	if zn != nil {
		childrenPaths := make([]rpc.Ppath, len(zn.children))
		for i, child := range zn.children {
			childrenPaths[i] = rpc.Ppath(child.name)
		}

		reply.Children = childrenPaths
		reply.Err = rpc.OK

		// add a watch if the flag is set
		if args.Watch.ShouldWatch {
			watchId := pn.getWatchId()
			reply.WatchId = watchId
			pn.childWatches.append(args.Path, &Watch{watchId: watchId, sessionId: args.SessionId})
		}
	} else {
		reply.Err = rpc.ErrNoFile
	}
}

// Delete a given znode.
func (pn *PanServer) Delete(args *rpc.DeleteArgs, reply *rpc.DeleteReply) {
	tsReq := TimestampedRequest{Timestamp: time.Now().UnixMicro(), Request: *args}
	err, res := pn.rsm.Submit(tsReq)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		*reply = *(res.(*rpc.DeleteReply))
	}
}

func (pn *PanServer) applyDelete(args *rpc.DeleteArgs, reply *rpc.DeleteReply, timestamp time.Time) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	if !pn.checkSession(args.SessionId, timestamp) {
		reply.Err = rpc.ErrSessionClosed
		return
	}

	path := args.Path.ParsePath()
	// Don't allow deletion of root node
	if len(path) <= 1 {
		reply.Err = rpc.ErrDeleteRoot
		return
	}

	parentPath := path[:len(path)-1]
	parentNode := pn.rootZNode.lookup(parentPath)
	if parentNode == nil {
		reply.Err = rpc.ErrNoFile
		return
	}

	reply.Err = parentNode.removeChild(path[len(path)-1], args.Version, true)

	// Fire child watches on the parent
	pn.addFiredWatches(pn.childWatches.fire(rpc.MakePpath(parentPath)))

	// Fire delete watches on the child
	pn.addFiredWatches(pn.deleteWatches.fire(args.Path))
}

// Reset the timeout for a given session.
func (pn *PanServer) KeepAlive(args *rpc.KeepAliveArgs, reply *rpc.KeepAliveReply) {
	tsReq := TimestampedRequest{Timestamp: time.Now().UnixMicro(), Request: *args}
	err, res := pn.rsm.Submit(tsReq)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		*reply = *(res.(*rpc.KeepAliveReply))
	}
}

func (pn *PanServer) applyKeepAlive(args *rpc.KeepAliveArgs, reply *rpc.KeepAliveReply, timestamp time.Time) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	if !pn.checkSession(args.SessionId, timestamp) {
		reply.Err = rpc.ErrSessionClosed
	}
}

// End the session with a given sessionId.
func (pn *PanServer) EndSession(args *rpc.EndSessionArgs, reply *rpc.EndSessionReply) {
	tsReq := TimestampedRequest{Timestamp: time.Now().UnixMicro(), Request: *args}
	err, res := pn.rsm.Submit(tsReq)

	if err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
	} else {
		*reply = *(res.(*rpc.EndSessionReply))
	}
}

func (pn *PanServer) applyEndSession(args *rpc.EndSessionArgs, reply *rpc.EndSessionReply) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	pn.cleanupSession(args.SessionId)
	reply.Err = rpc.OK
}

// Call this function with a watchId and return when that watch is fired.
func (pn *PanServer) WatchWait(args *rpc.WatchWaitArgs, reply *rpc.WatchWaitReply) {
	goalWatch := Watch{sessionId: args.SessionId, watchId: args.WatchId}

	for !pn.killed() {
		pn.mu.Lock()

		// Check if the watch has been fired.
		if watchInfo, exists := pn.firedWatches[goalWatch]; exists {
			reply.WatchEvent = *watchInfo
			reply.Err = rpc.OK

			delete(pn.firedWatches, goalWatch)
			pn.mu.Unlock()
			return
		}
		pn.watchCond.Wait()

		pn.mu.Unlock()
	}
}

// Kill this server.
func (pn *PanServer) Kill() {
	atomic.StoreInt32(&pn.dead, 1)
}

func (pn *PanServer) killed() bool {
	z := atomic.LoadInt32(&pn.dead)
	return z == 1
}

func registerLabgobArgs() {
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.StartSessionArgs{})
	labgob.Register(rpc.EndSessionArgs{})
	labgob.Register(rpc.KeepAliveArgs{})
	labgob.Register(rpc.CreateArgs{})
	labgob.Register(rpc.ExistsArgs{})
	labgob.Register(rpc.GetDataArgs{})
	labgob.Register(rpc.SetDataArgs{})
	labgob.Register(rpc.GetChildrenArgs{})
	labgob.Register(rpc.DeleteArgs{})
	labgob.Register(rpc.GetHighestSeqArgs{})
	labgob.Register(rpc.WatchWaitArgs{})
	labgob.Register(TimestampedRequest{})
}

// Must return quickly
func StartPanServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	registerLabgobArgs()

	pn := &PanServer{me: me, peers: servers, rootZNode: &ZNode{name: "", sequenceNums: make(map[string]int), sessionToSeqNum: make(map[Key]int)}, sessions: make(map[int]time.Time), ephemeralNodes: make(map[int][]rpc.Ppath)}

	pn.initializeWatchlists()
	pn.watchCond = sync.NewCond(&pn.mu)
	pn.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, pn)

	return []tester.IService{pn, pn.rsm.Raft()}
}
