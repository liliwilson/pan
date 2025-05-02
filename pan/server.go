package pan

import (
	// "fmt"
	"pan/panapi/rpc"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/kvraft1/rsm"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type ZNode struct {
	name     string
	data     string
	version  rpc.Pversion
	children []*ZNode

	sequenceNums map[string]int
}

// Insert a node into a child's znode list at the correct spot.
// Returns the new node object and a bool indicating success/failure of the operation.
// Failure only occurs if a child with the given name already exists.
func (zn *ZNode) addChild(name string, data string, sequential bool) (*ZNode, bool) {
	// If sequential, find the name
	childName := name
	if sequential {
		seqNum := zn.sequenceNums[name]
		childName += strconv.Itoa(seqNum)
		zn.sequenceNums[name] = seqNum + 1
	}

	// Check if the child already exists, and if not, find where to insert it to maintain sorted order
	child, idx := zn.findChild(childName)
	if child != nil {
		return child, false
	}

	childZNode := ZNode{name: childName, data: data, version: 1, sequenceNums: make(map[string]int)}

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
	sessionId string
	watchId   string
}

type PanServer struct {
	me    int
	dead  int32 // set by Kill(); required for tester
	rsm   *rsm.RSM
	peers []*labrpc.ClientEnd

	// ZK data structures
	mu             sync.Mutex
	rootZNode      *ZNode
	sessions       map[string]time.Time   // map session ID to timeout?
	dataWatches    map[rpc.Ppath][]*Watch // map path to list of data watches on that path
	childWatches   map[rpc.Ppath][]*Watch // map path to list of child watches on that path
	ephemeralNodes map[string][]rpc.Ppath // map session IDs to list of ephemeral znode paths
}

func (pn *PanServer) DoOp(req any) any {
	return ""
}

func (pn *PanServer) Restore([]byte) {}

func (pn *PanServer) Snapshot() []byte {
	return make([]byte, 0)
}

func newSessionTimeout() time.Time {
	timeout := 10 * time.Second // TODO change this timeout
	return time.Now().Add(timeout)
}

// Given a session id, returns true iff the session is still live.
// Resets the session timeout if the session is live.
func (pn *PanServer) checkSession(sessionId string) bool {
	timeout, ok := pn.sessions[sessionId]
	if !ok || time.Now().After(timeout) {
		pn.cleanupSession(sessionId)
		return false
	}

	pn.sessions[sessionId] = newSessionTimeout()
	return true
}

// Cleans up a session that has ended, removing it from session list
// and deleting ephemeral nodes.
func (pn *PanServer) cleanupSession(sessionId string) {
	ephemeralNodes := pn.ephemeralNodes[sessionId]
	for _, path := range ephemeralNodes {
		path := path.ParsePath()
		parentNode := pn.rootZNode.lookup(path[:len(path)-1])
		if parentNode != nil {
			parentNode.removeChild(path[len(path)-1], 0, false)
		}

	}

	delete(pn.sessions, sessionId)
	delete(pn.ephemeralNodes, sessionId)
}

// Start a session for a given client. Returns a session ID.
func (pn *PanServer) StartSession(args *rpc.SessionArgs, reply *rpc.SessionReply) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	pn.sessions[args.SessionId] = newSessionTimeout()
}

func (pn *PanServer) Create(args *rpc.CreateArgs, reply *rpc.CreateReply) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	if !pn.checkSession(args.SessionId) {
		reply.Err = rpc.ErrSessionClosed
	}

	path := args.Path.ParsePath()

	znode, idx := pn.rootZNode.lookupPrefix(path)

	if idx == -1 {
		reply.Err = rpc.ErrOnCreate
	} else {
		for ; idx < len(path); idx++ {
			// ignore the success/failure flag from addChild because already existing child should have been caught by lookupPrefix
			if idx == len(path)-1 {
				znode, _ = znode.addChild(path[idx], args.Data, args.Flags.Sequential)
				path[len(path)-1] = znode.name // update path with seq name
			} else {
				znode, _ = znode.addChild(path[idx], "", false)
			}
		}

		createdPath := rpc.Ppath(strings.Join(path, "/"))

		if args.Flags.Ephemeral {
			pn.ephemeralNodes[args.SessionId] = append(pn.ephemeralNodes[args.SessionId], createdPath)
		}

		reply.ZNodeName = createdPath
		reply.Err = rpc.OK
	}
}

func (pn *PanServer) Exists(args *rpc.ExistsArgs, reply *rpc.ExistsReply) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	if !pn.checkSession(args.SessionId) {
		reply.Err = rpc.ErrSessionClosed
	}

	path := args.Path.ParsePath()
	zn := pn.rootZNode.lookup(path)

	// if we found a znode, lookup returns true
	reply.Result = zn != nil
	reply.Err = rpc.OK
}

func (pn *PanServer) GetData(args *rpc.GetDataArgs, reply *rpc.GetDataReply) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	if !pn.checkSession(args.SessionId) {
		reply.Err = rpc.ErrSessionClosed
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
}

func (pn *PanServer) SetData(args *rpc.SetDataArgs, reply *rpc.SetDataReply) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	if !pn.checkSession(args.SessionId) {
		reply.Err = rpc.ErrSessionClosed
	}

	path := args.Path.ParsePath()
	zn := pn.rootZNode.lookup(path)

	if zn != nil {
		if zn.version == args.Version {
			zn.data = args.Data
			zn.version++

			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrVersion
		}
	} else {
		reply.Err = rpc.ErrNoFile
	}
}

func (pn *PanServer) GetChildren(args *rpc.GetChildrenArgs, reply *rpc.GetChildrenReply) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	if !pn.checkSession(args.SessionId) {
		reply.Err = rpc.ErrSessionClosed
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
	} else {
		reply.Err = rpc.ErrNoFile
	}
}

func (pn *PanServer) Delete(args *rpc.DeleteArgs, reply *rpc.DeleteReply) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	if !pn.checkSession(args.SessionId) {
		reply.Err = rpc.ErrSessionClosed
	}

	path := args.Path.ParsePath()
	// Don't allow deletion of root node
	if len(path) <= 1 {
		reply.Err = rpc.ErrDeleteRoot
		return
	}

	parentNode := pn.rootZNode.lookup(path[:len(path)-1])
	if parentNode == nil {
		reply.Err = rpc.ErrNoFile
		return
	}

	reply.Err = parentNode.removeChild(path[len(path)-1], args.Version, true)
}

// Reset the timeout for a given session.
func (pn *PanServer) KeepAlive(args *rpc.SessionArgs, reply *rpc.SessionReply) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	if !pn.checkSession(args.SessionId) {
		reply.Err = rpc.ErrSessionClosed
	}
}

// End the session with a given sessionId.
func (pn *PanServer) EndSession(args *rpc.SessionArgs, reply *rpc.SessionReply) {
	pn.mu.Lock()
	defer pn.mu.Unlock()

	pn.cleanupSession(args.SessionId)
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
	pn := &PanServer{me: me, peers: servers, rootZNode: &ZNode{name: ""}, sessions: make(map[string]time.Time), ephemeralNodes: make(map[string][]rpc.Ppath)}
	pn.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, pn)

	return []tester.IService{pn, pn.rsm.Raft()}
}
