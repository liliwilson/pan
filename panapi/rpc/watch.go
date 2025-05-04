package rpc

type Watch struct {
	ShouldWatch bool
	Callback    func(WatchArgs)
}

const (
	// For ZkState
	Def = ""

	// For EventType
	NodeCreated         = "NodeCreated"
	NodeDeleted         = "NodeDeleted"
	NodeDataChanged     = "NodeDataChanged"
	NodeChildrenChanged = "NodeChildrenChanged"
)

type WatchArgs struct {
	ZkState   string
	EventType string
	Path      Ppath
}

func EmptyWatch(_ WatchArgs) {}
