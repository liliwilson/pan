package rpc

import "strings"

type Pversion int
type Ppath string
type Flag struct {
	Ephemeral  bool
	Sequential bool
}

func (path *Ppath) ParsePath() []string {
	return strings.Split(string(*path), "/")
}

func (path *Ppath) Suffix() string {
	dirs := path.ParsePath()
	return dirs[len(dirs) - 1]
}

type Watch struct {
	ShouldWatch bool
	Callback    func(WatchArgs)
}

type WatchArgs struct {
	IsGetData bool
	NewData   string

	IsExists bool

	IsGetChildren bool
	NewChildren   []Ppath

	// Should be set for all three events
	ZNodeExists bool
}

func EmptyWatch(_ WatchArgs) {}

type Err string

const (
	OK               = "OK"
	ErrNoFile        = "ErrNoFile"
	ErrOnCreate      = "ErrOnCreate"
	ErrVersion       = "ErrVersion"
	ErrSessionClosed = "ErrSessionClosed"
	ErrDeleteRoot    = "ErrDeleteRoot"

	// Err returned by Session only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)

type SessionArgs struct {
	SessionId string
}

type SessionReply struct {
	Err Err
}

type CreateArgs struct {
	SessionId string
	Path      Ppath
	Data      string
	Flags     Flag
}

type CreateReply struct {
	ZNodeName Ppath
	Err       Err
}

type ExistsArgs struct {
	SessionId string
	Path      Ppath
	Watch     Watch
}

type ExistsReply struct {
	Result bool
	Err    Err
}

type GetDataArgs struct {
	SessionId string
	Path      Ppath
	Watch     Watch
}

type GetDataReply struct {
	Data    string
	Version Pversion
	Err     Err
}

type SetDataArgs struct {
	SessionId string
	Path      Ppath
	Data      string
	Version   Pversion
}

type SetDataReply struct {
	Err Err
}

type GetChildrenArgs struct {
	SessionId string
	Path      Ppath
	Watch     Watch
}

type GetChildrenReply struct {
	Children []Ppath
	Err      Err
}

type DeleteArgs struct {
	SessionId string
	Path      Ppath
	Version   Pversion
}

type DeleteReply struct {
	Err Err
}
