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
	return dirs[len(dirs)-1]
}

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

type StartSessionArgs struct {
}

type StartSessionReply struct {
	SessionId int
	Err       Err
}

type EndSessionArgs struct {
	SessionId int
}

type EndSessionReply struct {
	Err Err
}

type KeepAliveArgs struct {
	SessionId int
}

type KeepAliveReply struct {
	Err Err
}

type CreateArgs struct {
	SessionId int
	Path      Ppath
	Data      string
	Flags     Flag
}

type CreateReply struct {
	ZNodeName Ppath
	CreatedBy int // the session ID of the creator of this znode
	Err       Err
}

type ExistsArgs struct {
	SessionId int
	Path      Ppath
	Watch     Watch
}

type ExistsReply struct {
	Result bool
	Err    Err
}

type GetDataArgs struct {
	SessionId int
	Path      Ppath
	Watch     Watch
}

type GetDataReply struct {
	Data    string
	Version Pversion
	Err     Err
}

type SetDataArgs struct {
	SessionId int
	Path      Ppath
	Data      string
	Version   Pversion
}

type SetDataReply struct {
	Err Err
}

type GetChildrenArgs struct {
	SessionId int
	Path      Ppath
	Watch     Watch
}

type GetChildrenReply struct {
	Children []Ppath
	Err      Err
}

type DeleteArgs struct {
	SessionId int
	Path      Ppath
	Version   Pversion
}

type DeleteReply struct {
	Err Err
}

type GetHighestSeqArgs struct {
	SessionId int
	Path      Ppath
}

type GetHighestSeqReply struct {
	SeqNum int
	Err    Err
}
