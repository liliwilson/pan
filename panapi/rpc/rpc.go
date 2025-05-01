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

type Err string

const (
	OK               = "OK"
	ErrNoFile        = "ErrNoFile"
	ErrOnCreate      = "ErrOnCreate"
	ErrVersion       = "ErrVersion"
	ErrSessionClosed = "ErrSessionClosed"
	ErrDeleteRoot    = "ErrDeleteRoot"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)

type CreateArgs struct {
	Path  Ppath
	Data  string
	Flags Flag
}

type CreateReply struct {
	ZNodeName Ppath
	Err       Err
}

type ExistsArgs struct {
	Path  Ppath
	Watch bool
}

type ExistsReply struct {
	Result bool
	Err    Err
}

type GetDataArgs struct {
	Path  Ppath
	Watch bool
}

type GetDataReply struct {
	Data    string
	Version Pversion
	Err     Err
}

type SetDataArgs struct {
	Path    Ppath
	Data    string
	Version Pversion
}

type SetDataReply struct {
	Err Err
}

type GetChildrenArgs struct {
	Path  Ppath
	Watch bool
}

type GetChildrenReply struct {
	Children []Ppath
	Err      Err
}

type DeleteArgs struct {
	Path    Ppath
	Version Pversion
}

type DeleteReply struct {
	Err Err
}
