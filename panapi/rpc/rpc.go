package rpc

type Err string

const (
	OK         = "OK"
	ErrNoFile  = "ErrNoFile"
	ErrVersion = "ErrVersion"

	// Err returned by Clerk only
	ErrMaybe = "ErrMaybe"

	// For future kvraft lab
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongGroup  = "ErrWrongGroup"
)
