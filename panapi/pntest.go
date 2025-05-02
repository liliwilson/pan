package panapi

import (
	"math/rand"
	"testing"

	"6.5840/tester1"
	"pan/panapi/rpc"
)

// n specifies the length of the string to be generated.
func RandValue(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

type IPNSession interface {
	Create(path rpc.Ppath, data string, flags rpc.Flag) (rpc.Ppath, rpc.Err)

	Delete(path rpc.Ppath, version rpc.Pversion) rpc.Err

	// Watches block (for now........)
	Exists(path rpc.Ppath, watch bool) (bool, rpc.Err)

	GetData(path rpc.Ppath, watch bool) (string, rpc.Pversion, rpc.Err)

	SetData(path rpc.Ppath, data string, version rpc.Pversion) rpc.Err

	GetChildren(path rpc.Ppath, watch bool) ([]rpc.Ppath, rpc.Err)

	Sync(path rpc.Ppath) rpc.Err

	// Ends the current client session
	EndSession()
}

type TestSession struct {
	IPNSession
	Clnt *tester.Clnt
}

type ISessionMaker interface {
	MakeSession() IPNSession
	DeleteSession(IPNSession)
}

type Test struct {
	*tester.Config
	t           *testing.T
	mck         ISessionMaker
	randomfiles bool
}

func MakeTest(t *testing.T, cfg *tester.Config, randomfiles bool, mck ISessionMaker) *Test {
	ts := &Test{
		Config:      cfg,
		t:           t,
		mck:         mck,
		randomfiles: randomfiles,
	}
	return ts
}

func (ts *Test) Cleanup() {
	ts.Config.End()
	ts.Config.Cleanup()
}

func (ts *Test) ConnectClnts(clnts []*tester.Clnt) {
	for _, c := range clnts {
		c.ConnectAll()
	}
}

func (ts *Test) MakeSession() IPNSession {
	return ts.mck.MakeSession()
}
