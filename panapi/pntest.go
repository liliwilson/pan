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

type IPNClerk interface {
	Create(path rpc.Ppath, data string, flags rpc.Flag) (rpc.Ppath, rpc.Err)

	Delete(path rpc.Ppath, version rpc.Pversion) rpc.Err

	// Watches block (for now........)
	Exists(path rpc.Ppath, watch bool) (bool, rpc.Err)

	GetData(path rpc.Ppath, watch bool) (string, rpc.Pversion, rpc.Err)

	SetData(path rpc.Ppath, data string, version rpc.Pversion) rpc.Err

	GetChildren(path rpc.Ppath, watch bool) ([]rpc.Ppath, rpc.Err)

	Sync(path rpc.Ppath) rpc.Err
}

type TestClerk struct {
	IPNClerk
	Clnt *tester.Clnt
}

type IClerkMaker interface {
	MakeClerk() IPNClerk
	DeleteClerk(IPNClerk)
}

type Test struct {
	*tester.Config
	t           *testing.T
	mck         IClerkMaker
	randomfiles bool
}

func MakeTest(t *testing.T, cfg *tester.Config, randomfiles bool, mck IClerkMaker) *Test {
	ts := &Test{
		Config: cfg,
		t: t,
		mck: mck,
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

func (ts *Test) MakeClerk() IPNClerk {
	return ts.mck.MakeClerk()
}
