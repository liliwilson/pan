package panapi

import (
	"testing"
	"math/rand"

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

type Pversion int
type Ppath string
type Flag struct {
	Ephemeral  bool
	Sequential bool
}

type IPNClerk interface {
	Create(path Ppath, data string, flags Flag) (Ppath, rpc.Err)

	Delete(path Ppath, version Pversion) rpc.Err

	// Watches block (for now........)
	Exists(path Ppath, watch bool) (bool, rpc.Err)

	GetData(path Ppath, watch bool) (string, Pversion, rpc.Err)

	SetData(path Ppath, data string, version Pversion) rpc.Err

	GetChildren(path Ppath, watch bool) ([]Ppath, rpc.Err)

	Sync(path Ppath) rpc.Err
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
