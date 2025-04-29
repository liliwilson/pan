package panapi

import (
	"testing"

	"6.5840/tester1"
)

type IPNClerk interface {
	Create(Ppath, string, int)
	Exists(Ppath, chan int) bool
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
