package apps

import (
	"testing"

	tester "6.5840/tester1"
	"6.5840/labrpc"
	"pan/pan"
	"pan/panapi"
)

type ISessionMaker interface {
	MakeSession() panapi.IPNSession
	DeleteSession(panapi.IPNSession)
}

type TestApp struct {
	mck ISessionMaker
}

func MakeTestApp(t *testing.T, clientCrash bool, leaderCrash bool) *TestApp {
	return &TestApp{}
}

func StartAppServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister) []tester.IService {
	return pan.StartPanServer(servers, gid, me, persister, -1)
}
