package pan

import (
	"testing"

	"pan/panapi"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type Test struct {
	t *testing.T
	*panapi.Test
	part         string // to print which test it is
	nclients     int
	nservers     int
	serverCrash  bool
	clientCrash  bool
	partitions   bool
	maxraftstate int // probably unecessary?
	randomfiles  bool
}

const Gid = tester.GRP0

func MakeTest(t *testing.T, part string, nclients int, nservers int, reliable bool, serverCrash bool, clientCrash bool, partitions bool, maxraftstate int, randomfiles bool) *Test {
	ts := &Test{
		t:            t,
		part:         part,
		nclients:     nclients,
		nservers:     nservers,
		serverCrash:  serverCrash,
		clientCrash:  clientCrash,
		partitions:   partitions,
		maxraftstate: maxraftstate,
		randomfiles:  randomfiles,
	}
	cfg := tester.MakeConfig(t, nservers, reliable, ts.StartPanServer)
	ts.Test = panapi.MakeTest(t, cfg, randomfiles, ts)
	return ts
}

func (ts *Test) StartPanServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister) []tester.IService {
	return StartPanServer(servers, gid, me, persister, ts.maxraftstate)
}

func (ts *Test) cleanup() {
	ts.Test.Cleanup()
}

func (ts *Test) MakeClerk() panapi.IPNClerk {
	clnt := ts.Config.MakeClient()
	ck := MakeClerk(clnt, ts.Group(Gid).SrvNames())
	return &panapi.TestClerk{ck, clnt}
}

func (ts *Test) DeleteClerk(ck panapi.IPNClerk) {
	tck := ck.(*panapi.TestClerk)
	ts.DeleteClient(tck.Clnt)
}
