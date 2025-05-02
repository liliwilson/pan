package pan

import (
	"testing"

	"6.5840/labrpc"
	"6.5840/tester1"
	"pan/panapi"
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
	ts.Begin(ts.makeTitle())
	return ts
}

func (ts *Test) StartPanServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister) []tester.IService {
	return StartPanServer(servers, gid, me, persister, ts.maxraftstate)
}

func (ts *Test) MakeSession() panapi.IPNSession {
	clnt := ts.Config.MakeClient()
	ck := MakeSession(clnt, ts.Group(Gid).SrvNames())
	return &panapi.TestSession{ck, clnt}
}

func (ts *Test) MakeSessionTo(to []int) panapi.IPNSession {
	ns := ts.Config.Group(Gid).SrvNamesTo(to)
	clnt := ts.Config.MakeClientTo(ns)
	ck := MakeSession(clnt, ts.Group(Gid).SrvNames())
	return &panapi.TestSession{ck, clnt}
}

func (ts *Test) DeleteSession(ck panapi.IPNSession) {
	tck := ck.(*panapi.TestSession)
	ts.DeleteClient(tck.Clnt)
}

func (ts *Test) cleanup() {
	ts.Test.Cleanup()
}

func (ts *Test) makeTitle() string {
	title := "Test: "
	if ts.serverCrash {
		// peers re-start, and thus persistence must work.
		title = title + "server restarts, "
	}
	if ts.clientCrash {
		title = title + "client restarts, "
	}
	if ts.partitions {
		// the network may partition
		title = title + "partitions, "
	}
	if ts.maxraftstate != -1 {
		title = title + "snapshots, "
	}
	if ts.randomfiles {
		title = title + "random files, "
	}
	if ts.nclients > 1 {
		title = title + "many clients"
	} else {
		title = title + "one client"
	}
	title = title + " (" + ts.part + ")" // 4A, 4B, 4C
	return title
}
