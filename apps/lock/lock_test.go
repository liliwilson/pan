package lock

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"pan/pan"
	"pan/panapi/rpc"

	tester "6.5840/tester1"
)

const (
	NCLNT    = 10
	NSEC     = 1
	NSERVERS = 5
)

func runLockClient(me int, ch_err chan string, ch_done chan struct{}, clientCrash bool, ts *pan.Test) {
	path := rpc.Ppath("/tester")

	session := ts.MakeSession()
	ck := MakeClerk(session, "lock")

	ck.Acquire(path)
	exists, _ := session.Exists(path+"/bad", rpc.Watch{})
	if exists {
		ch_err <- "Two clients acquired lock at the same time"
		return
	}
	session.Create(path+"/bad", "", rpc.Flag{Ephemeral: true})
	session.Create(path+"/seq-", "", rpc.Flag{Sequential: true})

	choice := rand.Int() % 5
	if choice == 0 && clientCrash {
		ts.Crash(session)
	} else if choice == 1 || choice == 2 {
		time.Sleep(5 * time.Second)
		session.EndSession()
	} else {
		time.Sleep(5 * time.Second)
		session.Delete(path+"/bad", 1)
		ck.Release(path)
	}

	ch_done <- struct{}{}
}

func runClients(t *testing.T, title string, nclnts int, clientCrash bool, leaderCrash bool) {
	ts := pan.MakeTest(t, title, nclnts, NSERVERS, true, leaderCrash, clientCrash, false, -1, false)
	defer ts.Cleanup()

	session := ts.MakeSession()
	seqPath := rpc.Ppath("/tester/seq-")

	ch_err := make(chan string)
	ch_dones := make([]chan struct{}, nclnts)
	ch_crash := make(chan struct{})
	for i := range nclnts {
		ch_dones[i] = make(chan struct{})
		go runLockClient(i, ch_err, ch_dones[i], clientCrash, ts)
	}

	if leaderCrash {
		go func() {
			defer func() { ts.Group(tester.GRP0).ConnectAll() }()
			for i := 0; true; i = (i + 1) % NSERVERS {
				select {
				case <-ch_crash:
					return
				default:
					ts.Group(tester.GRP0).ShutdownServer(i)
					time.Sleep(time.Second)
					ts.Group(tester.GRP0).StartServer(i)
				}
			}
		}()
	}

	for i := range nclnts {
		select {
		case err := <-ch_err:
			t.Fatal(err)
		case <-ch_dones[i]:
			continue
		}
	}

	if leaderCrash {
		ch_crash <- struct{}{}
	}

	name, _ := session.Create(seqPath, "", rpc.Flag{Sequential: true})
	if name != rpc.Ppath(fmt.Sprintf("%s%d", seqPath, nclnts)) {
		ts.Fatalf("Should have created %s; instead created %s", fmt.Sprintf("%s%d", seqPath, nclnts), name)
	}
}

func TestOneClientNoErrors(t *testing.T) {
	runClients(t, "TestOneClientNoErrors", 1, false, false)
}

func TestManyClientsNoErrors(t *testing.T) {
	runClients(t, "TestManyClientsNoErrors", NCLNT, false, false)
}

func TestOneClientJustClientCrashes(t *testing.T) {
	runClients(t, "TestOneClientJustClientCrashes", 1, true, false)
}

func TestManyClientsJustClientCrashes(t *testing.T) {
	runClients(t, "TestManyClientsJustClientCrashes", NCLNT, true, false)
}

func TestOneClientJustLeaderCrash(t *testing.T) {
	runClients(t, "TestOneClientJustLeaderCrash", 1, false, true)
}

func TestManyClientsJustLeaderCrash(t *testing.T) {
	runClients(t, "TestManyClientsJustLeaderCrash", NCLNT, false, true)
}

func TestOneClientBothClientAndLeaderCrash(t *testing.T) {
	runClients(t, "TestOneClientBothClientAndLeaderCrash", 1, true, true)
}

func TestManyClientsBothClientAndLeaderCrash(t *testing.T) {
	runClients(t, "TestManyClientsBothClientAndLeaderCrash", NCLNT, true, true)
}
