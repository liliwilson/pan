package pan

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	// "6.5840/kvraft1/rsm"
	// "6.5840/kvsrv1/rpc"
	// "6.5840/rpc.
	tester "6.5840/tester1"

	"pan/panapi"
	"pan/panapi/rpc"
)

func compareGetData(file string, expectedData string, expectedVersion rpc.Pversion, actualData string, actualVersion rpc.Pversion) (bool, string) {
	if expectedData != actualData || expectedVersion != actualVersion {
		return false, fmt.Sprintf("Got %s with data '%s' and version %d; expected data '%s' with version %d\n", file, actualData, actualVersion, expectedData, expectedVersion)
	}
	return true, ""
}

func compareGetChildren(dir string, expectedChildren []rpc.Ppath, actualChildren []rpc.Ppath) (bool, string) {
	if !reflect.DeepEqual(expectedChildren, actualChildren) {
		return false, fmt.Sprintf("Got %s with children %v; expected children %v\n", dir, actualChildren, expectedChildren)
	}
	return true, ""
}

func TestBasic(t *testing.T) {
	ts := MakeTest(t, "Synchronous One-client Basic", 1, 3, true, false, false, false, -1, false)
	defer ts.Cleanup()
	ck := ts.MakeSession()
	// Test Create and Exists
	ck.Create("/a", "", rpc.Flag{})
	ck.Create("/a/b", "hello", rpc.Flag{})

	exists, _ := ck.Exists("/a", rpc.Watch{ShouldWatch: false, Callback: func() {}})
	if !exists {
		ts.t.Fatalf("/a should exist\n")
	}

	exists, _ = ck.Exists("/a/b", rpc.Watch{ShouldWatch:false, Callback: func() {}})
	if !exists {
		ts.t.Fatalf("/a/b should exist\n")
	}

	exists, _ = ck.Exists("/a/b/c", rpc.Watch{ShouldWatch: false, Callback: func() {}})
	if exists {
		ts.t.Fatalf("/a/b/c should not exist\n")
	}

	exists, _ = ck.Exists("/a/c", rpc.Watch{ShouldWatch: false, Callback: func() {}})
	if exists {
		ts.t.Fatalf("/a/c should not exist\n")
	}

	// Test GetData
	data, version, _ := ck.GetData("/a/b", rpc.Watch{ShouldWatch: false, Callback: func() {}})
	if ok, err := compareGetData("/a/b", "hello", 1, data, version); !ok {
		ts.t.Fatal(err)
	}
	// TestSetData
	ck.SetData("/a/b", "bye", 1)
	data, version, _ = ck.GetData("/a/b", rpc.Watch{ShouldWatch: false, Callback: func() {}})
	if ok, err := compareGetData("/a/b", "bye", 2, data, version); !ok {
		ts.t.Fatal(err)
	}
	// Test Get Children
	ck.Create("/a/c", "1", rpc.Flag{})
	ck.Create("/a/d", "2", rpc.Flag{})
	children, _ := ck.GetChildren("/a", rpc.Watch{ShouldWatch: false, Callback: func() {}})
	if ok, err := compareGetChildren("/a", []rpc.Ppath{"b", "c", "d"}, children); !ok {
		ts.t.Fatal(err)
	}
	// Test Delete and Exists
	ck.Delete("/a/b", 2)
	exists, _ = ck.Exists("/a/b", rpc.Watch{ShouldWatch: false, Callback: func() {}})
	if exists {
		ts.t.Fatal("/a/b should not exist\n")
	}
}

// Have concurrent clients create sequential nodes
func TestManyClientSequential(t *testing.T) {
	const (
		nclients = 3
		iters    = 100
	)
	ts := MakeTest(t, "Many Client Sequential", nclients, 3, true, false, false, false, -1, false)
	defer ts.Cleanup()
	ck := ts.MakeSession()
	zname, err := ck.Create("/a/seq-", "data", rpc.Flag{Sequential: true})
	if err != rpc.OK || zname != rpc.Ppath("/a/seq-0") {
		ts.t.Fatalf("Initial sequential znode name was %s; expected /a/seq-0\n", zname)
	}
	cks := make([]panapi.IPNSession, nclients)
	chs := make([]chan int, nclients)
	for i := range 3 {
		chs[i] = make(chan int)
		cks[i] = ts.MakeSession()
		go func(idx int) {
			start := time.Now()
			count := 0
			for time.Since(start) < 5 * time.Millisecond {
				cks[i].Create("/a/seq-", "data", rpc.Flag{Sequential: true})
				count += 1
			}
			chs[i] <- count
		}(i)
	}
	c0 := <-chs[0]
	c1 := <-chs[1]
	c2 := <-chs[2]
	total := c0 + c1 + c2 + 1
	zname, err = ck.Create("/a/seq-", "data", rpc.Flag{Sequential: true})
	if err != rpc.OK || zname != rpc.Ppath(fmt.Sprintf("/a/seq-%d", total)) {
		ts.t.Fatalf("Created %s after %d previous sequential znode creations\n", zname, total)
	}
}

// disconnect clerk and wait to see if ephemeral are gone
func TestEphemeral(t *testing.T) {
	ts := MakeTest(t, "Ephemeral znodes", 1, 3, true, false, true, false, -1, false)
	defer ts.Cleanup()
	path := rpc.Ppath("/a/testEpheral")
	ck1 := ts.MakeSession()
	ck1.Create(path, "data", rpc.Flag{Ephemeral: true})
	exists, _ := ck1.Exists(path, rpc.Watch{ShouldWatch: false, Callback: func() {}})
	if !exists {
		ts.t.Fatal("Znode is missing after creation\n")
	}
	ck1.EndSession()
	// Allow session end to propogate
	time.Sleep(time.Second * 1)
	ck := ts.MakeSession()
	exists, _ = ck.Exists(path, rpc.Watch{ShouldWatch: false, Callback: func() {}})
	if exists {
		ts.t.Fatal("Ephemeral znode exists after creator disconnected\n")
	}
}

// create a lot of sequential and ephemeral nodes; disconnect;
// reconnect and make sure next node is higher
func TestSequentialMonotonicallyIncreases(t *testing.T) {
	const (
		nclients = 3
		iters    = 100
	)
	ts := MakeTest(t, "Sequential Monotonically Increases", 1, 3, true, false, true, false, -1, false)
	defer ts.Cleanup()
	path := rpc.Ppath("/b/seq-")
	ck := ts.MakeSession()
	cks := make([]panapi.IPNSession, nclients)
	chs := make([]chan int, nclients)
	for i := range nclients {
		chs[i] = make(chan int)
		cks[i] = ts.MakeSession()
		go func(idx int) {
			start := time.Now()
			count := 0
			for time.Since(start) < 5 * time.Millisecond {
				cks[i].Create(path, "data", rpc.Flag{Sequential: true, Ephemeral: true})
				count += 1
			}
			cks[i].EndSession()
			chs[i] <- count
		}(i)
	}
	c0 := <-chs[0]
	c1 := <-chs[1]
	c2 := <-chs[2]
	children, err := ck.GetChildren("/b", rpc.Watch{ShouldWatch: false, Callback: func() {}})
	if err != rpc.OK || len(children) > 0 {
		ts.t.Fatal("/b should have no children after nodes crashed\n")
	}
	fname, err := ck.Create(path, "data", rpc.Flag{Sequential: true, Ephemeral: true})
	if err != rpc.OK || fname != rpc.Ppath(fmt.Sprintf("/b/seq-%d", c0+c1+c2)) {
		ts.t.Fatalf("Created %s when %d previous ephermeral znodes were created", fname, c0+c1+c2)
	}
}

func (ts *Test) GenericTest() {
	const (
		NITER  = 3
		NSEC   = 1
		T      = NSEC * time.Second
		NFILES = 100
	)
	defer ts.Cleanup()

	ch_partitioner := make(chan bool)
	ch_spawn := make(chan panapi.TestType)
	ch_crash := make(chan struct{})
	ch_err := make(chan string)
	ck := ts.MakeSession()
	results := make([]panapi.TestType, NITER)
	for i := 0; i < NITER; i++ {
		file := rpc.Ppath(fmt.Sprintf("/a/b%d/f-", i))
		go func() {
			tt := ts.StartSessionsAndWait(ts.nclients, T, file, ts.clientCrash, ch_err)
			ch_spawn <- tt
		}()

		// Let clients perform ops without interruption
		time.Sleep(100 * time.Millisecond)

		if ts.leaderCrash {
			go func() {
				for i := 0; i < ts.nservers; i++ {
					ts.Group(Gid).ShutdownServer(i)
					time.Sleep(T)
					ts.Group(Gid).StartServer(i)
				}
				ts.Group(Gid).ConnectAll()
				ch_crash <- struct{}{}
			}()
		}

		if ts.partitions {
			go ts.Partitioner(Gid, ch_partitioner)
		}

		if ts.leaderCrash {
			<-ch_crash // waits for leaders to stop crashing
		}
		var res panapi.TestType
		select {
		case res = <-ch_spawn:
		case err_msg := <-ch_err:
			ts.t.Fatal(err_msg)
		}

		if ts.partitions {
			// this if block ends the partitioning
			ch_partitioner <- true
			<-ch_partitioner
			ts.Group(Gid).ConnectAll()
			time.Sleep(T)
		}

		// at this point, all network should be good
		results[i] = res
	}
	time.Sleep(10 * time.Second)
	for i := range NITER {
		parent := rpc.Ppath(fmt.Sprintf("/a/b%d", i))
		ts.CheckRes(ck, results[i], parent)
	}
}

func TestNoErrors(t *testing.T) {
	ts := MakeTest(t, "No Unreliability", 3, 5, true, false, false, false, -1, false)
	tester.AnnotateTest("TestNoErrors", 5)
	ts.GenericTest()
}

func TestClientCrashes(t *testing.T) {
	ts := MakeTest(t, "Test Client Crashes", 3, 5, true, false, true, false, -1, false)
	tester.AnnotateTest("TestClientCrashes", 5)
	ts.GenericTest()
}

func TestLeaderCrashes(t *testing.T) {
	ts := MakeTest(t, "Test Leader Crashes", 3, 5, true, true, false, false, -1, false)
	tester.AnnotateTest("TestLeaderCrashes", 5)
	ts.GenericTest()
}

func TestPartitions(t *testing.T) {
	ts := MakeTest(t, "Test Partitions", 3, 5, true, false, false, true, -1, false)
	tester.AnnotateTest("TestPartitions", 5)
	ts.GenericTest()
}

func TestClientCrashesLeaderCrashes(t *testing.T) {
	ts := MakeTest(t, "Test Client and Leader Crashes", 3, 5, true, true, true, false, -1, false)
	tester.AnnotateTest("TestClientCrashesLeaderCrashes", 5)
	ts.GenericTest()
}

func TestClientCrashesWithPartitions(t *testing.T) {
	ts := MakeTest(t, "Test Client Crashes and Partitions", 3, 5, true, false, true, true, -1, false)
	tester.AnnotateTest("TestClientCrashesWithPartitions", 5)
	ts.GenericTest()
}

func TestLeaderCrashesWithPartitions(t *testing.T) {
	ts := MakeTest(t, "Test Leader Crashes and Partitions", 3, 5, true, true, false, true, -1, false)
	tester.AnnotateTest("TestLeaderCrashesWithPartitions", 5)
	ts.GenericTest()
}

func TestClientCrashesLeaderCrashesWithPartitions(t *testing.T) {
	ts := MakeTest(t, "Test Client Crashes and Leader Crashes and Partitions", 3, 5, true, true, true, true, -1, false)
	tester.AnnotateTest("TestClientCrashesLeaderCrashesWithPartitions", 5)
	ts.GenericTest()
}
