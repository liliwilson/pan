package pan

import (
	"fmt"
	"reflect"
	"slices"
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

	exists, _ := ck.Exists("/a", rpc.Watch{ShouldWatch: false, Callback: func(wa rpc.WatchArgs) {}})
	if !exists {
		ts.t.Fatalf("/a should exist\n")
	}

	exists, _ = ck.Exists("/a/b", rpc.Watch{ShouldWatch: false, Callback: func(wa rpc.WatchArgs) {}})
	if !exists {
		ts.t.Fatalf("/a/b should exist\n")
	}

	exists, _ = ck.Exists("/a/b/c", rpc.Watch{ShouldWatch: false, Callback: func(wa rpc.WatchArgs) {}})
	if exists {
		ts.t.Fatalf("/a/b/c should not exist\n")
	}

	exists, _ = ck.Exists("/a/c", rpc.Watch{ShouldWatch: false, Callback: func(wa rpc.WatchArgs) {}})
	if exists {
		ts.t.Fatalf("/a/c should not exist\n")
	}

	// Test GetData
	data, version, _ := ck.GetData("/a/b", rpc.Watch{ShouldWatch: false, Callback: func(wa rpc.WatchArgs) {}})
	if ok, err := compareGetData("/a/b", "hello", 1, data, version); !ok {
		ts.t.Fatal(err)
	}
	// TestSetData
	ck.SetData("/a/b", "bye", 1)
	data, version, _ = ck.GetData("/a/b", rpc.Watch{ShouldWatch: false, Callback: func(wa rpc.WatchArgs) {}})
	if ok, err := compareGetData("/a/b", "bye", 2, data, version); !ok {
		ts.t.Fatal(err)
	}
	// Test Get Children
	ck.Create("/a/c", "1", rpc.Flag{})
	ck.Create("/a/d", "2", rpc.Flag{})
	children, _ := ck.GetChildren("/a", rpc.Watch{ShouldWatch: false, Callback: func(wa rpc.WatchArgs) {}})
	if ok, err := compareGetChildren("/a", []rpc.Ppath{"b", "c", "d"}, children); !ok {
		ts.t.Fatal(err)
	}
	// Test Delete and Exists
	ck.Delete("/a/b", 2)
	exists, _ = ck.Exists("/a/b", rpc.Watch{ShouldWatch: false, Callback: func(wa rpc.WatchArgs) {}})
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
			for time.Since(start) < 5*time.Millisecond {
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
	exists, _ := ck1.Exists(path, rpc.Watch{ShouldWatch: false, Callback: func(wa rpc.WatchArgs) {}})
	if !exists {
		ts.t.Fatal("Znode is missing after creation\n")
	}
	ck1.EndSession()
	// Allow session end to propogate
	time.Sleep(time.Second * 1)
	ck := ts.MakeSession()
	exists, _ = ck.Exists(path, rpc.Watch{ShouldWatch: false, Callback: func(wa rpc.WatchArgs) {}})
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
			for time.Since(start) < 5*time.Millisecond {
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
	children, err := ck.GetChildren("/b", rpc.Watch{ShouldWatch: false, Callback: func(wa rpc.WatchArgs) {}})
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
	// ch_crash := make(chan struct{})
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

		if ts.partitions {
			go ts.Partitioner(Gid, ch_partitioner)
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

		if ts.leaderCrash {
			for i := 0; i < ts.nservers; i++ {
				ts.Group(Gid).ShutdownServer(i)
			}
			time.Sleep(T)
			for i := 0; i < ts.nservers; i++ {
				ts.Group(Gid).StartServer(i)
			}
			ts.Group(Gid).ConnectAll()
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

func TestJustClientCrashes(t *testing.T) {
	ts := MakeTest(t, "Test Client Crashes", 3, 5, true, false, true, false, -1, false)
	tester.AnnotateTest("TestClientCrashes", 5)
	ts.GenericTest()
}

func TestJustLeaderCrashes(t *testing.T) {
	ts := MakeTest(t, "Test Leader Crashes", 3, 5, true, true, false, false, -1, false)
	tester.AnnotateTest("TestLeaderCrashes", 5)
	ts.GenericTest()
}

// func TestJustPartitions(t *testing.T) {
// 	ts := MakeTest(t, "Test Partitions", 3, 5, true, false, false, true, -1, false)
// 	tester.AnnotateTest("TestPartitions", 5)
// 	ts.GenericTest()
// }

func TestBothClientCrashesLeaderCrashes(t *testing.T) {
	ts := MakeTest(t, "Test Client and Leader Crashes", 3, 5, true, true, true, false, -1, false)
	tester.AnnotateTest("TestClientCrashesLeaderCrashes", 5)
	ts.GenericTest()
}

// func TestBothClientCrashesWithPartitions(t *testing.T) {
// 	ts := MakeTest(t, "Test Client Crashes and Partitions", 3, 5, true, false, true, true, -1, false)
// 	tester.AnnotateTest("TestClientCrashesWithPartitions", 5)
// 	ts.GenericTest()
// }
//
// func TestBothLeaderCrashesWithPartitions(t *testing.T) {
// 	ts := MakeTest(t, "Test Leader Crashes and Partitions", 3, 5, true, true, false, true, -1, false)
// 	tester.AnnotateTest("TestLeaderCrashesWithPartitions", 5)
// 	ts.GenericTest()
// }
//
// func TestAllClientCrashesLeaderCrashesWithPartitions(t *testing.T) {
// 	ts := MakeTest(t, "Test Client Crashes and Leader Crashes and Partitions", 3, 5, true, true, true, true, -1, false)
// 	tester.AnnotateTest("TestClientCrashesLeaderCrashesWithPartitions", 5)
// 	ts.GenericTest()
// }

func TestJustWatch(t *testing.T) {
	ts := MakeTest(t, "Test Watches on all three functions", 3, 5, true, false, false, false, -1, false)
	tester.AnnotateTest("TestWatchBasic", 5)
	defer ts.Cleanup()

	ck := ts.MakeSession()
	const (
		NITERS = 60
	)
	for i := range NITERS {
		dir := rpc.Ppath(fmt.Sprintf("/a/b%d", i))
		ck.Create(dir, "init", rpc.Flag{})
		if i%3 == 0 { // Exists
			testfile := dir + "/create"
			ch_create := make(chan rpc.WatchArgs)
			ck.Exists(testfile, rpc.Watch{ShouldWatch: true, Callback: func(args rpc.WatchArgs) {
				ch_create <- args
			}})
			ck.Create(testfile, "start", rpc.Flag{})
			received := <-ch_create
			if received.EventType != rpc.NodeCreated {
				ts.t.Fatalf("Expected rpc.NodeCreated as the event type; got %s", received.EventType)
			} else if received.Path != testfile {
				ts.t.Fatalf("Expected %s as the Path; got %s", testfile, received.Path)
			}
			exists, _ := ck.Exists(testfile, rpc.Watch{ShouldWatch: false, Callback: rpc.EmptyWatch})
			if !exists {
				ts.t.Fatalf("%s does not exist after watch signalling creation", testfile)
			}
			ck.Exists(testfile, rpc.Watch{ShouldWatch: true, Callback: func(args rpc.WatchArgs) {
				ch_create <- args
			}})
			ck.Delete(testfile, 1)
			received = <-ch_create
			if received.EventType != rpc.NodeDeleted {
				ts.t.Fatalf("Expected rpc.NodeDeleted as the event type; got %s", received.EventType)
			} else if received.Path != testfile {
				ts.t.Fatalf("Expected %s as the Path; got %s", testfile, received.Path)
			}
			exists, _ = ck.Exists(testfile, rpc.Watch{ShouldWatch: false, Callback: rpc.EmptyWatch})
			if exists {
				ts.t.Fatalf("%s exists after watch signalling deletion", testfile)
			}
		} else if i%3 == 1 { // GetData
			testfile := dir + "/create"
			ck.Create(testfile, "init", rpc.Flag{})
			ch_newdata := make(chan rpc.WatchArgs)
			ck.GetData(testfile, rpc.Watch{ShouldWatch: true, Callback: func(args rpc.WatchArgs) {
				ch_newdata <- args
			}})
			randString := panapi.RandValue(15)
			ck.SetData(testfile, randString, 1)
			received := <-ch_newdata
			if received.EventType != rpc.NodeDataChanged {
				ts.t.Fatalf("Expected rpc.NodeDataChanged as the event type; got %s", received.EventType)
			} else if received.Path != testfile {
				ts.t.Fatalf("Expected %s as the Path; got %s", testfile, received.Path)
			}
			data, _, _ := ck.GetData(testfile, rpc.Watch{ShouldWatch: false, Callback: rpc.EmptyWatch})
			if data != randString {
				ts.t.Fatal("Got different data from read than what was written")
			}
		} else { // GetChildren
			testfile1 := dir + "/create"
			testfile2 := dir + "/delete"
			ck.Create(testfile2, "", rpc.Flag{})
			ch_children := make(chan rpc.WatchArgs)
			ck.GetChildren(dir, rpc.Watch{ShouldWatch: true, Callback: func(args rpc.WatchArgs) {
				ch_children <- args
			}})
			ck.Create(testfile1, "", rpc.Flag{})
			received := <-ch_children
			if received.EventType != rpc.NodeChildrenChanged {
				ts.t.Fatalf("Expected rpc.NodeChildrenChanged as the event type; got %s", received.EventType)
			} else if received.Path != dir {
				ts.t.Fatalf("Expected %s as the Path; got %s", dir, received.Path)
			}
			children, _ := ck.GetChildren(dir, rpc.Watch{ShouldWatch: false})
			if len(children) != 2 || children[0] != rpc.Ppath(testfile1.Suffix()) || children[1] != rpc.Ppath(testfile2.Suffix()) {
				ts.t.Fatalf("Got %v from GetChildren; should be [%s, %s]", children, testfile1, testfile2)
			}
			ck.GetChildren(dir, rpc.Watch{ShouldWatch: true, Callback: func(args rpc.WatchArgs) {
				ch_children <- args
			}})
			ck.Delete(testfile2, 1)
			received = <-ch_children
			children, _ = ck.GetChildren(dir, rpc.Watch{ShouldWatch: false})
			if len(children) != 1 || children[0] != rpc.Ppath(testfile1.Suffix()) {
				ts.t.Fatalf("Got %v from watch; should be [create]", children)
			}
		}
	}
}

func TestWatchWithServerFailure(t *testing.T) {
	ts := MakeTest(t, "Test Watches on all three functions", 3, 5, true, true, false, false, -1, false)
	tester.AnnotateTest("TestWatchBasic", 5)
	defer ts.Cleanup()

	ck := ts.MakeSession()
	const (
		NITERS = 3
	)
	for i := range NITERS {
		dir := rpc.Ppath(fmt.Sprintf("/a/b%d", i))
		ck.Create(dir, "init", rpc.Flag{})
		if i%3 == 0 { // Exists
			testfile := dir + "/create"
			ch_create := make(chan rpc.WatchArgs)
			ck.Exists(testfile, rpc.Watch{ShouldWatch: true, Callback: func(args rpc.WatchArgs) {
				ch_create <- args
			}})
			ck.Create(testfile, "start", rpc.Flag{})
			received := <-ch_create
			if received.EventType != rpc.NodeCreated {
				ts.t.Fatalf("Expected rpc.NodeCreated as the event type; got %s", received.EventType)
			} else if received.Path != testfile {
				ts.t.Fatalf("Expected %s as the Path; got %s", testfile, received.Path)
			}
			exists, _ := ck.Exists(testfile, rpc.Watch{ShouldWatch: false, Callback: rpc.EmptyWatch})
			if !exists {
				ts.t.Fatalf("%s does not exist after watch signalling creation", testfile)
			}
			ck.Exists(testfile, rpc.Watch{ShouldWatch: true, Callback: func(args rpc.WatchArgs) {
				ch_create <- args
			}})
			for i := 0; i < ts.nservers; i++ {
				ts.Group(Gid).ShutdownServer(i)
			}
			time.Sleep(time.Second)
			for i := 0; i < ts.nservers; i++ {
				ts.Group(Gid).StartServer(i)
			}
			ts.Group(Gid).ConnectAll()
			ck.Delete(testfile, 1)
			received = <-ch_create
			if received.EventType != rpc.NodeDeleted {
				ts.t.Fatalf("Expected rpc.NodeDeleted as the event type; got %s", received.EventType)
			} else if received.Path != testfile {
				ts.t.Fatalf("Expected %s as the Path; got %s", testfile, received.Path)
			}
			exists, _ = ck.Exists(testfile, rpc.Watch{ShouldWatch: false, Callback: rpc.EmptyWatch})
			if exists {
				ts.t.Fatalf("%s exists after watch signalling deletion", testfile)
			}
		} else if i%3 == 1 { // GetData
			testfile := dir + "/create"
			ck.Create(testfile, "init", rpc.Flag{})
			ch_newdata := make(chan rpc.WatchArgs)
			ck.GetData(testfile, rpc.Watch{ShouldWatch: true, Callback: func(args rpc.WatchArgs) {
				ch_newdata <- args
			}})
			for i := 0; i < ts.nservers; i++ {
				ts.Group(Gid).ShutdownServer(i)
			}
			time.Sleep(time.Second)
			for i := 0; i < ts.nservers; i++ {
				ts.Group(Gid).StartServer(i)
			}
			ts.Group(Gid).ConnectAll()
			randString := panapi.RandValue(15)
			ck.SetData(testfile, randString, 1)
			received := <-ch_newdata
			if received.EventType != rpc.NodeDataChanged {
				ts.t.Fatalf("Expected rpc.NodeDataChanged as the event type; got %s", received.EventType)
			} else if received.Path != testfile {
				ts.t.Fatalf("Expected %s as the Path; got %s", testfile, received.Path)
			}
			data, _, _ := ck.GetData(testfile, rpc.Watch{ShouldWatch: false, Callback: rpc.EmptyWatch})
			if data != randString {
				ts.t.Fatal("Got different data from read than what was written")
			}
		} else { // GetChildren
			testfile1 := dir + "/create"
			testfile2 := dir + "/delete"
			ck.Create(testfile2, "", rpc.Flag{})
			ch_children := make(chan rpc.WatchArgs)
			ck.GetChildren(dir, rpc.Watch{ShouldWatch: true, Callback: func(args rpc.WatchArgs) {
				ch_children <- args
			}})
			for i := 0; i < ts.nservers; i++ {
				ts.Group(Gid).ShutdownServer(i)
			}
			time.Sleep(time.Second)
			for i := 0; i < ts.nservers; i++ {
				ts.Group(Gid).StartServer(i)
			}
			ts.Group(Gid).ConnectAll()
			ck.Create(testfile1, "", rpc.Flag{})
			received := <-ch_children
			if received.EventType != rpc.NodeChildrenChanged {
				ts.t.Fatalf("Expected rpc.NodeChildrenChanged as the event type; got %s", received.EventType)
			} else if received.Path != dir {
				ts.t.Fatalf("Expected %s as the Path; got %s", dir, received.Path)
			}
			children, _ := ck.GetChildren(dir, rpc.Watch{ShouldWatch: false})
			if len(children) != 2 || children[0] != rpc.Ppath(testfile1.Suffix()) || children[1] != rpc.Ppath(testfile2.Suffix()) {
				ts.t.Fatalf("Got %v from GetChildren; should be [%s, %s]", children, testfile1, testfile2)
			}
			ck.GetChildren(dir, rpc.Watch{ShouldWatch: true, Callback: func(args rpc.WatchArgs) {
				ch_children <- args
			}})
			ck.Delete(testfile2, 1)
			received = <-ch_children
			children, _ = ck.GetChildren(dir, rpc.Watch{ShouldWatch: false})
			if len(children) != 1 || children[0] != rpc.Ppath(testfile1.Suffix()) {
				ts.t.Fatalf("Got %v from watch; should be [create]", children)
			}
		}
	}
}

// Create an ephemeral znode, and then crash. Watching client should see notif
// Implicitly tests disconnecting client causes ephemeral znode to disappear
func TestWatchEphermeral(t *testing.T) {
	ts := MakeTest(t, "Test Watches on Ephemeral Znodes", 3, 5, true, false, false, false, -1, false)
	tester.AnnotateTest("TestWatchEphemeral", 5)
	defer ts.Cleanup()

	created := make(chan struct{})
	crash := make(chan struct{})
	ck := ts.MakeSession()
	go func() {
		ck1 := ts.MakeSession()
		ck1.Create("/a/b", "data", rpc.Flag{Ephemeral: true})
		created <- struct{}{}
		<-crash
		ts.Crash(ck1)
	}()

	<-created // /a/b has been created
	ch_watch := make(chan struct{})
	exists, _ := ck.Exists("/a/b", rpc.Watch{
		ShouldWatch: true,
		Callback:    func(_ rpc.WatchArgs) { ch_watch <- struct{}{} },
	})
	if !exists {
		ts.t.Fatal("Expected /a/b to exist after creation")
	}
	crash <- struct{}{}
	seen := false
	now := time.Now()
	for !seen || time.Since(now) < 1*time.Second {
		select {
		case <-ch_watch:
			seen = true
			now = time.Now()
		default:
			exists, _ := ck.Exists("/a/b", rpc.Watch{ShouldWatch: false, Callback: func(_ rpc.WatchArgs) {}})
			if exists && seen {
				ts.t.Fatal("/a/b is visible after watch triggered")
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestWatchEphermeralServerCrashes(t *testing.T) {
	ts := MakeTest(t, "Test Watches on Ephemeral Znodes", 3, 5, true, true, false, false, -1, false)
	tester.AnnotateTest("TestWatchEphemeral", 5)
	defer ts.Cleanup()

	created := make(chan struct{})
	crash := make(chan struct{})
	ck := ts.MakeSession()
	go func() {
		ck1 := ts.MakeSession()
		ck1.Create("/a/b", "data", rpc.Flag{Ephemeral: true})
		created <- struct{}{}
		<-crash
		ts.Crash(ck1)
	}()

	<-created // /a/b has been created
	ch_watch := make(chan struct{})
	exists, _ := ck.Exists("/a/b", rpc.Watch{
		ShouldWatch: true,
		Callback:    func(_ rpc.WatchArgs) { ch_watch <- struct{}{} },
	})
	if !exists {
		ts.t.Fatal("Expected /a/b to exist after creation")
	}
	for i := 0; i < ts.nservers; i++ {
		ts.Group(Gid).ShutdownServer(i)
	}
	time.Sleep(time.Second)
	for i := 0; i < ts.nservers; i++ {
		ts.Group(Gid).StartServer(i)
	}
	ts.Group(Gid).ConnectAll()
	crash <- struct{}{}
	seen := false
	now := time.Now()
	for !seen || time.Since(now) < 1*time.Second {
		select {
		case <-ch_watch:
			seen = true
			now = time.Now()
		default:
			exists, _ := ck.Exists("/a/b", rpc.Watch{ShouldWatch: false, Callback: func(_ rpc.WatchArgs) {}})
			if exists && seen {
				ts.t.Fatal("/a/b is visible after watch triggered")
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestWatchSequential(t *testing.T) {
	ts := MakeTest(t, "Test Watches on sequential nodes", 3, 5, true, false, false, false, -1, false)
	tester.AnnotateTest("TestWatchSequential", 5)
	defer ts.Cleanup()

	ck := ts.MakeSession()
	ch_path := make(chan rpc.Ppath)
	ck.Exists("/a/b-30", rpc.Watch{ShouldWatch: true, Callback: func(_ rpc.WatchArgs) {
		path, _ := ck.Create("/a/b-", "", rpc.Flag{Sequential: true})
		ch_path <- path
	}})

	ck2 := ts.MakeSession()
	for range 50 {
		ck2.Create("/a/b-", "", rpc.Flag{Sequential: true})
	}
	path := <-ch_path
	children, _ := ck.GetChildren("/a", rpc.Watch{ShouldWatch: false, Callback: rpc.EmptyWatch})
	if len(children) != 51 {
		ts.t.Fatalf("Expected /a to have 51 children; got %d instead", len(children))
	}
	if !slices.Contains(children, rpc.Ppath(path.Suffix())) {
		ts.t.Fatalf("/a does not have znode created by callback")
	}
}

func TestWatchSequentialWithServerCrash(t *testing.T) {
	ts := MakeTest(t, "Test Watches on sequential nodes", 3, 5, true, false, false, false, -1, false)
	tester.AnnotateTest("TestWatchSequential", 5)
	defer ts.Cleanup()

	ck := ts.MakeSession()
	ch_path := make(chan rpc.Ppath)
	ck.Exists("/a/b-30", rpc.Watch{ShouldWatch: true, Callback: func(_ rpc.WatchArgs) {
		path, _ := ck.Create("/a/b-", "", rpc.Flag{Sequential: true})
		ch_path <- path
	}})
	// Crash
	for i := 0; i < ts.nservers; i++ {
		ts.Group(Gid).ShutdownServer(i)
	}
	time.Sleep(time.Second)
	for i := 0; i < ts.nservers; i++ {
		ts.Group(Gid).StartServer(i)
	}
	ts.Group(Gid).ConnectAll()

	ck2 := ts.MakeSession()
	for range 50 {
		ck2.Create("/a/b-", "", rpc.Flag{Sequential: true})
	}
	path := <-ch_path
	children, _ := ck.GetChildren("/a", rpc.Watch{ShouldWatch: false, Callback: rpc.EmptyWatch})
	if len(children) != 51 {
		ts.t.Fatalf("Expected /a to have 51 children; got %d instead", len(children))
	}
	if !slices.Contains(children, rpc.Ppath(path.Suffix())) {
		ts.t.Fatalf("/a does not have znode created by callback")
	}
}
