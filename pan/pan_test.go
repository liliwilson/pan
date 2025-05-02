package pan

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	// "6.5840/kvraft1/rsm"
	// "6.5840/kvsrv1/rpc"
	// "6.5840/rpc.
	// tester "6.5840/tester1"

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

	exists, _ := ck.Exists("/a", false)
	if !exists {
		ts.t.Fatalf("/a should exist\n")
	}

	exists, _ = ck.Exists("/a/b", false)
	if !exists {
		ts.t.Fatalf("/a/b should exist\n")
	}

	exists, _ = ck.Exists("/a/b/c", false)
	if exists {
		ts.t.Fatalf("/a/b/c should not exist\n")
	}

	exists, _ = ck.Exists("/a/c", false)
	if exists {
		ts.t.Fatalf("/a/c should not exist\n")
	}

	// Test GetData
	data, version, _ := ck.GetData("/a/b", false)
	if ok, err := compareGetData("/a/b", "hello", 1, data, version); !ok {
		ts.t.Fatal(err)
	}
	// TestSetData
	ck.SetData("/a/b", "bye", 1)
	data, version, _ = ck.GetData("/a/b", false)
	if ok, err := compareGetData("/a/b", "bye", 2, data, version); !ok {
		ts.t.Fatal(err)
	}
	// Test Get Children
	ck.Create("/a/c", "1", rpc.Flag{})
	ck.Create("/a/d", "2", rpc.Flag{})
	children, _ := ck.GetChildren("/a", false)
	if ok, err := compareGetChildren("/a", []rpc.Ppath{"b", "c", "d"}, children); !ok {
		ts.t.Fatal(err)
	}
	// Test Delete and Exists
	ck.Delete("/a/b", 2)
	exists, _ = ck.Exists("/a/b", false)
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
			for time.Since(start) < time.Second {
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
	exists, _ := ck1.Exists(path, false)
	if !exists {
		ts.t.Fatal("Znode is missing after creation\n")
	}
	ck1.EndSession()
	// Allow session end to propogate
	time.Sleep(time.Second * 1)
	ck := ts.MakeSession()
	exists, _ = ck.Exists(path, false)
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
			for time.Since(start) < time.Second {
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
	children, err := ck.GetChildren("/b", false)
	if err != rpc.OK || len(children) > 0 {
		ts.t.Fatal("/b should have no children after nodes crashed\n")
	}
	fname, err := ck.Create(path, "data", rpc.Flag{Sequential: true, Ephemeral: true})
	if err != rpc.OK || fname != rpc.Ppath(fmt.Sprintf("/b/seq-%d", c0+c1+c2)) {
		ts.t.Fatalf("Created %s when %d previous ephermeral znodes were created", fname, c0+c1+c2)
	}
}
