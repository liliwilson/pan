package pan

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	// "6.5840/kvraft1/rsm"
	// "6.5840/kvsrv1/rpc"
	// "6.5840/panapi"
	// tester "6.5840/tester1"
	"pan/panapi"
	"pan/panapi/rpc"
)

func compareGetData(file string, expectedData string, expectedVersion panapi.Pversion, actualData string, actualVersion panapi.Pversion) (bool, string) {
	if expectedData != actualData || expectedVersion != actualVersion {
		return false, fmt.Sprintf("Got %s with data '%s' and version %d; expected data '%s' with version %d\n", file, actualData, actualVersion, expectedData, expectedVersion)
	}
	return true, ""
}

func compareGetChildren(dir string, expectedChildren []panapi.Ppath, actualChildren []panapi.Ppath) (bool, string) {
	if !reflect.DeepEqual(expectedChildren, actualChildren) {
		return false, fmt.Sprintf("Got %s with children %v; expected children %v\n", dir, actualChildren, expectedChildren)
	}
	return true, ""
}

func TestBasic(t *testing.T) {
	ts := MakeTest(t, "Synchronous One-client Basic", 1, 3, true, false, false, false, -1, false)
	defer ts.Cleanup()
	ck := ts.MakeClerk()
	// Test Create and Exists
	ck.Create("/a", "", panapi.Flag{})
	ck.Create("/a/b", "hello", panapi.Flag{})
	exists, _ := ck.Exists("/a/b", false)
	if !exists {
		ts.t.Fatalf("/a/b should exist\n")
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
	ck.Create("/a/c", "1", panapi.Flag{})
	ck.Create("/a/d", "2", panapi.Flag{})
	children, _ := ck.GetChildren("/a", false)
	if ok, err := compareGetChildren("/a", []panapi.Ppath{"b", "c", "d"}, children); !ok {
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
	ck := ts.MakeClerk()
	cks := make([]panapi.IPNClerk, nclients)
	chs := make([]chan int, nclients)
	for i := range 3 {
		cks[i] = ts.MakeClerk()
		go func(idx int) {
			start := time.Now()
			count := 0
			for time.Since(start) < time.Second {
				cks[i].Create("/a/seq-", "data", panapi.Flag{Sequential: true})
				count += 1
			}
			chs[i] <- count
		}(i)
	}
	c0 := <-chs[0]
	c1 := <-chs[1]
	c2 := <-chs[2]
	total := c0 + c1 + c2
	zname, err := ck.Create("/a/seq-", "data", panapi.Flag{Sequential: true})
	if err != rpc.OK || zname != panapi.Ppath(fmt.Sprintf("/a/seq-%d", total)) {
		ts.t.Fatalf("Created %s after %d previous sequential znode creations\n", zname, total)
	}
}
