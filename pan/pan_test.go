package pan

import (
	"fmt"
	"pan/panapi/rpc"
	"reflect"
	"testing"
	// "6.5840/kvraft1/rsm"
	// "6.5840/kvsrv1/rpc"
	// "6.5840/panapi"
	// tester "6.5840/tester1"
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
	ck := ts.MakeClerk()
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
