package pan

import (
	"testing"

	// "6.5840/kvraft1/rsm"
	// "6.5840/kvsrv1/rpc"
	// "6.5840/panapi"
	// tester "6.5840/tester1"
)

func TestCheck(t *testing.T) {
	ts := MakeTest(t, "name", 1, 3, false, false, false, false, -1, false)
	defer ts.Cleanup()
	ck := ts.MakeClerk()
	for range 10 {
		ck.Create("a/b", "hello", 0)
	}
	if !ck.Exists("a/b", false) {
		ts.t.Fatalf("bruh")
	}
}
