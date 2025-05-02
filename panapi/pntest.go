package panapi

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"pan/panapi/rpc"

	"6.5840/tester1"
)

// n specifies the length of the string to be generated.
func RandValue(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

type IPNSession interface {
	Create(path rpc.Ppath, data string, flags rpc.Flag) (rpc.Ppath, rpc.Err)

	Delete(path rpc.Ppath, version rpc.Pversion) rpc.Err

	// Watches block (for now........)
	Exists(path rpc.Ppath, watch bool) (bool, rpc.Err)

	GetData(path rpc.Ppath, watch bool) (string, rpc.Pversion, rpc.Err)

	SetData(path rpc.Ppath, data string, version rpc.Pversion) rpc.Err

	GetChildren(path rpc.Ppath, watch bool) ([]rpc.Ppath, rpc.Err)

	Sync(path rpc.Ppath) rpc.Err

	// Ends the current client session
	EndSession()
}

type TestSession struct {
	IPNSession
	Clnt *tester.Clnt
}

type ISessionMaker interface {
	MakeSession() IPNSession
	DeleteSession(IPNSession)
}

type Test struct {
	*tester.Config
	t           *testing.T
	mck         ISessionMaker
	randomfiles bool
}

func MakeTest(t *testing.T, cfg *tester.Config, randomfiles bool, mck ISessionMaker) *Test {
	ts := &Test{
		Config:      cfg,
		t:           t,
		mck:         mck,
		randomfiles: randomfiles,
	}
	return ts
}

func (ts *Test) Cleanup() {
	ts.Config.End()
	ts.Config.Cleanup()
}

func (ts *Test) ConnectClnts(clnts []*tester.Clnt) {
	for _, c := range clnts {
		c.ConnectAll()
	}
}

func (ts *Test) MakeSession() IPNSession {
	return ts.mck.MakeSession()
}

func MakeFiles(n int) []rpc.Ppath {
	files := make([]rpc.Ppath, n)
	for i := 0; i < n; i++ {
		files[i] = rpc.Ppath("/a/f" + strconv.Itoa(i))
	}
	return files
}

// repartition the servers periodically
func (ts *Test) Partitioner(gid tester.Tgid, ch chan bool) {
	defer func() { ch <- true }()
	for true {
		select {
		case <-ch:
			return
		default:
			a := make([]int, ts.Group(gid).N())
			for i := 0; i < ts.Group(gid).N(); i++ {
				a[i] = (rand.Int() % 2)
			}
			pa := make([][]int, 2)
			for i := 0; i < 2; i++ {
				pa[i] = make([]int, 0)
				for j := 0; j < ts.Group(gid).N(); j++ {
					if a[j] == i {
						pa[i] = append(pa[i], j)
					}
				}
			}
			ts.Group(gid).Partition(pa[0], pa[1])
			tester.AnnotateTwoPartitions(pa[0], pa[1])
			time.Sleep(1*time.Second + time.Duration(rand.Int63()%200)*time.Millisecond)
		}
	}
}

type TestType struct {
	Remaining map[rpc.Ppath]bool
	Expected  int
}

func (ts *Test) CheckRes(ck IPNSession, res TestType, file rpc.Ppath) {
	children, _ := ck.GetChildren(file, false)
	if len(children) != len(res.Remaining) {
		ts.t.Fatalf("Number of children: %d, number of remaining ephemeral znodes: %d\n", len(children), len(res.Remaining))
	}
	for _, path := range children {
		if _, ok := res.Remaining[file + "/" + path]; !ok {
			ts.t.Fatalf("%s is a child but should have been deleted by client crash\n", path)
		}
	}
	actual, _ := ck.Create(file + "/f-", "", rpc.Flag{Ephemeral: false, Sequential: true})
	expected := file + "/f-" + rpc.Ppath(strconv.Itoa(res.Expected))
	if actual != expected {
		ts.t.Fatalf("Created znode %s but expected znode %s instead\n", actual, expected)
	}
}

func (ts *Test) StartSessionsAndWait(nclnt int, t time.Duration, file rpc.Ppath, clientCrashes bool, ch_err chan string) TestType {
	const (
		PERITER = 10
		ITERS   = 5
	)
	ch_path := make([]chan []rpc.Ppath, nclnt)
	for i := range nclnt {
		ch_path[i] = make(chan []rpc.Ppath)
		go func() {
			session := ts.MakeSession()
			paths := make([]rpc.Ppath, 0)
			for range ITERS {
				iter_paths := make([]rpc.Ppath, 0)
				for range PERITER {
					fname, _ := session.Create(file, "", rpc.Flag{Sequential: true, Ephemeral: true})
					iter_paths = append(iter_paths, fname)
				}
				if clientCrashes && (rand.Int()%100) < 30 {
					ts.Crash(session)
				} else {
					paths = append(paths, iter_paths...)
				}
				session = ts.MakeSession()
			}
			ch_path[i] <- paths
		}()
	}
	remaining := make(map[rpc.Ppath]bool)
	for i := range nclnt {
		paths := <-ch_path[i]
		for _, path := range paths {
			if _, ok := remaining[path]; ok {
				err := fmt.Sprintf("Path %s created by two clients", path)
				ch_err <- err
				ts.t.FailNow()
			}
			remaining[path] = true
		}
	}
	return TestType{Remaining: remaining, Expected: PERITER * ITERS * nclnt}
}

func (ts *Test) Crash(session IPNSession) {
	testSession := session.(*TestSession)
	testSession.Clnt.DisconnectAll()
}
