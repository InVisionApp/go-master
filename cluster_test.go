package master

import (
	"errors"
	"fmt"
	"time"

	"github.com/InVisionApp/go-logger"
	"github.com/InVisionApp/go-master/backend"
	"github.com/InVisionApp/go-master/fakes/fakebackend"
	"github.com/InVisionApp/go-master/safe"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/relistan/go-director"
)

var _ = Describe("scenarios", func() {
	var (
		m1       *master
		m2       *master
		fakeLock *fakebackend.FakeMasterLock
		testID1  = "test-uuid-1"
		testID2  = "test-uuid-2"
	)

	BeforeEach(func() {
		fakeLock = &fakebackend.FakeMasterLock{}

		m1 = newTestMaster(testID1, fakeLock)
		m2 = newTestMaster(testID2, fakeLock)
	})

	Context("2 nodes", func() {
		Context("start in order", func() {
			It("first becomes master", func() {
				fakeLock.LockReturnsOnCall(0, nil)
				fakeLock.LockReturnsOnCall(1, errors.New("already taken"))

				err1 := m1.Start()
				time.Sleep(time.Millisecond)
				err2 := m2.Start()

				Expect(err1).ToNot(HaveOccurred())
				Expect(err2).ToNot(HaveOccurred())

				Expect(m1.isMaster.Val()).To(BeTrue())
				Expect(m2.isMaster.Val()).To(BeFalse())
			})

			It("first fails second becomes master", func() {
				fakeLock.LockReturnsOnCall(0, errors.New("already taken"))
				fakeLock.LockReturnsOnCall(1, nil)

				err1 := m1.Start()
				time.Sleep(time.Millisecond)
				err2 := m2.Start()
				time.Sleep(time.Millisecond)

				Expect(err1).ToNot(HaveOccurred())
				Expect(err2).ToNot(HaveOccurred())

				Expect(m1.isMaster.Val()).To(BeFalse())
				Expect(m2.isMaster.Val()).To(BeTrue())
			})
		})

		Context("start together", func() {
			It("first becomes master", func() {
				fakeLock.LockStub = func(info *backend.MasterInfo) error {
					if info.MasterID == testID1 {
						return nil
					}

					return errors.New("wrong node")
				}

				err1 := m1.Start()
				err2 := m2.Start()
				time.Sleep(time.Millisecond)

				Expect(err1).ToNot(HaveOccurred())
				Expect(err2).ToNot(HaveOccurred())

				Expect(m1.isMaster.Val()).To(BeTrue())
				Expect(m2.isMaster.Val()).To(BeFalse())

				Expect(fakeLock.LockCallCount()).To(Equal(2))
			})

			It("first fails second becomes master", func() {
				fakeLock.LockStub = func(info *backend.MasterInfo) error {
					if info.MasterID == testID2 {
						return nil
					}

					return errors.New("wrong node")
				}

				err1 := m1.Start()
				err2 := m2.Start()
				time.Sleep(time.Millisecond)

				Expect(err1).ToNot(HaveOccurred())
				Expect(err2).ToNot(HaveOccurred())

				Expect(m1.isMaster.Val()).To(BeFalse())
				Expect(m2.isMaster.Val()).To(BeTrue())
			})
		})

		Context("failover", func() {
			var tl *testlock

			// first start become master
			// second start trial loop
			// first writes heartbeat
			// first fails heartbeat
			// second takes over
			// second writes heartbeat

			BeforeEach(func() {
				tl = &testlock{firstMaster: testID1, secondMaster: testID2}

				m1.heartBeat = director.NewImmediateTimedLooper(3, time.Millisecond, nil)
				m2.heartBeat = director.NewImmediateTimedLooper(3, time.Millisecond, nil)

				m1.lock = tl
				m2.lock = tl
			})

			It("does the things", func() {
				err1 := m1.Start()
				err2 := m2.Start()
				time.Sleep(time.Millisecond * 200)

				Expect(err1).ToNot(HaveOccurred())
				Expect(err2).ToNot(HaveOccurred())

				fmt.Println(tl.chronology)
			})
		})
	})
})

type testlock struct {
	isMaster   string
	lost       bool
	heartbeat  int
	chronology []string

	firstMaster  string
	secondMaster string
}

func (t *testlock) Lock(info *backend.MasterInfo) error {
	switch info.MasterID {
	case t.firstMaster:
		// if there is no master, first becomes master
		if t.isMaster == "" {
			t.isMaster = info.MasterID
			t.chronology = append(t.chronology, info.MasterID+" become master")
			return nil
		}

		// first is master and one heartbeat has been written
		if t.heartbeat == 1 {
			t.lost = true
			t.chronology = append(t.chronology, info.MasterID+" lost master")
			return errors.New("lost master")
		}

	case t.secondMaster:
		// if first was master but lock was lost, second becomes master
		if t.isMaster == t.firstMaster && t.lost {
			t.isMaster = info.MasterID
			t.lost = false
			t.chronology = append(t.chronology, info.MasterID+" become master")
		}
	}

	// all other calls fail
	return errors.New("failed to become master")
}

func (t *testlock) Status() (*backend.MasterInfo, error) {
	return &backend.MasterInfo{MasterID: t.isMaster}, nil
}

func (t *testlock) UnLock(masterID string) error {
	return nil
}

func (t *testlock) WriteHeartbeat(info *backend.MasterInfo) error {
	if info.MasterID != t.isMaster {
		return errors.New("wrong node")
	}

	if info.MasterID == t.firstMaster {
		switch t.heartbeat {
		case 0:
			t.heartbeat++
			return nil
		case 1:
			return errors.New("lock lost")
		}
	}

	if info.MasterID == t.secondMaster {
		return nil
	}

	return errors.New("nope")
}

func newTestMaster(id string, fakeLock backend.MasterLock) *master {
	return &master{
		heartBeatFreq: time.Millisecond,
		lock:          fakeLock,

		uuid:     id,
		version:  "abcd",
		isMaster: safe.NewBool(),
		info:     &backend.MasterInfo{},

		heartBeat: director.NewFreeLooper(1, nil),

		log: log.NewNoop(),
	}
}
