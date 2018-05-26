package master

import (
	"errors"
	"time"

	"github.com/InVisionApp/go-logger"
	"github.com/InVisionApp/go-logger/shims/testlog"
	"github.com/InVisionApp/go-master/backend"
	"github.com/InVisionApp/go-master/fakes/fakebackend"
	"github.com/InVisionApp/go-master/safe"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/relistan/go-director"
)

var _ = Describe("New", func() {
	var (
		m              *master
		mCfg           *MasterConfig
		fakeMasterLock *fakebackend.FakeMasterLock
	)

	BeforeEach(func() {
		fakeMasterLock = &fakebackend.FakeMasterLock{}

		mCfg = &MasterConfig{
			HeartBeatFrequency: time.Millisecond,
			MasterLock:         fakeMasterLock,
			Version:            "testver",
		}
	})

	JustBeforeEach(func() {
		mst := New(mCfg)
		m, _ = mst.(*master)
	})

	Context("meets the interface", func() {
		var _ Master = m
	})

	It("populates values", func() {
		Expect(m.version).ToNot(BeEmpty())
		Expect(m.heartBeatFreq).ToNot(BeNil())
		Expect(m.lock).ToNot(BeNil())
	})

	It("sets up the heartbeat", func() {
		Expect(m.heartBeat).ToNot(BeNil())
	})

	It("sets logrus logger if nil", func() {
		Expect(m.log).ToNot(BeNil())
	})

	It("generates a UUID", func() {
		Expect(m.uuid).ToNot(BeEmpty())
	})

	It("nil hooks if not provided", func() {
		Expect(m.startHook).To(BeNil())
		Expect(m.stopHook).To(BeNil())
	})

	Context("hooks provided", func() {
		var startfunc func()
		var stopfunc func()

		BeforeEach(func() {
			startfunc = func() {}
			stopfunc = func() {}
			mCfg.StartHook = startfunc
			mCfg.StopHook = stopfunc
		})

		It("sets up hooks if provided", func() {
			Expect(m.startHook).ToNot(BeNil())
			//Expect(m.startHook).To(Equal(startfunc))

			Expect(m.stopHook).ToNot(BeNil())
			//Expect(m.stopHook).To(BeEquivalentTo(stopfunc))
		})
	})
})

var _ = Describe("master", func() {
	var (
		m              *master
		fakeMasterLock *fakebackend.FakeMasterLock
		testUUID       = "test-uuid"
		testVersion    = "test-version"
	)

	BeforeEach(func() {
		fakeMasterLock = &fakebackend.FakeMasterLock{}

		m = &master{
			heartBeatFreq: time.Millisecond,
			lock:          fakeMasterLock,

			uuid:     testUUID,
			version:  testVersion,
			isMaster: safe.NewBool(),
			info:     &backend.MasterInfo{},

			heartBeat: director.NewFreeLooper(1, nil),

			log: log.NewNoop(),
		}
	})

	Describe("start", func() {
		It("kicks off the heartbeat", func() {
			err := m.Start()
			Expect(err).ToNot(HaveOccurred())

			time.Sleep(time.Millisecond)

			Expect(fakeMasterLock.LockCallCount()).To(Equal(1))
		})

		Context("already the master", func() {
			BeforeEach(func() {
				m.isMaster.SetTrue()
			})

			It("errors with already master", func() {
				err := m.Start()

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("already master"))
			})
		})

		Describe("runHeartBeat", func() {
			Context("not the master currently", func() {
				BeforeEach(func() {
					m.isMaster.SetFalse()
				})

				It("tries to become the master", func() {
					m.runHeartBeat()

					Expect(fakeMasterLock.LockCallCount()).To(Equal(1))
				})

				It("sets info if becomes master", func() {
					fakeMasterLock.LockReturns(nil)

					m.runHeartBeat()

					Expect(m.isMaster.Val()).To(BeTrue())
					Expect(m.info.MasterID).To(Equal(testUUID))
					Expect(m.info.Version).To(Equal(testVersion))
				})

				It("triggers start hook if becomes master", func() {
					fakeMasterLock.LockReturns(nil)
					testhook := &testHooks{}
					m.startHook = testhook.startHook

					m.runHeartBeat()

					time.Sleep(time.Millisecond)

					Expect(testhook.startCalls).To(Equal(1))
				})

				It("skips start hook if nil", func() {
					m.startHook = nil

					m.runHeartBeat()

					Expect(m.isMaster.Val()).To(BeTrue())
				})

				It("sets nothing if fails to become master", func() {
					fakeMasterLock.LockReturns(errors.New("failed"))

					m.runHeartBeat()

					Expect(m.isMaster.Val()).To(BeFalse())
					Expect(m.info.MasterID).To(BeEmpty())
					Expect(m.info.Version).To(BeEmpty())
				})
			})

			Context("already the master", func() {
				BeforeEach(func() {
					m.isMaster.SetTrue()
					m.info = &backend.MasterInfo{
						MasterID: m.uuid,
						Version:  m.version,
					}
				})

				It("writes a heartbeat", func() {
					m.runHeartBeat()

					Expect(fakeMasterLock.WriteHeartbeatCallCount()).To(Equal(1))
				})

				Context("heartbeat fails", func() {
					BeforeEach(func() {
						fakeMasterLock.WriteHeartbeatReturns(errors.New("failed"))
					})

					It("sends an error down the errChan", func() {
						ec := make(chan error)
						m.errors = ec

						m.runHeartBeat()

						select {
						case e := <-ec:
							Expect(e.Error()).To(ContainSubstring("failed"))
						}
					})

					It("cleans up the master", func() {
						m.runHeartBeat()

						Expect(m.isMaster.Val()).To(BeFalse())
						Expect(m.info.MasterID).To(BeEmpty())
						Expect(m.info.Version).To(BeEmpty())
					})

					It("calls on the stop hook", func() {
						testhook := &testHooks{}
						m.stopHook = testhook.stopHook

						m.runHeartBeat()

						time.Sleep(time.Millisecond)

						Expect(testhook.stopCalls).To(Equal(1))
					})

					It("skips the stop hook if nil", func() {
						m.stopHook = nil

						m.runHeartBeat()

						Expect(m.isMaster.Val()).To(BeFalse())
					})
				})
			})
		})

	})

	Describe("stop", func() {
		It("does not error if not the master", func() {
			tlog := testlog.New()
			m.log = tlog

			m.isMaster.SetFalse()

			err := m.Stop()

			Expect(err).ToNot(HaveOccurred())
			Expect(tlog.Bytes()).To(ContainSubstring("not currently the master"))
		})

		Context("is the master", func() {
			BeforeEach(func() {
				m.isMaster.SetTrue()
			})

			It("stops without error", func() {
				err := m.Stop()

				Expect(err).ToNot(HaveOccurred())
			})

			It("resets the values", func() {
				err := m.Stop()

				Expect(err).ToNot(HaveOccurred())
				Expect(m.isMaster.Val()).To(BeFalse())
				Expect(m.info.MasterID).To(BeEmpty())
				Expect(m.info.Version).To(BeEmpty())
			})

			// TODO: Looks like there is a leak actually
			//It("does not have a goroutine leak", func() {
			//	beginNum := runtime.NumGoroutine()
			//	err := m.Stop()
			//	endNum := runtime.NumGoroutine()
			//
			//	Expect(err).ToNot(HaveOccurred())
			//
			//	Expect(beginNum).To(Equal(endNum))
			//})

			Context("backend unlock fails", func() {
				BeforeEach(func() {
					fakeMasterLock.UnLockReturns(errors.New("failed"))
				})

				It("does not error", func() {
					err := m.Stop()

					Expect(err).ToNot(HaveOccurred())
				})

				It("logs a message", func() {
					tlog := testlog.New()
					m.log = tlog

					m.Stop()

					Expect(tlog.Bytes()).To(ContainSubstring("failed to release lock"))
				})
			})

			It("does not call stop hook", func() {
				testhook := &testHooks{}
				m.stopHook = testhook.stopHook

				m.Stop()

				Expect(testhook.stopCalls).To(Equal(0))
			})
		})
	})

	Describe("IsMaster", func() {
		It("returns true if master", func() {
			m.isMaster.SetTrue()

			Expect(m.IsMaster()).To(BeTrue())
		})

		It("returns false if not master", func() {
			m.isMaster.SetFalse()

			Expect(m.IsMaster()).To(BeFalse())
		})
	})

	Describe("Status", func() {
		It("returns true if master", func() {
			m.isMaster.SetTrue()

			st, err := m.Status()
			Expect(err).ToNot(HaveOccurred())

			stat, ok := st.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(stat["is_master"]).To(BeTrue())
		})

		It("returns false if not master", func() {
			m.isMaster.SetFalse()

			st, err := m.Status()
			Expect(err).ToNot(HaveOccurred())

			stat, ok := st.(map[string]interface{})
			Expect(ok).To(BeTrue())
			Expect(stat["is_master"]).To(BeFalse())
		})
	})
})

type testHooks struct {
	startCalls int
	stopCalls  int
}

func (t *testHooks) startHook() {
	t.startCalls++
}

func (t *testHooks) stopHook() {
	t.stopCalls++
}
