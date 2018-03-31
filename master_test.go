package master

import (
	"time"

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
	)

	BeforeEach(func() {
		fakeMasterLock = &fakebackend.FakeMasterLock{}

		m = &master{
			heartBeatFreq: time.Millisecond,
			lock:          fakeMasterLock,

			uuid:     "test-uuid",
			isMaster: safe.NewBool(),

			heartBeat: director.NewFreeLooper(1, nil),
		}
	})

	Context("start", func() {

	})

})
