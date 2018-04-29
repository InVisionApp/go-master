package mongo

import (
	"time"
	"fmt"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"github.com/InVisionApp/go-master/backend"
)

/************************
MasterLock Implementation
************************/

const MasterInfoID = "there-is-only-one-of-these"

type MongoMasterInfo struct {
	ID            string    `bson:"_id"`
	MasterID      string    `bson:"master_id"`
	Version       string    `bson:"version"`
	StartedAt     time.Time `bson:"started_at"`
	LastHeartbeat time.Time `bson:"last_heartbeat"`
}

func (m *MongoMasterInfo) toMasterInfo() *backend.MasterInfo {
	return &backend.MasterInfo{
		MasterID:      m.MasterID,
		Version:       m.Version,
		StartedAt:     m.StartedAt,
		LastHeartbeat: m.LastHeartbeat,
	}
}

// Achieve a lock to become the master. If lock is successful, the provided
// MasterInfo will be filled out and recorded. The MasterInfo passed in will be filled
// out with the remaining details.
func (m *MongoBackend) Lock(info *backend.MasterInfo) error {
	// get the heartbeat first to see if there is one before inserting
	oldMMI := &MongoMasterInfo{}
	err := m.lock.Collection().FindId(MasterInfoID).One(oldMMI)
	// an error has occurred and it is not a NotFound
	if err != nil && err != mgo.ErrNotFound {
		e := fmt.Errorf("failed to fetch current master info: %v", err)
		m.log.Error(e)
		return e
	}

	// has not been long enough since the last valid heart beat
	if time.Since(oldMMI.LastHeartbeat) < m.heartBeatFreq*2 {
		return fmt.Errorf("valid lock already in place")
	}

	t := time.Now()
	mmi := &MongoMasterInfo{
		ID:            MasterInfoID, // this is always the same. creates the lock
		MasterID:      info.MasterID,
		Version:       info.Version,
		StartedAt:     t,
		LastHeartbeat: t,
	}

	if err := m.lock.Collection().Insert(mmi); err != nil {
		return err
	}

	return nil
}

// Release the lock to relinquish the master role. This will not succeed if the
// provided masterID does not match the ID of the current master.
func (m *MongoBackend) UnLock(masterID string) error {
	query := bson.M{"master_id": masterID}

	if err := m.lock.Collection().Remove(query); err != nil {
		if err == mgo.ErrNotFound { // not found is ok, already gone
			return nil
		}

		return err
	}

	return nil
}

// Write a heartbeat to ensure that the master role is not lost.
// If successful, the last heartbeat time is written to the passed MasterInfo
func (m *MongoBackend) WriteHeartbeat(info *backend.MasterInfo) error {
	query := bson.M{"master_id": info.MasterID}

	change := mgo.Change{
		Update:    bson.M{"last_heartbeat": time.Now()},
		ReturnNew: true,
	}

	mmi := &MongoMasterInfo{}
	_, err := m.lock.coll.Find(query).Apply(change, mmi)
	if err != nil {
		if err == mgo.ErrNotFound { //not found is ok
			return nil
		}

		return err
	}

	info.LastHeartbeat = mmi.LastHeartbeat

	return nil
}

// Get the current master status. Provides the MasterInfo of the current master.
func (m *MongoBackend) Status() (*backend.MasterInfo, error) {
	mi := &backend.MasterInfo{}
	if err := m.lock.Collection().FindId(MasterInfoID).One(mi); err != nil {
		return nil, fmt.Errorf("failed to fetch master info: %v", err)
	}

	return mi, nil
}
