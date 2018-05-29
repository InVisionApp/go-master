package mongo

import (
	"fmt"
	"time"

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
	t := time.Now()
	mmi := &MongoMasterInfo{
		ID:            MasterInfoID, // this is always the same. creates the lock
		MasterID:      info.MasterID,
		Version:       info.Version,
		StartedAt:     t,
		LastHeartbeat: t,
	}

	err := m.lock.Collection().FindId(MasterInfoID).One(oldMMI)
	// an error has occurred and it is not a NotFound
	if err != nil {
		if err == mgo.ErrNotFound {
			// perform an insert
			if err := m.lock.Collection().Insert(mmi); err != nil {
				m.log.Errorf("unable to insert initial lock: %v", err)
				return err
			}

			m.log.Info("successfully inserted initial lock")
			return nil
		}

		e := fmt.Errorf("failed to fetch current master info: %v", err)
		m.log.Error(e)

		m.log.Debug("attempting to refresh sessions in case of db issues")
		m.refresh()

		return e
	}

	// has not been long enough since the last valid heart beat
	if time.Since(oldMMI.LastHeartbeat) < m.heartBeatFreq*2 && oldMMI.MasterID != info.MasterID {
		return fmt.Errorf("valid lock already in place")
	}

	query := bson.M{"master_id": oldMMI.MasterID, "last_heartbeat": oldMMI.LastHeartbeat}

	change := mgo.Change{
		Update:    mmi,
		ReturnNew: true,
	}

	if _, err := m.lock.Collection().Find(query).Apply(change, mmi); err != nil {
		lErr := fmt.Errorf("unable to complete findModify: %v", err)
		m.log.Error(lErr)
		return lErr
	}

	return nil

}

// Force refresh all sessions (bypassing SmartCollection's auto-refresh)
// TODO: add auto-refreshing functionality across the board (GetNextJob, CompleteJob, etc.)
func (m *MongoBackend) refresh() {
	m.lock.Collection().Database.Session.Refresh()
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

	lastHeartbeat := time.Now()

	change := bson.M{
		"$set": bson.M{
			"last_heartbeat": lastHeartbeat,
		},
	}

	if err := m.lock.coll.Update(query, change); err != nil {
		return fmt.Errorf("Unable to complete heartbeat update: %v", err)
	}

	info.LastHeartbeat = lastHeartbeat

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
