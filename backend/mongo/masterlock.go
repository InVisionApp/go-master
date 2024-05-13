package mongo

import (
	"context"
	"fmt"
	"time"

	mgo "github.com/qiniu/qmgo"
	"go.mongodb.org/mongo-driver/bson"

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
func (m *MongoBackend) Lock(ctx context.Context, info *backend.MasterInfo) error {
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

	err := m.lock.Collection().Find(ctx, bson.M{"_id": MasterInfoID}).One(oldMMI)
	// an error has occurred and it is not a NotFound
	if err != nil {
		if err == mgo.ErrNoSuchDocuments {
			// perform an insert
			if _, err := m.lock.Collection().InsertOne(ctx, mmi); err != nil {
				return fmt.Errorf("unable to insert initial lock: %v", err)
			}

			return nil
		}

		err = fmt.Errorf("failed to fetch current master info: %v", err)

		return err
	}

	// has not been long enough since the last valid heart beat
	if time.Since(oldMMI.LastHeartbeat) < m.heartBeatFreq*2 && oldMMI.MasterID != info.MasterID {
		return fmt.Errorf("valid lock already in place")
	}

	query := bson.M{"master_id": oldMMI.MasterID, "last_heartbeat": oldMMI.LastHeartbeat}

	change := mgo.Change{
		Update:    bson.M{"$set": mmi},
		ReturnNew: true,
	}

	if err := m.lock.Collection().Find(ctx, query).Apply(change, mmi); err != nil {
		err = fmt.Errorf("unable to complete findModify: %v", err)
		return err
	}

	return nil

}

// Release the lock to relinquish the master role. This will not succeed if the
// provided masterID does not match the ID of the current master.
func (m *MongoBackend) UnLock(ctx context.Context, masterID string) error {
	query := bson.M{"master_id": masterID}

	if err := m.lock.Collection().Remove(ctx, query); err != nil {
		if err == mgo.ErrNoSuchDocuments { // not found is ok, already gone
			return nil
		}

		return err
	}

	return nil
}

// Write a heartbeat to ensure that the master role is not lost.
// If successful, the last heartbeat time is written to the passed MasterInfo
func (m *MongoBackend) WriteHeartbeat(ctx context.Context, info *backend.MasterInfo) error {
	query := bson.M{"master_id": info.MasterID}

	lastHeartbeat := time.Now()

	change := bson.M{
		"$set": bson.M{
			"last_heartbeat": lastHeartbeat,
		},
	}

	if err := m.lock.coll.UpdateOne(ctx, query, change); err != nil {
		return fmt.Errorf("Unable to complete heartbeat update: %v", err)
	}

	info.LastHeartbeat = lastHeartbeat

	return nil
}

// Get the current master status. Provides the MasterInfo of the current master.
func (m *MongoBackend) Status(ctx context.Context) (*backend.MasterInfo, error) {
	mi := &backend.MasterInfo{}
	if err := m.lock.Collection().Find(ctx, bson.M{"_id": MasterInfoID}).One(mi); err != nil {
		return nil, fmt.Errorf("failed to fetch master info: %v", err)
	}

	return mi, nil
}
