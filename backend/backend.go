package backend

import (
	"context"
	"time"
)

//go:generate counterfeiter -o ../fakes/fakebackend/fake_masterlock.go . MasterLock

type MasterLock interface {
	// Achieve a lock to become the master. If lock is successful, the provided
	// MasterInfo will be filled out and recorded. The MasterInfo passed in will be filled
	// out with the remaining details.
	Lock(ctx context.Context, info *MasterInfo) error

	// Release the lock to relinquish the master role. This will not succeed if the
	// provided masterID does not match the ID of the current master.
	UnLock(ctx context.Context, masterID string) error

	// Write a heartbeat to ensure that the master role is not lost.
	// If successful, the last heartbeat time is written to the passed MasterInfo
	WriteHeartbeat(ctx context.Context, info *MasterInfo) error

	// Get the current master status. Provides the MasterInfo of the current master.
	Status(ctx context.Context) (*MasterInfo, error)
}

type MasterInfo struct {
	MasterID string
	Version  string
	//The time it became a master
	StartedAt time.Time
	//The last successful heartbeat
	LastHeartbeat time.Time
}
