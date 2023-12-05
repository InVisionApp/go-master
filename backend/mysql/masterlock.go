package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/InVisionApp/go-master/backend"
)

/************************
MasterLock Implementation
************************/

const (
	MasterLockID        = 0
	HeartbeatMultiplier = 3
)

type MySQLMasterInfo struct {
	ID            int       `db:"id,omitempty"`
	MasterID      string    `db:"master_id"`
	Version       string    `db:"version"`
	StartedAt     time.Time `db:"started_at"`
	LastHeartbeat time.Time `db:"last_heartbeat"`
}

func (m *MySQLMasterInfo) toMasterInfo() *backend.MasterInfo {
	return &backend.MasterInfo{
		MasterID:      m.MasterID,
		Version:       m.Version,
		StartedAt:     m.StartedAt,
		LastHeartbeat: m.LastHeartbeat,
	}
}

func (m *MySQLBackend) Lock(ctx context.Context, info *backend.MasterInfo) error {
	curInfo, ok, err := m.getMasterInfo()
	if err != nil {
		return err
	}

	// found and the current master lock has not expired
	if ok && time.Since(curInfo.LastHeartbeat) < m.heartbeatFreq*HeartbeatMultiplier {
		return fmt.Errorf("found active master %s", curInfo.MasterID)
	}

	now := time.Now()
	mysqlInfo := &MySQLMasterInfo{
		ID:            MasterLockID,
		MasterID:      info.MasterID,
		Version:       info.Version,
		StartedAt:     now,
		LastHeartbeat: now,
	}

	// no master lock record present
	if !ok {
		// if another node gets ahead of this, it will fail on the unique constraint
		if err := m.insertMasterInfo(mysqlInfo); err != nil {
			return err
		}

		// inserted new record
		return nil
	}

	// a master lock record is present but expired
	query := fmt.Sprintf(
		`UPDATE %s SET
			master_id = :new_master_id,
			version = :new_version,
			started_at = :new_started_at,
			last_heartbeat = :new_last_heartbeat
		WHERE
			id = :id AND
			master_id = :master_id AND
			version = :version AND
			started_at = :started_at AND
			last_heartbeat = :last_heartbeat`,
		m.TableName)

	fields := map[string]interface{}{
		// new fields
		"new_master_id":      mysqlInfo.MasterID,
		"new_version":        mysqlInfo.Version,
		"new_started_at":     mysqlInfo.StartedAt,
		"new_last_heartbeat": mysqlInfo.LastHeartbeat,
		// prev fields
		"id":             MasterLockID,
		"master_id":      curInfo.MasterID,
		"version":        curInfo.Version,
		"started_at":     curInfo.StartedAt,
		"last_heartbeat": curInfo.LastHeartbeat,
	}

	res, err := m.db.NamedExec(query, fields)
	if err != nil {
		return fmt.Errorf("failed to update lock: %v", err)
	}

	// if no rows were modified, then probably another master got the lock
	if rows, err := res.RowsAffected(); err != nil || rows == 0 {
		return errors.New("did not achieve a master lock")
	}

	return nil
}

func (m *MySQLBackend) getMasterInfo() (*MySQLMasterInfo, bool, error) {
	query := fmt.Sprintf("SELECT * FROM %s WHERE id = %d", m.TableName, MasterLockID)

	minfo := &MySQLMasterInfo{}

	err := m.db.Get(minfo, query)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("failed to fetch current master state: %v", err)
	}

	return minfo, true, nil
}

func (m *MySQLBackend) insertMasterInfo(info *MySQLMasterInfo) error {
	query := fmt.Sprintf(`
		INSERT INTO %s (id, master_id, version, started_at, last_heartbeat)
		VALUES (:id, :master_id, :version, :started_at, :last_heartbeat)`,
		m.TableName)

	if _, err := m.db.NamedExec(query, info); err != nil {
		return fmt.Errorf("failed to obtain a master lock: %v", err)
	}

	return nil
}

func (m *MySQLBackend) UnLock(ctx context.Context, masterID string) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE id = ? AND master_id = ?`, m.TableName)

	if _, err := m.db.Exec(query, MasterLockID, masterID); err != nil {
		return fmt.Errorf("failed to release master lock: %v", err)
	}

	// if no rows were affected, that's still ok because the record was not there

	return nil
}

func (m *MySQLBackend) WriteHeartbeat(ctx context.Context, info *backend.MasterInfo) error {
	query := fmt.Sprintf(`
		UPDATE %s SET
			last_heartbeat = :last_heartbeat
		WHERE
			master_id = :master_id`,
		m.TableName)

	mi := &MySQLMasterInfo{
		MasterID:      info.MasterID,
		LastHeartbeat: time.Now(),
	}

	res, err := m.db.NamedExec(query, mi)
	if err != nil {
		return err
	}

	if rows, err := res.RowsAffected(); err != nil || rows == 0 {
		return errors.New("no rows updated")
	}

	info.LastHeartbeat = mi.LastHeartbeat

	return nil
}

func (m *MySQLBackend) Status(ctx context.Context) (*backend.MasterInfo, error) {
	info, ok, err := m.getMasterInfo()
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, errors.New("no master currently")
	}

	return info.toMasterInfo(), nil
}
