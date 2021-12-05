package goll

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type SyncAdapter interface {
	Lock(ctx context.Context, tenantKey string) error
	Fetch(ctx context.Context, tenantKey string) (string, error)
	Write(ctx context.Context, tenantKey string, serializedData string) error
	Unlock(ctx context.Context, tenantKey string) error
}

type syncTxOptions struct {
	TenantKey  string
	TenantData *loadLimiterDefaultImplTenantData
	ReadOnly   bool
}

// TODO return info on sync transaction to caller
// to propagate non-blocking errors
/*
type syncTxResult struct {
	...
}
*/
func (instance *loadLimiterDefaultImpl) withSyncTransaction(task func(), txOptions syncTxOptions) error {
	if instance.SyncAdapter == nil {
		task()
		return nil
	}

	if txOptions.TenantData == nil {
		if txOptions.TenantKey == "" {
			return errors.New("no tenant data available for sync transaction")
		}
		txOptions.TenantData = instance.getTenant(txOptions.TenantKey)
	}

	tenantKey := txOptions.TenantKey
	tenant := txOptions.TenantData
	l := instance.Logger
	adapter := instance.SyncAdapter

	adapterContext := context.Background()

	logPrefix := fmt.Sprintf("[sync tx %s] ", tenantKey)

	l.Info(logPrefix + "acquiring lock")

	err := adapter.Lock(adapterContext, tenantKey)

	if err != nil {
		return fmt.Errorf("error acquiring lock: %v", err.Error())
	}
	l.Info(logPrefix + "lock acquired")

	defer func() {
		l.Info(logPrefix + "releasing lock")
		rerr := adapter.Unlock(adapterContext, tenantKey)
		if rerr != nil {
			l.Info(fmt.Sprintf(logPrefix+"could not release lock: %v", rerr.Error()))
		} else {
			l.Info(logPrefix + "lock released")
		}
	}()

	l.Info(logPrefix + "fetching status")
	status, err := adapter.Fetch(adapterContext, tenantKey)
	if err != nil {
		// TODO how do we handle this?
		// should block the whole flow?
		l.Error(fmt.Sprintf("could not fetch status: %v", err.Error()))
	} else {
		l.Info(logPrefix + "fetched status")

		if status == "" {
			l.Warning(logPrefix + "no status on remote store, skipping status check")
		} else {
			err = instance.restoreSerializedStatus(status, tenant)
			if err != nil {
				instance.Logger.Error(fmt.Sprintf("error restoring status from remote store: %s", err.Error()))
				// TODO how do we handle this?
				// should not block the whole flow.
			}
		}
	}

	versionBefore := tenant.Version

	l.Info(logPrefix + "executing task")
	task()

	changed := tenant.Version > versionBefore
	if txOptions.ReadOnly {
		if changed {
			l.Warning("sync transaction should have been readonly but changed version. skipping status write but something's off here")
		}
	} else if changed {
		l.Info(fmt.Sprintf(logPrefix + "writing updated status to remote store"))
		status = instance.serializeStatus(tenantKey, tenant)

		err = adapter.Write(adapterContext, tenantKey, status)
		if err != nil {
			instance.Logger.Error(fmt.Sprintf("could not write status: %v", err.Error()))
			// TODO how do we handle this?
			// should not block the whole flow.
		}
	} else {
		l.Info(logPrefix + "task did not change status, skipping writeback")
	}

	l.Info(logPrefix + "end")
	return nil
}

func (instance *compositeLoadLimiterDefaultImpl) withSyncTransaction(task func(), txOptions syncTxOptions) error {
	if instance.SyncAdapter == nil {
		task()
		return nil
	}
	if txOptions.TenantKey == "" {
		return errors.New("no tenant data available for sync transaction")
	}

	tenantKey := txOptions.TenantKey

	l := instance.Logger
	adapter := instance.SyncAdapter

	adapterContext := context.Background()

	logPrefix := fmt.Sprintf("[sync tx %s] ", tenantKey)

	l.Info(logPrefix + "acquiring lock")

	err := adapter.Lock(adapterContext, tenantKey)

	if err != nil {
		return fmt.Errorf("error acquiring lock: %v", err.Error())
	}
	l.Info(logPrefix + "lock acquired")

	defer func() {
		l.Info(logPrefix + "releasing lock")
		rerr := adapter.Unlock(adapterContext, tenantKey)
		if rerr != nil {
			l.Info(fmt.Sprintf(logPrefix+"could not release lock: %v", rerr.Error()))
		} else {
			l.Info(logPrefix + "lock released")
		}
	}()

	numLimiters := len(instance.Limiters)

	l.Info(logPrefix + "fetching status")
	status, err := adapter.Fetch(adapterContext, tenantKey)
	if err != nil {
		// TODO how do we handle this?
		// should block the whole flow?
		l.Error(fmt.Sprintf("could not fetch status: %v", err.Error()))
	} else {
		l.Info(logPrefix + "fetched status")

		if status == "" {
			l.Warning(logPrefix + "no status on remote store, skipping status check")
		} else {
			statusSplit := strings.Split(status, ";")
			if len(statusSplit) != len(instance.Limiters) {
				instance.Logger.Error("error restoring status from remote store: invalid number of sublimiters")
				// TODO how do we handle this?
				// should not block the whole flow.
			} else {
				for i, limiter := range instance.Limiters {
					tenant := limiter.getTenant(tenantKey)
					err = limiter.restoreSerializedStatus(statusSplit[i], tenant)
					if err != nil {
						instance.Logger.Error(fmt.Sprintf("error restoring status from remote store: %s", err.Error()))
						// TODO how do we handle this?
						// should not block the whole flow.
					}
				}
			}
		}
	}

	// versionBefore := instance.Version
	versionsBefore := make([]uint64, numLimiters)
	for i, limiter := range instance.Limiters {
		versionsBefore[i] = limiter.getTenant(tenantKey).Version
	}

	l.Info(logPrefix + "executing task")
	task()

	changed := false
	for i, limiter := range instance.Limiters {
		if limiter.getTenant(tenantKey).Version != versionsBefore[i] {
			changed = true
			break
		}
	}

	if txOptions.ReadOnly {
		if changed {
			l.Warning("sync transaction should have been readonly but changed version. skipping status write but something's off here")
		}
	} else if changed {
		l.Info(fmt.Sprintf(logPrefix + "writing updated status to remote store"))
		// status = instance.serializeStatus()
		limitersStatus := ""
		for _, limiter := range instance.Limiters {
			tenant := limiter.getTenant(tenantKey)
			limitersStatus += (limiter.serializeStatus(tenantKey, tenant) + ";")
		}
		if numLimiters > 0 {
			limitersStatus = strings.TrimRight(limitersStatus, ";")
		}

		err = adapter.Write(adapterContext, tenantKey, limitersStatus)
		if err != nil {
			instance.Logger.Error(fmt.Sprintf("could not write status: %v", err.Error()))
			// TODO how do we handle this?
			// should not block the whole flow.
		}
	} else {
		l.Info(logPrefix + "task did not change status, skipping writeback")
	}

	l.Info(logPrefix + "end")
	return nil
}

func (instance *loadLimiterDefaultImpl) serializeStatus(tenantKey string, tenant *loadLimiterDefaultImplTenantData) string {
	out := fmt.Sprintf("v1/%d/%d/", tenant.Version, tenant.WindowTotal)
	if tenant.WasOver {
		out += "1"
	} else {
		out += "0"
	}
	out += "/"
	qLen := tenant.WindowQueue.Len()

	segstr := ""
	for i := 0; i < qLen; i++ {
		seg := tenant.WindowQueue.At(i).(*windowSegment)
		segstr += fmt.Sprintf("%d:%d,", seg.StartTime, seg.Value)
	}
	if qLen > 0 {
		segstr = strings.TrimRight(segstr, ",")
	}
	out += segstr

	return out
}

func (instance *loadLimiterDefaultImpl) restoreSerializedStatus(serialized string, tenant *loadLimiterDefaultImplTenantData) error {
	splitted := strings.Split(serialized, "/")
	tokenLen := len(splitted)
	if tokenLen < 1 {
		return errors.New("not enough tokens")
	}
	serializationVersion := splitted[0]
	if serializationVersion != "v1" {
		return fmt.Errorf("invalid serialization version %v", serializationVersion)
	}

	if tokenLen != 5 {
		return errors.New("invalid number of tokens for v1 format")
	}

	version := splitted[1]
	versionRaw, err := strconv.Atoi(version)
	if err != nil {
		return fmt.Errorf("could not parse version: %w", err)
	}
	remoteVersion := uint64(versionRaw)

	if tenant.Version == remoteVersion {
		instance.Logger.Debug("instance version is up to date with serialized data, nothing to do")
		return nil
	} else if remoteVersion < tenant.Version {
		// something bad happened
		return fmt.Errorf("serialized instance version %d is older than current version %d", remoteVersion, tenant.Version)
	}

	instance.Logger.Debug("instance version is not up to date with serialized data, hydrating state")

	windowTotalRaw, err := strconv.Atoi(splitted[2])
	if err != nil {
		return fmt.Errorf("could not parse windowTotal: %w", err)
	}

	wasOver := false
	if splitted[3] == "1" {
		wasOver = true
	}

	// apply queue
	splittedSegments := strings.Split(splitted[4], ",")
	q := tenant.WindowQueue
	rLen := len(splittedSegments)

	q.Clear()

	// iterate on the segments and make sure the data matches
	for i := 0; i < rLen; i++ {
		splittedSegment := strings.Split(splittedSegments[rLen-i-1], ":")
		if len(splittedSegment) != 2 {
			return fmt.Errorf("invalid format for segment #%d", i)
		}

		remoteStartTime, err := strconv.Atoi(splittedSegment[0])
		if err != nil {
			return fmt.Errorf("could not parse start time for segment %d: %w", i, err)
		}
		remoteValue, err := strconv.Atoi(splittedSegment[1])
		if err != nil {
			return fmt.Errorf("could not parse value for segment %d: %w", i, err)
		}

		q.PushFront(&windowSegment{
			StartTime: uint64(remoteStartTime),
			Value:     uint64(remoteValue),
		})
	}

	tenant.WindowTotal = uint64(windowTotalRaw)
	tenant.WasOver = wasOver
	tenant.Version = remoteVersion

	return nil
}
