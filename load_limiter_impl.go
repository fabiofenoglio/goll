package goll

import (
	"sync"
	"time"

	"github.com/gammazero/deque"
)

// loadLimiterDefaultImpl holds all the required
// runtime data together with the parsed configuration.
type loadLimiterDefaultImpl struct {
	Logger Logger
	Config *loadLimiterEffectiveConfig

	// Time functions can be overridden for testing.
	TimeFunc  func() time.Time
	SleepFunc func(d time.Duration)

	// a lock provides thread safety.
	Lock sync.Mutex

	// SyncAdapter is an implementation used to synchronize
	// the limiter data in a clustered environment.
	SyncAdapter SyncAdapter

	// we keep all runtime data for tenants
	// in a map indexed by tenant key
	TenantData map[string]*loadLimiterDefaultImplTenantData
}

type loadLimiterDefaultImplTenantData struct {
	// a deque implementation is used to represent the sliding window
	// as we need to operate on both sides of the window.
	WindowQueue *deque.Deque

	// WasOver signals that a rejection was sent with the last request
	WasOver bool

	// WindowTotal stores the total active load aggregated from the window.
	WindowTotal uint64

	// Versioning data for persistence and synchronization
	Version uint64
}

// loadLimiterEffectiveConfig holds the validated and parsed configuration
// that was obtained from the user-provided configuration.
type loadLimiterEffectiveConfig struct {
	// max absolute load
	MaxLoad uint64

	// window composition
	WindowSize        uint64
	WindowSegmentSize uint64
	NumSegments       uint64

	// features control
	SkipRetryInComputing bool

	// overstep penalty
	ApplyOverstepPenalty       bool
	AbsoluteOverstepPenalty    uint64
	OverstepPenaltySegmentSpan uint64

	// request overhead penalty
	ApplyRequestOverheadPenalty       bool
	RequestOverheadPenaltyFactor      float64
	RequestOverheadPenaltySegmentSpan uint64

	// penalty capping
	ApplyPenaltyCapping   bool
	AbsoluteMaxPenaltyCap uint64
}

// windowSegment represents a single segment the activeWindow is divided in
type windowSegment struct {
	StartTime uint64
	Value     uint64
}

func (instance *loadLimiterDefaultImpl) getTenant(key string) *loadLimiterDefaultImplTenantData {
	existing, exists := instance.TenantData[key]
	if exists {
		return existing
	}

	newTenantData := &loadLimiterDefaultImplTenantData{
		WindowTotal: 0,
		WasOver:     false,
		Version:     1,
	}

	// call setMinCapacity on queue
	// to avoid dynamically resizing and improve performance.
	windowQueue := instance.newWindowQueue()
	newTenantData.WindowQueue = windowQueue

	instance.TenantData[key] = newTenantData
	return newTenantData
}

func (instance *loadLimiterDefaultImpl) newWindowQueue() *deque.Deque {
	minQueueCapacity := int(instance.Config.NumSegments) * 3
	return deque.New(minQueueCapacity, minQueueCapacity)
}

func (instance *loadLimiterDefaultImpl) currentTime() time.Time {
	// hook time provider here to allow easier testing
	return instance.TimeFunc()
}

func (instance *loadLimiterDefaultImpl) sleep(d time.Duration) {
	// hook time provider here to allow easier testing
	instance.SleepFunc(d)
}

// for future usage (persistence)
func (instance *loadLimiterDefaultImpl) markDirty(req *submitRequest) {
	req.TenantData.Version++
}

func (instance *loadLimiterDefaultImpl) IsComposite() bool {
	return false
}

// Stats returns runtime statistics useful to evaluate system status,
// performance and overhead.
func (instance *loadLimiterDefaultImpl) Stats(tenantKey string) (RuntimeStatistics, error) {
	instance.Lock.Lock()
	defer instance.Lock.Unlock()

	var out RuntimeStatistics
	var outErr error

	err := instance.withSyncTransaction(func() {
		out, outErr = instance.stats(tenantKey)
	}, syncTxOptions{
		TenantKey: tenantKey,
		ReadOnly:  true,
	})
	if err != nil {
		return out, err
	}

	return out, outErr
}

func (instance *loadLimiterDefaultImpl) stats(tenantKey string) (RuntimeStatistics, error) {

	tenant := instance.getTenant(tenantKey)

	var out RuntimeStatistics

	qLen := tenant.WindowQueue.Len()

	segments := make([]uint64, qLen)

	for i := 0; i < qLen; i++ {
		segments[i] = tenant.WindowQueue.At(i).(*windowSegment).Value
	}

	out = RuntimeStatistics{
		WindowTotal:    tenant.WindowTotal,
		WindowSegments: segments,
	}

	return out, nil
}

// core methods have been moved to the submit.go and window.go files
