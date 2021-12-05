package goll

import (
	"fmt"
	"math"
	"time"
)

// SubmitResult holds the result of a load request.
//
// the Accepted field will be true if the request was accepted.
//
// If the request was rejected and RetryIn is enabled,
// the RetryInAvailable field will be true and the RetryIn field
// will be the amount the client is required to wait
// before resubmitting a request for the same load.
type SubmitResult struct {
	Accepted         bool
	RetryInAvailable bool
	RetryIn          time.Duration
}

// SubmitUntilResult holds the result of a load request
// automatically handled and optionally retried via SubmitUntil.
//
// the Error field will be nil if the request was accepted.
//
// The AttemptsNumber and WaitedFor will provide information about
// the delay and attempts made by the SubmitUntil handler.
//
// You can check the returned Error field with errors.Is against
// the sentinels goll.ErrLoadRequestTimeout or goll.ErrLoadRequestRejected,
// or you can cast them to the
// gollLoadRequestSubmissionTimeout / goll.LoadRequestRejected
// types if you need additional info.
type SubmitUntilResult struct {
	AttemptsNumber uint64
	WaitedFor      time.Duration
	Error          error
}

func (s *SubmitResult) String() string {
	if s.Accepted {
		return "LoadRequestSubmitResult[Accepted]"
	} else if s.RetryInAvailable {
		return fmt.Sprintf("LoadRequestSubmitResult[Rejected, RetryIn: %v ms]", s.RetryIn.Milliseconds())
	} else {
		return "LoadRequestSubmitResult[Rejected]"
	}
}

// we use this struct to pass info around
type submitRequest struct {
	TenantKey               string
	TenantData              *loadLimiterDefaultImplTenantData
	RequestedLoad           uint64
	RequestedTimestamp      uint64
	RequestSegmentStartTime uint64
}

func (instance *loadLimiterDefaultImpl) buildLoadRequest(timestamp time.Time, tenantKey string, load uint64) *submitRequest {
	t := uint64(timestamp.UnixMilli())

	return &submitRequest{
		TenantKey:               tenantKey,
		TenantData:              instance.getTenant(tenantKey),
		RequestedLoad:           load,
		RequestedTimestamp:      t,
		RequestSegmentStartTime: instance.locateSegmentStartTime(t),
	}
}

// Probe checks if the given load would be allowed right now.
// it is a readonly method that does not modify the current window data.
func (instance *loadLimiterDefaultImpl) Probe(tenantKey string, load uint64) (bool, error) {
	t := instance.currentTime()

	instance.Lock.Lock()
	defer instance.Lock.Unlock()

	var result bool

	err := instance.withSyncTransaction(func() {
		req := instance.buildLoadRequest(t, tenantKey, load)

		result = instance.probe(req)
	}, syncTxOptions{
		TenantKey: tenantKey,
		ReadOnly:  true,
	})

	if err != nil {
		return false, err
	}

	return result, nil
}

func (instance *loadLimiterDefaultImpl) probe(req *submitRequest) bool {
	instance.rotateWindow(req)

	totalWouldBe := req.TenantData.WindowTotal + req.RequestedLoad

	return totalWouldBe <= instance.Config.MaxLoad
}

// Submit asks for the given load to be accepted.
// The result object contains an Accepted property
// together with RetryIn information when available.
func (instance *loadLimiterDefaultImpl) Submit(tenantKey string, load uint64) (SubmitResult, error) {
	t := instance.currentTime()

	instance.Lock.Lock()
	defer instance.Lock.Unlock()

	var res SubmitResult

	err := instance.withSyncTransaction(func() {
		req := instance.buildLoadRequest(t, tenantKey, load)

		if instance.probe(req) {
			instance.acceptLoad(req)
			res = SubmitResult{
				Accepted: true,
			}
		} else {
			rejectDetails := instance.rejectLoad(req)
			res = *rejectDetails
		}

	}, syncTxOptions{
		TenantKey: tenantKey,
		ReadOnly:  false,
	})

	if err != nil {
		return res, err
	}

	return res, nil
}

func (instance *loadLimiterDefaultImpl) acceptLoad(req *submitRequest) {
	tenant := req.TenantData

	currentSegment := tenant.WindowQueue.Front().(*windowSegment)

	tenant.WasOver = false

	tenant.WindowTotal += req.RequestedLoad
	currentSegment.Value += req.RequestedLoad

	instance.applyCapping(req)
	instance.markDirty(req)
}

func (instance *loadLimiterDefaultImpl) rejectLoad(req *submitRequest) *SubmitResult {
	tenant := req.TenantData

	someAdded := false
	dirty := false

	if !tenant.WasOver {
		// instance was not overloaded, this request is the first to overstep
		if instance.Config.ApplyOverstepPenalty {
			instance.distributePenalty(
				req,
				instance.Config.AbsoluteOverstepPenalty,
				instance.Config.OverstepPenaltySegmentSpan,
			)
			someAdded = true
		}

		// switch to overload status
		tenant.WasOver = true
		dirty = true

	} else {
		// request submitted when instance was already overloaded
		if instance.Config.ApplyRequestOverheadPenalty {
			penalty := math.Round(instance.Config.RequestOverheadPenaltyFactor * float64(req.RequestedLoad))
			if penalty >= 1.0 {
				instance.distributePenalty(
					req,
					uint64(penalty),
					instance.Config.RequestOverheadPenaltySegmentSpan,
				)
				someAdded = true
				dirty = true
			}
		}
	}

	if someAdded {
		instance.applyCapping(req)
	}
	if dirty {
		instance.markDirty(req)
	}

	if !instance.Config.SkipRetryInComputing {
		if retryIn, err := instance.computeRetryIn(req); err == nil {
			return &SubmitResult{
				Accepted:         false,
				RetryInAvailable: true,
				RetryIn:          retryIn,
			}
		}
	}

	return &SubmitResult{
		Accepted:         false,
		RetryInAvailable: false,
	}
}

// SubmitUntil asks for the given load to be accepted and,
// in case of rejection, automatically handles retries and delays.
// In case of acceptance a nil value is returned.
// In case of timeout or other errors a non-nil error is returned.
//
// You can check the returned error with errors.Is against
// the sentinels goll.ErrLoadRequestTimeout or goll.ErrLoadRequestRejected,
// or you can cast them to the
// gollLoadRequestSubmissionTimeout / goll.LoadRequestRejected
// types if you need additional info.
func (instance *loadLimiterDefaultImpl) SubmitUntil(tenantKey string, load uint64, timeout time.Duration) error {
	res := instance.submitUntil(tenantKey, load, timeout)
	return res.Error
}

// SubmitUntil asks for the given load to be accepted and,
// in case of rejection, automatically handles retries and delays.
// In case of acceptance a nil Error field is returned in the output object.
// In case of timeout or other errors a non-nil Error field is returned in the output object.
//
// Unlink SubmitUntil, more information about the request is returned with the output object,
// notably the amount of time waited and the amount of submissions attempt.
//
// You can check the returned Error field with errors.Is against
// the sentinels goll.ErrLoadRequestTimeout or goll.ErrLoadRequestRejected,
// or you can cast them to the
// gollLoadRequestSubmissionTimeout / goll.LoadRequestRejected
// types if you need additional info.
func (instance *loadLimiterDefaultImpl) SubmitUntilWithDetails(tenantKey string, load uint64, timeout time.Duration) SubmitUntilResult {
	return instance.submitUntil(tenantKey, load, timeout)
}

func (instance *loadLimiterDefaultImpl) submitUntil(tenantKey string, load uint64, timeout time.Duration) SubmitUntilResult {

	t := instance.currentTime()

	out := SubmitUntilResult{
		AttemptsNumber: 0,
		WaitedFor:      0,
		Error:          nil,
	}

	if timeout < 0 {
		instance.Logger.Warning("submit of task failed because of invalid timeout")
		out.Error = &LoadRequestRejected{
			Reason: "invalid timeout",
		}
		return out
	}

	timeoutAt := t.Add(timeout)

	for {
		out.AttemptsNumber++
		submitResult, err := instance.Submit(tenantKey, load)
		if err != nil {
			instance.Logger.Warning(fmt.Sprintf("submit of task failed: %s", err.Error()))
			out.Error = fmt.Errorf("error submitting load request: %w", err)
			break
		}

		if submitResult.Accepted {
			break
		}

		if instance.Config.SkipRetryInComputing {
			instance.Logger.Warning("submit of task failed and retry is not supported")
			out.Error = &LoadRequestRejected{
				Reason: "retry not supported",
			}
			break
		}

		if !submitResult.RetryInAvailable || submitResult.RetryIn <= 0 {
			instance.Logger.Warning("submit of task failed and can't be retried")
			out.Error = &LoadRequestRejected{
				Reason: "excessive requested load",
			}
			break
		}

		if instance.currentTime().Add(submitResult.RetryIn).After(timeoutAt) {
			instance.Logger.Warning("submit of task failed and retrying timed out")
			out.Error = &LoadRequestTimeout{
				WaitedFor:      out.WaitedFor,
				AttemptsNumber: out.AttemptsNumber,
			}
			break
		}

		waitFor := submitResult.RetryIn
		instance.Logger.Debug(fmt.Sprintf("submit of task was rejected, waiting %v ms and retrying", waitFor.Milliseconds()))
		instance.sleep(waitFor)
		out.WaitedFor += waitFor

		instance.Logger.Debug("submit of task will now be reattempted")
	}

	return out
}
