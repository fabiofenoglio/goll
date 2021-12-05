package goll

import (
	"fmt"
	"sync"
	"time"
)

type compositeLoadLimiterDefaultImpl struct {
	Logger   Logger
	Config   *compositeLoadLimiterEffectiveConfig
	Limiters []*loadLimiterDefaultImpl
	Lock     sync.Mutex

	// SyncAdapter is an implementation used to synchronize
	// the limiter data in a clustered environment.
	SyncAdapter SyncAdapter

	TimeFunc  func() time.Time
	SleepFunc func(d time.Duration)
}

type compositeLoadLimiterEffectiveConfig struct{}

func (instance *compositeLoadLimiterDefaultImpl) currentTime() time.Time {
	// hook time provider here to allow easier testing
	return instance.TimeFunc()
}

func (instance *compositeLoadLimiterDefaultImpl) sleep(d time.Duration) {
	// hook time provider here to allow easier testing
	instance.SleepFunc(d)
}

// Probe checks if the given load would be allowed right now.
// it is a readonly method that does not modify the current window data.
func (instance *compositeLoadLimiterDefaultImpl) Probe(tenantKey string, load uint64) (bool, error) {
	t := instance.currentTime()

	// lock the composite instance for thread safety.
	instance.Lock.Lock()
	defer instance.Lock.Unlock()

	outResult := true
	var outErr error

	err := instance.withSyncTransaction(func() {
		// a composite Probe will return true
		// if all combined limiters do.
		for _, limiter := range instance.Limiters {
			req := limiter.buildLoadRequest(t, tenantKey, load)

			r := limiter.probe(req)

			if !r {
				outResult = false
				break
			}
		}

	}, syncTxOptions{
		TenantKey: tenantKey,
		ReadOnly:  true,
	})

	if err != nil {
		return false, err
	}

	return outResult, outErr
}

// Submit asks for the given load to be accepted.
// The result object contains an Accepted property
// together with RetryIn information when available.
//
// Submit for a composite instance
// behaves like the same method for the single limiter
// but is a bit more complex
// because it requires a multi-phase process.
//
// first all the instances are probed
// only if all the instances returned true,
// acceptLoad is called on every instance.
//
// otherwise rejectLoad is called on all the rejecting instances
// the probed and not-confirmed instances that returned true are discarded
//
// if at least one of the rejection responses had a valid RetryIn field
// the output will have a RetryIn corresponding to the highest
// RetryIn of all reject responses.
func (instance *compositeLoadLimiterDefaultImpl) Submit(tenantKey string, load uint64) (SubmitResult, error) {

	// lock the composite instance for thread safety.
	instance.Lock.Lock()
	defer instance.Lock.Unlock()

	var result SubmitResult

	err := instance.withSyncTransaction(func() {
		result = instance.submit(tenantKey, load)
	}, syncTxOptions{
		TenantKey: tenantKey,
		ReadOnly:  false,
	})

	return result, err
}

func (instance *compositeLoadLimiterDefaultImpl) submit(tenantKey string, load uint64) SubmitResult {
	allAccepted := true
	highestWaitTime := time.Duration(0)

	t := instance.currentTime()

	// the probe/acceptLoad/rejectLoad flow requires
	// a stateful struct to be passed.
	// since the process is multiphase, we have to save
	// those structs and use the same in every phase.
	requestMaps := make(map[int]*submitRequest)

	for i, limiter := range instance.Limiters {

		sr := limiter.buildLoadRequest(t, tenantKey, load)
		requestMaps[i] = sr

		// first all the instances are probed
		probeResult := limiter.probe(sr)

		if !probeResult {
			allAccepted = false

			// rejectLoad is called on all the rejecting instances
			rejectionResult := limiter.rejectLoad(sr)

			// if at least one of the rejection responses had a valid RetryIn field
			// the output will have a RetryIn corresponding to the highest
			// RetryIn of all reject responses.
			if rejectionResult.RetryInAvailable &&
				(highestWaitTime == 0 || rejectionResult.RetryIn > highestWaitTime) {
				highestWaitTime = rejectionResult.RetryIn
			}
		}
	}

	if allAccepted {
		// only if all the instances returned true,
		// acceptLoad is called on every instance.
		for i, limiter := range instance.Limiters {
			req := requestMaps[i]
			limiter.acceptLoad(req)
		}
	}

	res := SubmitResult{
		Accepted:         allAccepted,
		RetryInAvailable: (!allAccepted && highestWaitTime > 0),
		RetryIn:          highestWaitTime,
	}

	return res
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
func (instance *compositeLoadLimiterDefaultImpl) SubmitUntil(tenantKey string, load uint64, timeout time.Duration) error {
	return instance.submitUntil(tenantKey, load, timeout).Error
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
func (instance *compositeLoadLimiterDefaultImpl) SubmitUntilWithDetails(tenantKey string, load uint64, timeout time.Duration) SubmitUntilResult {
	return instance.submitUntil(tenantKey, load, timeout)
}

func (instance *compositeLoadLimiterDefaultImpl) submitUntil(tenantKey string, load uint64, timeout time.Duration) SubmitUntilResult {

	// save the original request time to compute the timeout treshold
	t := instance.currentTime()

	out := SubmitUntilResult{
		AttemptsNumber: 0,
		WaitedFor:      0,
		Error:          nil,
	}

	// a negative timeout is not allowed
	// and will be rejected immediately
	if timeout < 0 {
		instance.Logger.Warning("submit of task failed because of invalid timeout")
		out.Error = &LoadRequestRejected{
			Reason: "invalid timeout",
		}
		return out
	}

	// compute the timeout treshold
	timeoutAt := t.Add(timeout)

	for {
		out.AttemptsNumber++

		// try a submit
		submitResult, err := instance.Submit(tenantKey, load)
		if err != nil {
			instance.Logger.Warning(fmt.Sprintf("submit of task failed: %s", err.Error()))
			out.Error = fmt.Errorf("error submitting load request: %w", err)
			break
		}

		if submitResult.Accepted {
			// accepted! break and return.
			break
		}

		// the request was rejected.
		// if not RetryIn was provided with the rejection,
		// we can't apply a retry policy.
		// So we fail with a LoadRequestRejected error
		if !submitResult.RetryInAvailable || submitResult.RetryIn <= 0 {
			instance.Logger.Warning("submit of task failed and can't be retried")
			out.Error = &LoadRequestRejected{
				Reason: "excessive requested load",
			}
			break
		}

		// We got a RetryIn from the rejection.
		// If the current time plus the required wait time
		// would go over the timeout treshold there's no point in waiting,
		// we fail with a LoadRequestTimeout error
		// without waiting.
		if instance.currentTime().Add(submitResult.RetryIn).After(timeoutAt) {
			instance.Logger.Warning("submit of task failed and retrying timed out")
			out.Error = &LoadRequestTimeout{
				WaitedFor:      out.WaitedFor,
				AttemptsNumber: out.AttemptsNumber,
			}
			break
		}

		// sleep for the exact required amount of time.
		waitFor := submitResult.RetryIn
		instance.Logger.Debug(fmt.Sprintf("submit of task was rejected, waiting %v ms and retrying", waitFor.Milliseconds()))
		instance.sleep(waitFor)
		out.WaitedFor += waitFor

		instance.Logger.Debug("submit of task will now be reattempted")
	}

	return out
}

// Stats returns runtime statistics usefule to evaluate system status,
// performance and overhead.
//
// In the case of a composite limiter, both statistics about the
// composite limiter itself and statistics for all the single composed
// limiters will be returned.
func (instance *compositeLoadLimiterDefaultImpl) Stats(tenantKey string) (CompositeRuntimeStatistics, error) {
	instance.Lock.Lock()
	defer instance.Lock.Unlock()

	var out CompositeRuntimeStatistics
	var outErr error

	err := instance.withSyncTransaction(func() {

		out = CompositeRuntimeStatistics{}

		cs, err := instance.compositeStats(tenantKey)
		if err != nil {
			outErr = err
			return
		}

		out.LimitersStats = cs

	}, syncTxOptions{
		TenantKey: tenantKey,
		ReadOnly:  true,
	})

	if err != nil {
		return out, err
	}

	return out, outErr
}

// compositeStats aggregates the statistics from the single loadLimiters.
func (instance *compositeLoadLimiterDefaultImpl) compositeStats(tenantKey string) ([]RuntimeStatistics, error) {

	num := len(instance.Limiters)
	out := make([]RuntimeStatistics, num)

	for i, limiter := range instance.Limiters {
		ls, err := limiter.stats(tenantKey)
		if err != nil {
			return nil, err
		}
		out[i] = ls
	}

	return out, nil
}

func (instance *compositeLoadLimiterDefaultImpl) IsComposite() bool {
	return true
}
