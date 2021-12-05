package goll

import (
	"fmt"
	"time"
)

var (
	// ErrLoadRequestTimeout is a sentinel for the error that
	// occurs when autoretrying a submission (ex. with SubmitUntil)
	// failed because the maximum timeout was reached
	ErrLoadRequestTimeout = &LoadRequestTimeout{}

	// ErrLoadRequestRejected is a sentinel for the error that
	// occurs when a submission can't be accepted
	// usually happens when:
	// - the request asks for a load greater than the limiter maximum load
	// - the request gets rejected and the limiter was built with SkipRetryInComputing = true
	ErrLoadRequestRejected = &LoadRequestRejected{}
)

// LoadRequestTimeout is returned when autoretrying a submission (ex. with SubmitUntil)
// failed because the maximum timeout was reached
type LoadRequestTimeout struct {
	AttemptsNumber uint64
	WaitedFor      time.Duration
}

func (e *LoadRequestTimeout) Error() string {
	return fmt.Sprintf(
		"LoadRequestTimeout: load submission failed and timed out after %v attempts in %v ms",
		e.AttemptsNumber,
		e.WaitedFor.Milliseconds(),
	)
}

func (e *LoadRequestTimeout) Is(tgt error) bool {
	_, ok := tgt.(*LoadRequestTimeout)
	return ok
}

// LoadRequestRejected is returned when a submission can't be accepted
// usually happens when:
// - the request asks for a load greater than the limiter maximum load
// - the request gets rejected and the limiter was built with SkipRetryInComputing = true
type LoadRequestRejected struct {
	Reason string
}

func (e *LoadRequestRejected) Error() string {
	return fmt.Sprintf("LoadRequestRejected: the requested load can't be accepted (%v)", e.Reason)
}

func (e *LoadRequestRejected) Is(tgt error) bool {
	_, ok := tgt.(*LoadRequestRejected)
	return ok
}
