package goll

import "time"

// LoadLimiter is the parent interface for all kinds
// of load limiters.
//
// You are encouraged to use this type when storing references
// to your limiters in order to allow for easier implementations switch.
type LoadLimiter interface {
	// Probe checks if the given load would be allowed right now.
	// it is a readonly method that does not modify the current window data.
	Probe(tenantKey string, load uint64) (bool, error)

	// Submit asks for the given load to be accepted.
	// The result object contains an Accepted property
	// together with RetryIn information when available.
	Submit(tenantKey string, load uint64) (SubmitResult, error)

	// SubmitUntil asks for the given load to be accepted and,
	// in case of rejection, automatically handles retries and delays.
	// In case of acceptance a nil value is returned.
	// In case of timeout or other errors a non-nil error is returned.
	//
	// You can check the returned error with errors.Is against
	// the sentinels goll.ErrLoadRequestTimeout or goll.ErrLoadRequestRejected,
	// or you can cast them to the
	// goll.LoadRequestSubmissionTimeout / goll.LoadRequestRejected
	// types if you need additional info.
	SubmitUntil(tenantKey string, load uint64, timeout time.Duration) error

	// SubmitUntil asks for the given load to be accepted and,
	// in case of rejection, automatically handles retries and delays.
	// In case of acceptance a nil Error field is returned in the output object.
	// In case of timeout or other errors a non-nil Error field is returned in the output object.
	//
	// Unlike SubmitUntil, more information about the request is returned with the output object,
	// like the amount of time waited and the amount of submissions attempt.
	//
	// You can check the returned Error field with errors.Is against
	// the sentinels goll.ErrLoadRequestTimeout or goll.ErrLoadRequestRejected,
	// or you can cast them to the
	// goll.LoadRequestSubmissionTimeout / goll.LoadRequestRejected
	// types if you need additional info.
	SubmitUntilWithDetails(tenantKey string, load uint64, timeout time.Duration) SubmitUntilResult

	// IsComposite returns true if the limiter is a CompositeLoadLimiter.
	IsComposite() bool
}

// StandaloneLoadLimiter is the specialized interface for the standard
// load limiters created with goll.New(...).
//
// Note that all types implementing StandaloneLoadLimiter also implements LoadLimiter:
// You are encouraged to use this type when storing references
// to your limiters in order to allow for easier implementations switch.
type StandaloneLoadLimiter interface {
	// Probe checks if the given load would be allowed right now.
	// it is a readonly method that does not modify the current window data.
	Probe(tenantKey string, load uint64) (bool, error)

	// Submit asks for the given load to be accepted.
	// The result object contains an Accepted property
	// together with RetryIn information when available.
	Submit(tenantKey string, load uint64) (SubmitResult, error)

	// SubmitUntil asks for the given load to be accepted and,
	// in case of rejection, automatically handles retries and delays.
	// In case of acceptance a nil value is returned.
	// In case of timeout or other errors a non-nil error is returned.
	//
	// You can check the returned error with errors.Is against
	// the sentinels goll.ErrLoadRequestTimeout or goll.ErrLoadRequestRejected,
	// or you can cast them to the
	// goll.LoadRequestSubmissionTimeout / goll.LoadRequestRejected
	// types if you need additional info.
	SubmitUntil(tenantKey string, load uint64, timeout time.Duration) error

	// SubmitUntil asks for the given load to be accepted and,
	// in case of rejection, automatically handles retries and delays.
	// In case of acceptance a nil Error field is returned in the output object.
	// In case of timeout or other errors a non-nil Error field is returned in the output object.
	//
	// Unlike SubmitUntil, more information about the request is returned with the output object,
	// like the amount of time waited and the amount of submissions attempt.
	//
	// You can check the returned Error field with errors.Is against
	// the sentinels goll.ErrLoadRequestTimeout or goll.ErrLoadRequestRejected,
	// or you can cast them to the
	// goll.LoadRequestSubmissionTimeout / goll.LoadRequestRejected
	// types if you need additional info.
	SubmitUntilWithDetails(tenantKey string, load uint64, timeout time.Duration) SubmitUntilResult

	// IsComposite is "inherited" from LoadLimiter
	// and always returns false for this type.
	IsComposite() bool

	// Stats returns runtime statistics useful to evaluate system status,
	// performance and overhead.
	Stats(tenantKey string) (RuntimeStatistics, error)

	// ForTenant returns a semplified proxy that applies the load limiting
	// for the specified tenant, dropping the tenantKey input parameter.
	//
	// It's useful to simplify the code when you are acting on a single tenant.
	//
	// Please note that this does not create a new limiter instance,
	// it just proxies the calls to the current limiter adding a fixed tenantKey.
	ForTenant(tenantKey string) SingleTenantStandaloneLoadLimiter

	// ForTenant returns a semplified proxy drops the tenantKey input parameter.
	//
	// It's useful to simplify the code when you don't need multitenancy.
	//
	// Please note that this does not create a new limiter instance,
	// it just proxies the calls to the current limiter adding a fixed tenantKey.
	AsSingleTenant() SingleTenantStandaloneLoadLimiter
}

// CompositeLoadLimiter is the specialized interface for the composite
// load limiters created with goll.NewComposite(...).
//
// Note that all types implementing CompositeLoadLimiter also implements LoadLimiter:
// You are encouraged to use this type when storing references
// to your limiters in order to allow for easier implementations switch.
type CompositeLoadLimiter interface {
	// Probe checks if the given load would be allowed right now.
	// it is a readonly method that does not modify the current window data.
	Probe(tenantKey string, load uint64) (bool, error)

	// Submit asks for the given load to be accepted.
	// The result object contains an Accepted property
	// together with RetryIn information when available.
	Submit(tenantKey string, load uint64) (SubmitResult, error)

	// SubmitUntil asks for the given load to be accepted and,
	// in case of rejection, automatically handles retries and delays.
	// In case of acceptance a nil value is returned.
	// In case of timeout or other errors a non-nil error is returned.
	//
	// You can check the returned error with errors.Is against
	// the sentinels goll.ErrLoadRequestTimeout or goll.ErrLoadRequestRejected,
	// or you can cast them to the
	// goll.LoadRequestSubmissionTimeout / goll.LoadRequestRejected
	// types if you need additional info.
	SubmitUntil(tenantKey string, load uint64, timeout time.Duration) error

	// SubmitUntil asks for the given load to be accepted and,
	// in case of rejection, automatically handles retries and delays.
	// In case of acceptance a nil Error field is returned in the output object.
	// In case of timeout or other errors a non-nil Error field is returned in the output object.
	//
	// Unlike SubmitUntil, more information about the request is returned with the output object,
	// like the amount of time waited and the amount of submissions attempt.
	//
	// You can check the returned Error field with errors.Is against
	// the sentinels goll.ErrLoadRequestTimeout or goll.ErrLoadRequestRejected,
	// or you can cast them to the
	// goll.LoadRequestSubmissionTimeout / goll.LoadRequestRejected
	// types if you need additional info.
	SubmitUntilWithDetails(tenantKey string, load uint64, timeout time.Duration) SubmitUntilResult

	// IsComposite is "inherited" from LoadLimiter
	// and always returns true for this type.
	IsComposite() bool

	// Stats returns runtime statistics useful to evaluate system status,
	// performance and overhead.
	//
	// In the case of a composite limiter, both statistics about the
	// composite limiter itself and statistics for all the single composed
	// limiters will be returned.
	Stats(tenantKey string) (CompositeRuntimeStatistics, error)

	// ForTenant returns a semplified proxy that applies the load limiting
	// for the specified tenant, dropping the tenantKey input parameter.
	//
	// It's useful to simplify the code when you are acting on a single tenant.
	//
	// Please note that this does not create a new limiter instance,
	// it just proxies the calls to the current limiter adding a fixed tenantKey.
	ForTenant(tenantKey string) SingleTenantCompositeLoadLimiter

	// ForTenant returns a semplified proxy drops the tenantKey input parameter.
	//
	// It's useful to simplify the code when you don't need multitenancy.
	//
	// Please note that this does not create a new limiter instance,
	// it just proxies the calls to the current limiter adding a fixed tenantKey.
	AsSingleTenant() SingleTenantCompositeLoadLimiter
}

// SingleTenantLoadLimiter is the specialized interface
// for load limiters that do not need to handle multitenancy.
//
// It works exactly like the standard limiter but drops the tenantKey
type SingleTenantLoadLimiter interface {
	// Probe checks if the given load would be allowed right now.
	// it is a readonly method that does not modify the current window data.
	Probe(load uint64) (bool, error)

	// Submit asks for the given load to be accepted.
	// The result object contains an Accepted property
	// together with RetryIn information when available.
	Submit(load uint64) (SubmitResult, error)

	// SubmitUntil asks for the given load to be accepted and,
	// in case of rejection, automatically handles retries and delays.
	// In case of acceptance a nil value is returned.
	// In case of timeout or other errors a non-nil error is returned.
	//
	// You can check the returned error with errors.Is against
	// the sentinels goll.ErrLoadRequestTimeout or goll.ErrLoadRequestRejected,
	// or you can cast them to the
	// goll.LoadRequestSubmissionTimeout / goll.LoadRequestRejected
	// types if you need additional info.
	SubmitUntil(load uint64, timeout time.Duration) error

	// SubmitUntil asks for the given load to be accepted and,
	// in case of rejection, automatically handles retries and delays.
	// In case of acceptance a nil Error field is returned in the output object.
	// In case of timeout or other errors a non-nil Error field is returned in the output object.
	//
	// Unlike SubmitUntil, more information about the request is returned with the output object,
	// like the amount of time waited and the amount of submissions attempt.
	//
	// You can check the returned Error field with errors.Is against
	// the sentinels goll.ErrLoadRequestTimeout or goll.ErrLoadRequestRejected,
	// or you can cast them to the
	// goll.LoadRequestSubmissionTimeout / goll.LoadRequestRejected
	// types if you need additional info.
	SubmitUntilWithDetails(load uint64, timeout time.Duration) SubmitUntilResult

	// IsComposite returns true if the limiter is a CompositeLoadLimiter.
	IsComposite() bool
}

// SingleTenantStandaloneLoadLimiter is the specialized interface
// for standalone (non composite) load limiters that do not need to handle multitenancy.
//
// It works exactly like the standard standalone limiter but drops the tenantKey
type SingleTenantStandaloneLoadLimiter interface {
	// Probe checks if the given load would be allowed right now.
	// it is a readonly method that does not modify the current window data.
	Probe(load uint64) (bool, error)

	// Submit asks for the given load to be accepted.
	// The result object contains an Accepted property
	// together with RetryIn information when available.
	Submit(load uint64) (SubmitResult, error)

	// SubmitUntil asks for the given load to be accepted and,
	// in case of rejection, automatically handles retries and delays.
	// In case of acceptance a nil value is returned.
	// In case of timeout or other errors a non-nil error is returned.
	//
	// You can check the returned error with errors.Is against
	// the sentinels goll.ErrLoadRequestTimeout or goll.ErrLoadRequestRejected,
	// or you can cast them to the
	// goll.LoadRequestSubmissionTimeout / goll.LoadRequestRejected
	// types if you need additional info.
	SubmitUntil(load uint64, timeout time.Duration) error

	// SubmitUntil asks for the given load to be accepted and,
	// in case of rejection, automatically handles retries and delays.
	// In case of acceptance a nil Error field is returned in the output object.
	// In case of timeout or other errors a non-nil Error field is returned in the output object.
	//
	// Unlike SubmitUntil, more information about the request is returned with the output object,
	// like the amount of time waited and the amount of submissions attempt.
	//
	// You can check the returned Error field with errors.Is against
	// the sentinels goll.ErrLoadRequestTimeout or goll.ErrLoadRequestRejected,
	// or you can cast them to the
	// goll.LoadRequestSubmissionTimeout / goll.LoadRequestRejected
	// types if you need additional info.
	SubmitUntilWithDetails(load uint64, timeout time.Duration) SubmitUntilResult

	// Stats returns runtime statistics useful to evaluate system status,
	// performance and overhead.
	Stats() (RuntimeStatistics, error)

	// IsComposite always returns false for this type.
	IsComposite() bool
}

// SingleTenantCompositeLoadLimiter is the specialized interface
// for composite load limiters that do not need to handle multitenancy.
//
// It works exactly like the standard composite limiter but drops the tenantKey
type SingleTenantCompositeLoadLimiter interface {
	// Probe checks if the given load would be allowed right now.
	// it is a readonly method that does not modify the current window data.
	Probe(load uint64) (bool, error)

	// Submit asks for the given load to be accepted.
	// The result object contains an Accepted property
	// together with RetryIn information when available.
	Submit(load uint64) (SubmitResult, error)

	// SubmitUntil asks for the given load to be accepted and,
	// in case of rejection, automatically handles retries and delays.
	// In case of acceptance a nil value is returned.
	// In case of timeout or other errors a non-nil error is returned.
	//
	// You can check the returned error with errors.Is against
	// the sentinels goll.ErrLoadRequestTimeout or goll.ErrLoadRequestRejected,
	// or you can cast them to the
	// goll.LoadRequestSubmissionTimeout / goll.LoadRequestRejected
	// types if you need additional info.
	SubmitUntil(load uint64, timeout time.Duration) error

	// SubmitUntil asks for the given load to be accepted and,
	// in case of rejection, automatically handles retries and delays.
	// In case of acceptance a nil Error field is returned in the output object.
	// In case of timeout or other errors a non-nil Error field is returned in the output object.
	//
	// Unlike SubmitUntil, more information about the request is returned with the output object,
	// like the amount of time waited and the amount of submissions attempt.
	//
	// You can check the returned Error field with errors.Is against
	// the sentinels goll.ErrLoadRequestTimeout or goll.ErrLoadRequestRejected,
	// or you can cast them to the
	// goll.LoadRequestSubmissionTimeout / goll.LoadRequestRejected
	// types if you need additional info.
	SubmitUntilWithDetails(load uint64, timeout time.Duration) SubmitUntilResult

	// Stats returns runtime statistics useful to evaluate system status,
	// performance and overhead.
	//
	// In the case of a composite limiter, both statistics about the
	// composite limiter itself and statistics for all the single composed
	// limiters will be returned.
	Stats() (CompositeRuntimeStatistics, error)

	// IsComposite always returns true for this type.
	IsComposite() bool
}

// RuntimeStatistics holds runtime statistics
// for a single load limiter.
type RuntimeStatistics struct {
	// WindowTotal holds the current active load in absolute units.
	WindowTotal uint64

	// WindowSegments is a slice holding the amount of absolute load
	// allocated to each segment of the window.
	WindowSegments []uint64
}

// RuntimeStatistics holds runtime statistics
// for a composite load limiter.
type CompositeRuntimeStatistics struct {

	// LimitersStats holds the statistics for each composed limiter
	LimitersStats []RuntimeStatistics
}
