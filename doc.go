// A highly configurable, feature-packed, variable-request-sized load limiter module.
//
// Features:
//
// - Sliding window/buckets algorithm for smooth limiting and cooldown
//
// - Limit amount of 'load' instead of simply limiting the number of requests by allowing load-aware requests
//
// - Support for automatic retry, delay or timeout on load submissions
//
// - Automatically compute RetryIn (time-to-availability) to easily give clients an amount of time to wait before resubmissions
//
// - Configurable penalties for over-max-load requests and for uncompliant clients who do not respect the required delays
//
// - Configurable window fragmentation for optimal smoothness vs performance tuning
//
// - Thread safe
//
package goll
