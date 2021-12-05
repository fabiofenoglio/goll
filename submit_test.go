package goll

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSubmit(t *testing.T) {
	ti := buildDefaultInstance(t)

	submitResult := submitNoError(ti.Instance.Submit(defaultTestTenantKey, 10))

	assert.NotNil(t, submitResult)
	assert.True(t, submitResult.Accepted)
	assert.False(t, submitResult.RetryInAvailable)
	assert.Zero(t, submitResult.RetryIn)

	assert.Contains(t, strings.ToLower(submitResult.String()), "accepted")

	assert.True(t, noErrors(ti.Instance.Probe(defaultTestTenantKey, 90)).(bool))

	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 90)).Accepted)

	rejected := submitNoError(ti.Instance.Submit(defaultTestTenantKey, 1))
	assert.False(t, rejected.Accepted)
	assert.True(t, rejected.RetryInAvailable)
	assert.Contains(t, strings.ToLower(rejected.String()), "rejected")
	assert.Contains(t, strings.ToLower(rejected.String()), "retry")

	rejected = submitNoError(ti.Instance.Submit(defaultTestTenantKey, 99999))
	assert.False(t, rejected.Accepted)
	assert.False(t, rejected.RetryInAvailable)
	assert.Contains(t, strings.ToLower(rejected.String()), "rejected")
	assert.NotContains(t, strings.ToLower(rejected.String()), "retry")

	assert.False(t, noErrors(ti.Instance.Probe(defaultTestTenantKey, 1)).(bool))

	ti.TimeTravel(defaultWindowSize.Milliseconds())

	assert.True(t, noErrors(ti.Instance.Probe(defaultTestTenantKey, 1)).(bool))

	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 1)).Accepted)
}

func BenchmarkSubmit50pc(b *testing.B) {
	ti := buildDefaultInstance(nil)
	instance := ti.Instance

	accepted := 0

	for i := 0; i < b.N; i++ {
		r := submitNoError(instance.Submit(defaultTestTenantKey, 2))
		if r.Accepted {
			accepted++
		}

		ti.TimeTravel(100)
	}

	if b.N > 10000 {
		pcg := 100.0 * float64(accepted) / float64(b.N)
		assert.True(b, pcg >= 40 && pcg <= 60,
			fmt.Sprintf(
				"the percentage of accepted requests"+
					" should be around 50 for a proper benchmarking"+
					" and is instead %.2f", pcg))
	}
}

func BenchmarkSubmitAllAccepted(b *testing.B) {
	ti := buildInstance(nil, func(config *Config) {
		config.MaxLoad *= 10
	})
	instance := ti.Instance

	accepted := 0

	for i := 0; i < b.N; i++ {
		r := submitNoError(instance.Submit(defaultTestTenantKey, 1))
		if r.Accepted {
			accepted++
		}

		ti.TimeTravel(100)
	}

	if b.N > 10000 {
		pcg := 100.0 * float64(accepted) / float64(b.N)
		assert.True(b, pcg >= 99.0,
			fmt.Sprintf(
				"the percentage of accepted requests"+
					" should be 100"+
					" and is instead %.2f", pcg))
	}
}

func BenchmarkSubmitAllRejected(b *testing.B) {
	ti := buildDefaultInstance(nil)
	instance := ti.Instance

	accepted := 0

	for i := 0; i < b.N; i++ {
		r := submitNoError(instance.Submit(defaultTestTenantKey, 30))
		if r.Accepted {
			accepted++
		}

		ti.TimeTravel(10)
	}

	if b.N > 10000 {
		pcg := 100.0 * float64(accepted) / float64(b.N)
		assert.True(b, pcg <= 1.0,
			fmt.Sprintf(
				"the percentage of accepted requests"+
					" should be less than 1"+
					" and is instead %.2f", pcg))
	}
}

func TestSubmitUntil(t *testing.T) {
	ti := buildDefaultInstance(t)
	applyMultiWindowConstantLoadDistribution(t, ti, defaultTestTenantKey, 8)

	// load not available, timeout 1ms. We expect to timeout with an ErrLoadRequestTimeout
	// but WaitedFor should be 0 (should not wait when it would be pointless to)
	res := ti.Instance.submitUntil(defaultTestTenantKey, 40, time.Duration(1)*time.Millisecond)

	assert.NotNil(t, res.Error)
	assert.ErrorIs(t, res.Error, ErrLoadRequestTimeout)
	assert.Contains(t, res.Error.Error(), "timed out")
	assert.Equal(t, uint64(1), res.AttemptsNumber)
	assert.Equal(t, time.Duration(0), res.WaitedFor)

	// goto 1019200
	ti.TimeTravel(200)

	// 20 currently available, asking for 40, 8 in each segment.
	// to free up three segments we have to wait 800 + 1000 + 1000 ms
	res = ti.Instance.submitUntil(defaultTestTenantKey, 40, time.Duration(10000)*time.Millisecond)

	assert.Nil(t, res.Error)
	assert.Equal(t, uint64(2), res.AttemptsNumber)
	assert.Equal(t, int64(2800), res.WaitedFor.Milliseconds())
}

func TestSubmitUntilExcessiveLoad(t *testing.T) {
	ti := buildDefaultInstance(t)

	// passing a negative duration should return a ErrLoadRequestRejected
	// without even attempting
	res := ti.Instance.submitUntil(defaultTestTenantKey, 5000000, time.Duration(1)*time.Second)

	assert.NotNil(t, res.Error)
	assert.ErrorIs(t, res.Error, ErrLoadRequestRejected)
	assert.Contains(t, res.Error.Error(), "excessive requested load")
	assert.Equal(t, uint64(1), res.AttemptsNumber)
	assert.Equal(t, time.Duration(0), res.WaitedFor)

	ti = buildDefaultInstance(t)
	applyMultiWindowConstantLoadDistribution(t, ti, defaultTestTenantKey, 8)
	res = ti.Instance.submitUntil(defaultTestTenantKey, 5000000, time.Duration(1)*time.Second)

	assert.NotNil(t, res.Error)
	assert.ErrorIs(t, res.Error, ErrLoadRequestRejected)
	assert.Contains(t, res.Error.Error(), "excessive requested load")
	assert.Equal(t, uint64(1), res.AttemptsNumber)
	assert.Equal(t, time.Duration(0), res.WaitedFor)
}

func TestSubmitUntilInvalidTimeout(t *testing.T) {
	ti := buildDefaultInstance(t)

	// passing a negative duration should return a ErrLoadRequestRejected
	// without even attempting
	res := ti.Instance.submitUntil(defaultTestTenantKey, 5, time.Duration(-1)*time.Second)

	assert.NotNil(t, res.Error)
	assert.ErrorIs(t, res.Error, ErrLoadRequestRejected)
	assert.Contains(t, res.Error.Error(), "invalid timeout")
	assert.Equal(t, uint64(0), res.AttemptsNumber)
	assert.Equal(t, time.Duration(0), res.WaitedFor)

	ti = buildDefaultInstance(t)
	applyMultiWindowConstantLoadDistribution(t, ti, defaultTestTenantKey, 8)
	res = ti.Instance.submitUntil(defaultTestTenantKey, 5, time.Duration(-1)*time.Second)

	assert.NotNil(t, res.Error)
	assert.ErrorIs(t, res.Error, ErrLoadRequestRejected)
	assert.Contains(t, res.Error.Error(), "invalid timeout")
	assert.Equal(t, uint64(0), res.AttemptsNumber)
	assert.Equal(t, time.Duration(0), res.WaitedFor)
}

func TestProbe(t *testing.T) {
	ti := buildDefaultInstance(t)

	// start at t = 1000000
	assert.Equal(t, "", ti.HashSegments(defaultTestTenantKey))

	assert.True(t, noErrors(ti.Instance.Probe(defaultTestTenantKey, 100)).(bool))
	assert.True(t, noErrors(ti.Instance.Probe(defaultTestTenantKey, 100)).(bool))
	assert.False(t, noErrors(ti.Instance.Probe(defaultTestTenantKey, 101)).(bool))

	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 10)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 10, "1000000:10")

	assert.True(t, noErrors(ti.Instance.Probe(defaultTestTenantKey, 90)).(bool))
	assert.True(t, noErrors(ti.Instance.Probe(defaultTestTenantKey, 1)).(bool))
	assert.False(t, noErrors(ti.Instance.Probe(defaultTestTenantKey, 91)).(bool))

	ti.TimeTravel(30000) // goto 1010000 after exactly 1 window size
	_, _ = ti.Instance.Probe(defaultTestTenantKey, 0)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 0, "1030000:0")
}

func TestStats(t *testing.T) {
	ti := buildDefaultInstance(t)

	// start at t = 1000000
	assert.Equal(t, "", ti.HashSegments(defaultTestTenantKey))

	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 10)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 10, "1000000:10")

	stats, err := ti.Instance.Stats(defaultTestTenantKey)
	assert.Nil(t, err)
	assert.Equal(t, RuntimeStatistics{
		WindowTotal:    uint64(10),
		WindowSegments: []uint64{10},
	}, stats)

	ti.TimeTravel(500) // goto 1000500
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 10)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 20, "1000000:20")

	stats, err = ti.Instance.Stats(defaultTestTenantKey)
	assert.Nil(t, err)
	assert.Equal(t, RuntimeStatistics{
		WindowTotal:    uint64(20),
		WindowSegments: []uint64{20},
	}, stats)

	ti.TimeTravel(500) // goto 1001000
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 30)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 50, "1001000:30", "1000000:20")

	stats, err = ti.Instance.Stats(defaultTestTenantKey)
	assert.Nil(t, err)
	assert.Equal(t, RuntimeStatistics{
		WindowTotal:    uint64(50),
		WindowSegments: []uint64{30, 20},
	}, stats)

	ti.TimeTravel(999) // goto 1001999
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 5)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 55, "1001000:35, 1000000:20")

	ti.TimeTravel(1) // goto 1002000
	ti.Instance.rotateWindow(ti.InternalRequest(defaultTestTenantKey, 0))
	ti.AssertWindowStatus(t, defaultTestTenantKey, 55, "1002000:0, 1001000:35, 1000000:20")

	ti.TimeTravel(8000) // goto 1010000 after exactly 1 window size
	ti.Instance.rotateWindow(ti.InternalRequest(defaultTestTenantKey, 0))
	ti.AssertWindowStatus(t, defaultTestTenantKey, 35, "1010000:0, 1002000:0, 1001000:35")

	ti.TimeTravel(999) // goto 1010999
	ti.Instance.rotateWindow(ti.InternalRequest(defaultTestTenantKey, 0))
	ti.AssertWindowStatus(t, defaultTestTenantKey, 35, "1010000:0, 1002000:0, 1001000:35")

	ti.TimeTravel(1) // goto 1011000
	ti.Instance.rotateWindow(ti.InternalRequest(defaultTestTenantKey, 0))
	ti.AssertWindowStatus(t, defaultTestTenantKey, 0, "1011000:0, 1010000:0, 1002000:0")

	stats, err = ti.Instance.Stats(defaultTestTenantKey)
	assert.Nil(t, err)
	assert.Equal(t, RuntimeStatistics{
		WindowTotal:    uint64(0),
		WindowSegments: []uint64{0, 0, 0},
	}, stats)

}
