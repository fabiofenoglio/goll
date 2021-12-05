package goll

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCompositeBasics(t *testing.T) {
	ti := buildDefaultCompositeInstance(t)

	assert.NotNil(t, ti.Instance)

	assert.True(t, noErrors(ti.Instance.Probe(defaultTestTenantKey, 1)).(bool))

	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 5)).Accepted)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 5)).Accepted)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 5)).Accepted)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 5)).Accepted)

	assert.False(t, noErrors(ti.Instance.Probe(defaultTestTenantKey, 1)).(bool))

	rejected := submitNoError(ti.Instance.Submit(defaultTestTenantKey, 1))
	assert.False(t, rejected.Accepted)
	assert.True(t, rejected.RetryInAvailable)
	assert.Equal(t, int64(1000), rejected.RetryIn.Milliseconds())

	// submitted 20 in the first segment, now wait
	stats, err := ti.Instance.Stats(defaultTestTenantKey)
	assert.Nil(t, err)

	assert.Equal(t, uint64(20), stats.LimitersStats[0].WindowTotal)

	assert.Equal(t, uint64(20), stats.LimitersStats[1].WindowTotal)

	ti.AssertWindowStatus(t, defaultTestTenantKey, []uint64{20, 20}, "0:1000000:20", "1:1000000:20")

	ti.TimeTravel(1000)
	_, _ = ti.Instance.Probe(defaultTestTenantKey, 0)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, []uint64{20, 0},
		"0:1001000:0", "0:1000000:20",
		"1:1001000:0",
	)
}

func TestCompositeImplementsLoadLimiter(t *testing.T) {
	ti := buildDefaultCompositeInstance(t)

	// ensure it implements correctly the superior LoadLimiter interface
	var asInterface LoadLimiter
	assert.Nil(t, asInterface)
	if asInterface == nil {
		asInterface = ti.Instance
		assert.True(t, noErrors(asInterface.Probe(defaultTestTenantKey, 1)).(bool))
	}
}

func TestCompositeSubmitUntil(t *testing.T) {
	ti := buildDefaultCompositeInstance(t)
	applyMultiWindowConstantLoadDistribution(t, ti, defaultTestTenantKey, 9)

	// load not available, timeout 1ms. We expect to timeout with an ErrLoadRequestTimeout
	// but WaitedFor should be 0 (should not wait when it would be pointless to)
	res := ti.Instance.submitUntil(defaultTestTenantKey, 15, 1*time.Millisecond)

	assert.NotNil(t, res.Error)
	assert.ErrorIs(t, res.Error, ErrLoadRequestTimeout)
	assert.Contains(t, res.Error.Error(), "timed out")
	assert.Equal(t, uint64(1), res.AttemptsNumber)
	assert.Equal(t, time.Duration(0), res.WaitedFor)

	// goto 1019200
	ti.TimeTravel(200)

	// 10 currently available, asking for 20, 9 in each segment.
	// to free up 2 segments we have to wait 800 + 1000 ms
	res = ti.Instance.submitUntil(defaultTestTenantKey, 20, time.Duration(10000)*time.Millisecond)

	assert.Nil(t, res.Error)
	assert.Equal(t, uint64(2), res.AttemptsNumber)
	assert.Equal(t, int64(1800), res.WaitedFor.Milliseconds())
}

func TestCompositeSubmitUntilExcessiveLoad(t *testing.T) {
	ti := buildDefaultCompositeInstance(t)

	// passing a negative duration should return a ErrLoadRequestRejected
	// without even attempting
	res := ti.Instance.submitUntil(defaultTestTenantKey, 5000000, time.Duration(1)*time.Second)

	assert.NotNil(t, res.Error)
	assert.ErrorIs(t, res.Error, ErrLoadRequestRejected)
	assert.Contains(t, res.Error.Error(), "excessive requested load")
	assert.Equal(t, uint64(1), res.AttemptsNumber)
	assert.Equal(t, time.Duration(0), res.WaitedFor)

	ti = buildDefaultCompositeInstance(t)
	applyMultiWindowConstantLoadDistribution(t, ti, defaultTestTenantKey, 8)
	res = ti.Instance.submitUntil(defaultTestTenantKey, 5000000, time.Duration(1)*time.Second)

	assert.NotNil(t, res.Error)
	assert.ErrorIs(t, res.Error, ErrLoadRequestRejected)
	assert.Contains(t, res.Error.Error(), "excessive requested load")
	assert.Equal(t, uint64(1), res.AttemptsNumber)
	assert.Equal(t, time.Duration(0), res.WaitedFor)
}

func TestCompositeSubmitUntilInvalidTimeout(t *testing.T) {
	ti := buildDefaultCompositeInstance(t)

	// passing a negative duration should return a ErrLoadRequestRejected
	// without even attempting
	res := ti.Instance.submitUntil(defaultTestTenantKey, 5, time.Duration(-1)*time.Second)

	assert.NotNil(t, res.Error)
	assert.ErrorIs(t, res.Error, ErrLoadRequestRejected)
	assert.Contains(t, res.Error.Error(), "invalid timeout")
	assert.Equal(t, uint64(0), res.AttemptsNumber)
	assert.Equal(t, time.Duration(0), res.WaitedFor)

	ti = buildDefaultCompositeInstance(t)
	applyMultiWindowConstantLoadDistribution(t, ti, defaultTestTenantKey, 8)
	res = ti.Instance.submitUntil(defaultTestTenantKey, 5, time.Duration(-1)*time.Second)

	assert.NotNil(t, res.Error)
	assert.ErrorIs(t, res.Error, ErrLoadRequestRejected)
	assert.Contains(t, res.Error.Error(), "invalid timeout")
	assert.Equal(t, uint64(0), res.AttemptsNumber)
	assert.Equal(t, time.Duration(0), res.WaitedFor)
}
