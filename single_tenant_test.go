package goll

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSingleTenantConversionFailsWithInvalidTenantKeys(t *testing.T) {

	ti := buildDefaultInstance(t)
	cti := buildDefaultCompositeInstance(t)

	assert.Panics(t, func() {
		_ = ti.Instance.ForTenant("")
	})
	assert.Panics(t, func() {
		_ = ti.Instance.ForTenant("   ")
	})
	assert.Panics(t, func() {
		_ = ti.Instance.ForTenant(singleTenantDefaultKey)
	})

	assert.Panics(t, func() {
		_ = cti.Instance.ForTenant("")
	})
	assert.Panics(t, func() {
		_ = cti.Instance.ForTenant("   ")
	})
	assert.Panics(t, func() {
		_ = cti.Instance.ForTenant(singleTenantDefaultKey)
	})
}

func TestSingleTenantSubmit(t *testing.T) {
	ti := buildDefaultInstance(t)
	instance := ti.Instance.AsSingleTenant()

	assert.False(t, instance.IsComposite())

	submitResult := submitNoError(instance.Submit(10))

	assert.NotNil(t, submitResult)
	assert.True(t, submitResult.Accepted)
	assert.False(t, submitResult.RetryInAvailable)
	assert.Zero(t, submitResult.RetryIn)

	assert.Contains(t, strings.ToLower(submitResult.String()), "accepted")

	assert.True(t, noErrors(instance.Probe(90)).(bool))

	assert.True(t, submitNoError(instance.Submit(90)).Accepted)

	rejected := submitNoError(instance.Submit(1))
	assert.False(t, rejected.Accepted)
	assert.True(t, rejected.RetryInAvailable)
	assert.Contains(t, strings.ToLower(rejected.String()), "rejected")
	assert.Contains(t, strings.ToLower(rejected.String()), "retry")

	rejected = submitNoError(instance.Submit(99999))
	assert.False(t, rejected.Accepted)
	assert.False(t, rejected.RetryInAvailable)
	assert.Contains(t, strings.ToLower(rejected.String()), "rejected")
	assert.NotContains(t, strings.ToLower(rejected.String()), "retry")

	assert.False(t, noErrors(instance.Probe(1)).(bool))

	ti.TimeTravel(defaultWindowSize.Milliseconds())

	assert.True(t, noErrors(instance.Probe(1)).(bool))

	assert.True(t, submitNoError(instance.Submit(1)).Accepted)
}

func TestSingleTenantSubmitUntilWithDetails(t *testing.T) {
	ti := buildDefaultInstance(t)
	applyMultiWindowConstantLoadDistribution(t, ti, defaultTestTenantKey, 8)
	instance := ti.Instance.ForTenant(defaultTestTenantKey)

	// load not available, timeout 1ms. We expect to timeout with an ErrLoadRequestTimeout
	// but WaitedFor should be 0 (should not wait when it would be pointless to)
	res := instance.SubmitUntilWithDetails(40, time.Duration(1)*time.Millisecond)

	assert.NotNil(t, res.Error)
	assert.ErrorIs(t, res.Error, ErrLoadRequestTimeout)
	assert.Contains(t, res.Error.Error(), "timed out")
	assert.Equal(t, uint64(1), res.AttemptsNumber)
	assert.Equal(t, time.Duration(0), res.WaitedFor)

	// goto 1019200
	ti.TimeTravel(200)

	// 20 currently available, asking for 40, 8 in each segment.
	// to free up three segments we have to wait 800 + 1000 + 1000 ms
	res = instance.SubmitUntilWithDetails(40, time.Duration(10000)*time.Millisecond)

	assert.Nil(t, res.Error)
	assert.Equal(t, uint64(2), res.AttemptsNumber)
	assert.Equal(t, int64(2800), res.WaitedFor.Milliseconds())
}

func TestSingleTenantStats(t *testing.T) {
	ti := buildDefaultInstance(t)
	instance := ti.Instance.ForTenant(defaultTestTenantKey)

	// start at t = 1000000
	assert.Equal(t, "", ti.HashSegments(defaultTestTenantKey))

	assert.True(t, submitNoError(instance.Submit(10)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 10, "1000000:10")

	stats, err := instance.Stats()
	assert.Nil(t, err)
	assert.Equal(t, RuntimeStatistics{
		WindowTotal:    uint64(10),
		WindowSegments: []uint64{10},
	}, stats)

	ti.TimeTravel(500) // goto 1000500
	assert.True(t, submitNoError(instance.Submit(10)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 20, "1000000:20")

	stats, err = instance.Stats()
	assert.Nil(t, err)
	assert.Equal(t, RuntimeStatistics{
		WindowTotal:    uint64(20),
		WindowSegments: []uint64{20},
	}, stats)

	ti.TimeTravel(500) // goto 1001000
	assert.True(t, submitNoError(instance.Submit(30)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 50, "1001000:30", "1000000:20")

	stats, err = instance.Stats()
	assert.Nil(t, err)
	assert.Equal(t, RuntimeStatistics{
		WindowTotal:    uint64(50),
		WindowSegments: []uint64{30, 20},
	}, stats)

	ti.TimeTravel(999) // goto 1001999
	assert.True(t, submitNoError(instance.Submit(5)).Accepted)
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

	stats, err = instance.Stats()
	assert.Nil(t, err)
	assert.Equal(t, RuntimeStatistics{
		WindowTotal:    uint64(0),
		WindowSegments: []uint64{0, 0, 0},
	}, stats)

}

func TestSingleTenantCompositeBasics(t *testing.T) {
	ti := buildDefaultCompositeInstance(t)
	instance := ti.Instance.ForTenant(defaultTestTenantKey)

	assert.True(t, instance.IsComposite())

	assert.NotNil(t, ti.Instance)

	assert.True(t, noErrors(instance.Probe(1)).(bool))

	assert.True(t, submitNoError(instance.Submit(5)).Accepted)
	assert.True(t, submitNoError(instance.Submit(5)).Accepted)
	assert.True(t, submitNoError(instance.Submit(5)).Accepted)
	assert.True(t, submitNoError(instance.Submit(5)).Accepted)

	assert.False(t, noErrors(instance.Probe(1)).(bool))

	rejected := submitNoError(instance.Submit(1))
	assert.False(t, rejected.Accepted)
	assert.True(t, rejected.RetryInAvailable)
	assert.Equal(t, int64(1000), rejected.RetryIn.Milliseconds())

	// submitted 20 in the first segment, now wait
	stats, err := instance.Stats()
	assert.Nil(t, err)

	assert.Equal(t, uint64(20), stats.LimitersStats[0].WindowTotal)

	assert.Equal(t, uint64(20), stats.LimitersStats[1].WindowTotal)

	ti.AssertWindowStatus(t, defaultTestTenantKey, []uint64{20, 20}, "0:1000000:20", "1:1000000:20")

	ti.TimeTravel(1000)
	_, _ = instance.Probe(0)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, []uint64{20, 0},
		"0:1001000:0", "0:1000000:20",
		"1:1001000:0",
	)
}

func TestSingleTenantCompositeSubmitUntil(t *testing.T) {
	ti := buildDefaultCompositeInstance(t)
	instance := ti.Instance.ForTenant(defaultTestTenantKey)
	applyMultiWindowConstantLoadDistribution(t, ti, defaultTestTenantKey, 9)

	// load not available, timeout 1ms. We expect to timeout with an ErrLoadRequestTimeout
	// but WaitedFor should be 0 (should not wait when it would be pointless to)
	res := instance.SubmitUntilWithDetails(15, 1*time.Millisecond)

	assert.NotNil(t, res.Error)
	assert.ErrorIs(t, res.Error, ErrLoadRequestTimeout)
	assert.Contains(t, res.Error.Error(), "timed out")
	assert.Equal(t, uint64(1), res.AttemptsNumber)
	assert.Equal(t, time.Duration(0), res.WaitedFor)

	// goto 1019200
	ti.TimeTravel(200)

	// 10 currently available, asking for 20, 9 in each segment.
	// to free up 2 segments we have to wait 800 + 1000 ms
	res = instance.SubmitUntilWithDetails(20, time.Duration(10000)*time.Millisecond)

	assert.Nil(t, res.Error)
	assert.Equal(t, uint64(2), res.AttemptsNumber)
	assert.Equal(t, int64(1800), res.WaitedFor.Milliseconds())
}
