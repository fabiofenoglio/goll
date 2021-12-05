package goll

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTimeMillis(t *testing.T) {
	ti := buildDefaultInstance(t)

	t1 := ti.CurrentTime
	assert.Equal(t, ti.CurrentTime, uint64(ti.Instance.currentTime().UnixMilli()))

	ti.TimeTravel(123)
	assert.Equal(t, t1+123, ti.CurrentTime)

	assert.Equal(t, t1+123, uint64(ti.Instance.currentTime().UnixMilli()))
}

func TestWindowRotation(t *testing.T) {
	ti := buildDefaultInstance(t)

	// start at t = 1000000
	assert.Equal(t, "", ti.HashSegments(defaultTestTenantKey))

	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 10)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 10, "1000000:10")

	ti.TimeTravel(500) // goto 1000500
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 10)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 20, "1000000:20")

	ti.TimeTravel(500) // goto 1001000
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 30)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 50, "1001000:30", "1000000:20")

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
}

func TestLocateSegmentStartTime(t *testing.T) {
	ti := buildDefaultInstance(t)

	assert.Equal(t, uint64(1000000), ti.Instance.locateSegmentStartTime(1000000))
	assert.Equal(t, uint64(1000000), ti.Instance.locateSegmentStartTime(1000999))
	assert.Equal(t, uint64(1001000), ti.Instance.locateSegmentStartTime(1001000))
	assert.Equal(t, uint64(1001000), ti.Instance.locateSegmentStartTime(1001999))
	assert.Equal(t, uint64(0), ti.Instance.locateSegmentStartTime(0))
	assert.Equal(t, uint64(0), ti.Instance.locateSegmentStartTime(1))
	assert.Equal(t, uint64(0), ti.Instance.locateSegmentStartTime(999))
	assert.Equal(t, uint64(1000), ti.Instance.locateSegmentStartTime(1000))
}

func TestEnsureLatestNSegments(t *testing.T) {
	// create an empty instance with no load
	ti := buildDefaultInstance(t)

	ti.Instance.ensureLatestNSegments(ti.InternalRequest(defaultTestTenantKey, 1), 0)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 0, "",
	)

	// re-create from scratch
	ti = buildDefaultInstance(t)
	ti.Instance.ensureLatestNSegments(ti.InternalRequest(defaultTestTenantKey, 1), 2)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 0,
		"1000000:0", "999000:0",
	)

	// re-create from scratch
	ti = buildDefaultInstance(t)
	_, _ = ti.Instance.Submit(defaultTestTenantKey, 10)
	ti.Instance.ensureLatestNSegments(ti.InternalRequest(defaultTestTenantKey, 1), 3)

	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 10,
		"1000000:10", "999000:0", "998000:0",
	)

	// re-create from scratch
	ti = buildDefaultInstance(t)
	_, _ = ti.Instance.Submit(defaultTestTenantKey, 10)
	ti.TimeTravel(3000)
	ti.Instance.ensureLatestNSegments(ti.InternalRequest(defaultTestTenantKey, 1), 1)

	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 10,
		"1003000:0", "1000000:10",
	)

	// re-create from scratch
	ti = buildDefaultInstance(t)
	_, _ = ti.Instance.Submit(defaultTestTenantKey, 10)
	ti.TimeTravel(3000)
	ti.Instance.ensureLatestNSegments(ti.InternalRequest(defaultTestTenantKey, 1), 4)

	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 10,
		"1003000:0", "1002000:0", "1001000:0", "1000000:10",
	)

	// re-create from scratch
	ti = buildDefaultInstance(t)
	ti.TimeTravel(5000)

	ti.Instance.ensureLatestNSegments(ti.InternalRequest(defaultTestTenantKey, 1), 3)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 0,
		"1005000:0", "1004000:0", "1003000:0",
	)

	// re-create from scratch
	ti = buildDefaultInstance(t)
	ti.Instance.rotateWindow(ti.InternalRequest(defaultTestTenantKey, 0))
	applySingleWindowLoadDistribution(t, ti, defaultTestTenantKey)

	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 72,
		"1008000:14", "1005000:15", "1004000:8", "1002000:20",
		"1001000:10", "1000000:5",
	)

	ti.Instance.ensureLatestNSegments(ti.InternalRequest(defaultTestTenantKey, 1), 7)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 72,
		"1009000:0", "1008000:14", "1007000:0", "1006000:0",
		"1005000:15", "1004000:8", "1003000:0", "1002000:20",
		"1001000:10", "1000000:5",
	)

	ti.Instance.ensureLatestNSegments(ti.InternalRequest(defaultTestTenantKey, 1), 3)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 72,
		"1009000:0", "1008000:14", "1007000:0", "1006000:0",
		"1005000:15", "1004000:8", "1003000:0", "1002000:20",
		"1001000:10", "1000000:5",
	)

	// re-create from scratch
	ti = buildDefaultInstance(t)
	applySingleWindowLoadDistribution(t, ti, defaultTestTenantKey)

	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 72,
		"1008000:14", "1005000:15", "1004000:8", "1002000:20",
		"1001000:10", "1000000:5",
	)

	// time is now 1009000
	assert.Equal(t, uint64(1009000), ti.CurrentTime)

	// wait 5000, time is now 1014000
	ti.TimeTravel(5000)
	assert.Equal(t, uint64(1014000), ti.CurrentTime)

	ti.Instance.ensureLatestNSegments(ti.InternalRequest(defaultTestTenantKey, 1), 4)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 72,
		"1014000:0", "1013000:0", "1012000:0", "1011000:0",
		"1008000:14", "1005000:15", "1004000:8", "1002000:20",
		"1001000:10", "1000000:5",
	)

	ti.Instance.rotateWindow(ti.InternalRequest(defaultTestTenantKey, 0))
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 29,
		"1014000:0", "1013000:0", "1012000:0", "1011000:0",
		"1008000:14", "1005000:15",
	)
}

func TestComputeRetryIn(t *testing.T) {
	ti := buildDefaultInstance(t)
	applySingleWindowLoadDistribution(t, ti, defaultTestTenantKey)

	// time is now 1009000
	assert.Equal(t, uint64(1009000), ti.CurrentTime)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 72,
		"1008000:14", "1005000:15", "1004000:8", "1002000:20",
		"1001000:10", "1000000:5",
	)

	// load is 72, probe for 28 should be ok but should fail for 29
	assert.True(t, noErrors(ti.Instance.Probe(defaultTestTenantKey, 28)).(bool))
	assert.False(t, noErrors(ti.Instance.Probe(defaultTestTenantKey, 29)).(bool))

	// compute RetryIn for 29
	retryIn, err := ti.Instance.computeRetryIn(ti.InternalRequest(defaultTestTenantKey, 29))
	assert.Nil(t, err)
	assert.Equal(t, int64(1000), retryIn.Milliseconds())

	ti.TimeTravel(300)
	retryIn, err = ti.Instance.computeRetryIn(ti.InternalRequest(defaultTestTenantKey, 29))
	assert.Nil(t, err)
	assert.Equal(t, int64(700), retryIn.Milliseconds())

	ti.TimeTravel(699)
	retryIn, err = ti.Instance.computeRetryIn(ti.InternalRequest(defaultTestTenantKey, 29))
	assert.Nil(t, err)
	assert.Equal(t, int64(1), retryIn.Milliseconds())

	ti.TimeTravel(1)
	assert.Equal(t, uint64(1010000), ti.CurrentTime)
	assert.True(t, noErrors(ti.Instance.Probe(defaultTestTenantKey, 33)).(bool))

	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 67,
		"1010000:0", "1009000:0", "1008000:14", "1005000:15", "1004000:8",
		"1002000:20", "1001000:10",
	)

	// We are now at time 1010000 and requiring retry time for a load of 70
	// to allow 70 the only segments left must be 1008000 and 1005000
	// the segment at 1004000 will disappear after 1004000 + 10000 = 1014000
	// so we expect 1014000 - 1010000 = 4000
	retryIn, err = ti.Instance.computeRetryIn(ti.InternalRequest(defaultTestTenantKey, 70))
	assert.Nil(t, err)
	assert.Equal(t, int64(4000), retryIn.Milliseconds())

	ti.TimeTravel(200)
	retryIn, err = ti.Instance.computeRetryIn(ti.InternalRequest(defaultTestTenantKey, 70))
	assert.Nil(t, err)
	assert.Equal(t, int64(3800), retryIn.Milliseconds())

	ti.TimeTravel(799)
	retryIn, err = ti.Instance.computeRetryIn(ti.InternalRequest(defaultTestTenantKey, 70))
	assert.Nil(t, err)
	assert.Equal(t, int64(3001), retryIn.Milliseconds())

	ti.TimeTravel(1)
	retryIn, err = ti.Instance.computeRetryIn(ti.InternalRequest(defaultTestTenantKey, 70))
	assert.Nil(t, err)
	assert.Equal(t, int64(3000), retryIn.Milliseconds())

	// get back to 1010000
	ti.TimeSet(1010000)

	// computing for all the load should give enough time for the window to clear completely
	_, _ = ti.Instance.Submit(defaultTestTenantKey, 10)

	retryIn, err = ti.Instance.computeRetryIn(ti.InternalRequest(defaultTestTenantKey, 100))
	assert.Nil(t, err)
	assert.Equal(t, int64(10000), retryIn.Milliseconds())

	ti.TimeTravel(900)

	retryIn, err = ti.Instance.computeRetryIn(ti.InternalRequest(defaultTestTenantKey, 100))
	assert.Nil(t, err)
	assert.Equal(t, int64(9100), retryIn.Milliseconds())

	// should not fail on negative numbers

	ti.TimeSet(1200000)
	_, _ = ti.Instance.Probe(defaultTestTenantKey, 0)

	ti.AssertWindowStatus(t, defaultTestTenantKey, 0, "1200000:0")

	retryIn, err = ti.Instance.computeRetryIn(ti.InternalRequest(defaultTestTenantKey, 30))
	assert.Nil(t, err)
	assert.Equal(t, time.Duration(0), retryIn)

}

func TestComputeRetryInCornerCases(t *testing.T) {
	// limiter status:
	//		windowTotal=1000
	//		segments=[
	//			17  69 123 145 200 147  52  93 115  39
	//		],
	//		38 requests processed
	// request for load of 27 was rejected, asked to wait for 10740 ms
	ti := buildInstance(t, func(config *Config) {
		config.MaxLoad = 1000
		config.WindowSize = 20 * time.Second
		config.WindowSegmentSize = 1 * time.Second
	})

	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 39)).Accepted)
	ti.TimeTravel(1000)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 115)).Accepted)
	ti.TimeTravel(1000)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 93)).Accepted)
	ti.TimeTravel(1000)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 52)).Accepted)
	ti.TimeTravel(1000)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 147)).Accepted)
	ti.TimeTravel(1000)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 200)).Accepted)
	ti.TimeTravel(1000)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 145)).Accepted)
	ti.TimeTravel(1000)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 123)).Accepted)
	ti.TimeTravel(1000)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 69)).Accepted)
	ti.TimeTravel(1000)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 17)).Accepted)

	res, err := ti.Instance.Submit(defaultTestTenantKey, 27)
	assert.Nil(t, err)
	assert.Equal(t, 11*time.Second, res.RetryIn)
}

func TestRemoveFromOldestSegments(t *testing.T) {
	ti := buildDefaultInstance(t)

	applySingleWindowLoadDistribution(t, ti, defaultTestTenantKey)

	// time is now 1009000
	assert.Equal(t, uint64(1009000), ti.CurrentTime)

	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 72,
		"1008000:14", "1005000:15", "1004000:8", "1002000:20",
		"1001000:10", "1000000:5",
	)

	// rotate to current time
	_, _ = ti.Instance.Probe(defaultTestTenantKey, 0)

	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 72,
		"1009000:0", "1008000:14", "1005000:15", "1004000:8",
		"1002000:20", "1001000:10", "1000000:5",
	)

	// go on one segment. this should make the segment '1000000:5' obsolete
	ti.TimeTravel(1000)

	// now remove from oldest
	r := ti.InternalRequest(defaultTestTenantKey, 0)
	ti.Instance.rotateWindow(r)
	ti.Instance.removeFromOldestSegments(r, 2)

	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 65,
		"1010000:0", "1009000:0", "1008000:14", "1005000:15",
		"1004000:8", "1002000:20", "1001000:8",
	)

	r = ti.InternalRequest(defaultTestTenantKey, 0)
	ti.Instance.rotateWindow(r)
	ti.Instance.removeFromOldestSegments(r, 4)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 61,
		"1010000:0", "1009000:0", "1008000:14", "1005000:15",
		"1004000:8", "1002000:20", "1001000:4",
	)

	r = ti.InternalRequest(defaultTestTenantKey, 0)
	ti.Instance.rotateWindow(r)
	ti.Instance.removeFromOldestSegments(r, 15)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 46,
		"1010000:0", "1009000:0", "1008000:14", "1005000:15",
		"1004000:8", "1002000:9",
	)

	r = ti.InternalRequest(defaultTestTenantKey, 0)
	ti.Instance.rotateWindow(r)
	ti.Instance.removeFromOldestSegments(r, 12)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 34,
		"1010000:0", "1009000:0", "1008000:14", "1005000:15",
		"1004000:5",
	)

	r = ti.InternalRequest(defaultTestTenantKey, 0)
	ti.Instance.rotateWindow(r)
	ti.Instance.removeFromOldestSegments(r, 0)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 34,
		"1010000:0", "1009000:0", "1008000:14", "1005000:15",
		"1004000:5",
	)

	r = ti.InternalRequest(defaultTestTenantKey, 0)
	ti.Instance.rotateWindow(r)
	ti.Instance.removeFromOldestSegments(r, 5)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 29,
		"1010000:0", "1009000:0", "1008000:14", "1005000:15",
	)

	r = ti.InternalRequest(defaultTestTenantKey, 0)
	ti.Instance.rotateWindow(r)
	ti.Instance.removeFromOldestSegments(r, 99999)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 0,
		"1010000:0",
	)
}

func TestDistributePenalty(t *testing.T) {
	// create an empty instance with no load
	ti := buildDefaultInstance(t)
	ti.TimeTravel(5000)

	ti.Instance.distributePenalty(ti.InternalRequest(defaultTestTenantKey, 0), 12, 3)

	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 12,
		"1005000:4", "1004000:4", "1003000:4",
	)

	ti.TimeTravel(4000)
	ti.Instance.distributePenalty(ti.InternalRequest(defaultTestTenantKey, 0), 8, 6)

	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 20,
		"1009000:2", "1008000:2", "1007000:1", "1006000:1",
		"1005000:5", "1004000:5", "1003000:4",
	)

	ti.Instance.distributePenalty(ti.InternalRequest(defaultTestTenantKey, 0), 3, 4)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 23,
		"1009000:3", "1008000:3", "1007000:2", "1006000:1",
		"1005000:5", "1004000:5", "1003000:4",
	)

	ti.Instance.distributePenalty(ti.InternalRequest(defaultTestTenantKey, 0), 2, 15)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 25,
		"1009000:4", "1008000:4", "1007000:2", "1006000:1",
		"1005000:5", "1004000:5", "1003000:4",
	)
}

func TestApplyCapping(t *testing.T) {
	ti := buildInstance(t, func(config *Config) {
		config.MaxPenaltyCapFactor = 0.40  // 0.40 * 100 -> 40
		config.OverstepPenaltyFactor = 0.9 // 0.90 * 100 -> 90
	})

	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 100)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 100, "1000000:100")

	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 1)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 140, "1000000:140")
}

func TestOverstepPenalty(t *testing.T) {
	// with no distribution factor (only last segment)
	ti := buildInstance(t, func(config *Config) {
		config.OverstepPenaltyFactor = 0.2 // 0.20 * 100 -> 20
	})

	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 100)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 100, "1000000:100")

	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 1)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 120, "1000000:120")

	// more requests should not add any load
	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 1)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 120, "1000000:120")

	ti.TimeTravel(2000)
	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 123)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 120, "1002000:0", "1000000:120")

	// with distribution factor of 0.5
	ti = buildInstance(t, func(config *Config) {
		config.OverstepPenaltyFactor = 0.22            // 0.22 * 100 -> 22
		config.OverstepPenaltyDistributionFactor = 0.5 // 0.5 of 10 segments = 5 segments
		// 22 / 5 segments = 5 5 4 4 4
	})
	ti.TimeTravel(30000)

	assert.Equal(t, uint64(10), ti.Instance.Config.NumSegments)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 100)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 100, "1030000:100")

	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 1)).Accepted)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 122,
		"1030000:105", "1029000:5", "1028000:4", "1027000:4", "1026000:4",
	)

	// with distribution factor of 0.00001
	ti = buildInstance(t, func(config *Config) {
		config.OverstepPenaltyFactor = 0.22 // 0.22 * 100 -> 22
		config.OverstepPenaltyDistributionFactor = 0.00001
	})
	ti.TimeTravel(30000)

	assert.Equal(t, uint64(10), ti.Instance.Config.NumSegments)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 100)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 100, "1030000:100")

	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 1)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 122, "1030000:122")

	// with distribution factor of 0.9999
	ti = buildInstance(t, func(config *Config) {
		config.OverstepPenaltyFactor = 0.22                // 0.22 * 100 -> 22
		config.OverstepPenaltyDistributionFactor = 0.99999 // should cover all segments
		// 22 / 10 segments = 3 for the first 2, 2 for the remaining 8
	})
	ti.TimeTravel(30000)

	assert.Equal(t, uint64(10), ti.Instance.Config.NumSegments)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 100)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 100, "1030000:100")

	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 1)).Accepted)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 122,
		"1030000:103", "1029000:3", "1028000:2", "1027000:2", "1026000:2",
		"1025000:2", "1024000:2", "1023000:2", "1022000:2", "1021000:2",
	)
}

func TestRequestOverheadPenalty(t *testing.T) {
	// with no distribution factor (only last segment)
	ti := buildInstance(t, func(config *Config) {
		config.RequestOverheadPenaltyFactor = 0.2
	})

	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 100)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 100, "1000000:100")

	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 1)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 100, "1000000:100")

	ti.TimeTravel(5000)

	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 20)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 104, "1005000:4", "1000000:100")

	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 31)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 110, "1005000:10", "1000000:100")

	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 29)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 116, "1005000:16", "1000000:100")

	// with distribution factor of 0.5
	ti = buildInstance(t, func(config *Config) {
		config.RequestOverheadPenaltyFactor = 0.37
		config.RequestOverheadPenaltyDistributionFactor = 0.5 // 0.5 of 10 segments = 5 segments
		// 22 / 5 segments = 5 5 4 4 4
	})
	ti.TimeTravel(30000)

	assert.Equal(t, uint64(10), ti.Instance.Config.NumSegments)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 100)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 100, "1030000:100")
	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 1)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 100, "1030000:100")

	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 30)).Accepted)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 111,
		"1030000:103", "1029000:2", "1028000:2", "1027000:2", "1026000:2",
	)

	// with distribution factor of 0.00001
	ti = buildInstance(t, func(config *Config) {
		config.RequestOverheadPenaltyFactor = 0.37
		config.RequestOverheadPenaltyDistributionFactor = 0.00001
	})
	ti.TimeTravel(30000)

	assert.Equal(t, uint64(10), ti.Instance.Config.NumSegments)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 100)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 100, "1030000:100")
	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 1)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 100, "1030000:100")

	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 30)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 111, "1030000:111")

	// with distribution factor of 0.9999
	ti = buildInstance(t, func(config *Config) {
		config.RequestOverheadPenaltyFactor = 0.33
		config.RequestOverheadPenaltyDistributionFactor = 0.9999
	})
	ti.TimeTravel(30000)

	assert.Equal(t, uint64(10), ti.Instance.Config.NumSegments)
	assert.True(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 100)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 100, "1030000:100")
	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 1)).Accepted)
	ti.AssertWindowStatus(t, defaultTestTenantKey, 100, "1030000:100")

	assert.False(t, submitNoError(ti.Instance.Submit(defaultTestTenantKey, 66)).Accepted)
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 122,
		"1030000:103", "1029000:3", "1028000:2", "1027000:2", "1026000:2",
		"1025000:2", "1024000:2", "1023000:2", "1022000:2", "1021000:2",
	)
}

func TestWindowRotationWithFutureSegments(t *testing.T) {
	// with no distribution factor (only last segment)
	ti := buildInstance(t, func(config *Config) {
		// NOP
	})

	applySingleWindowLoadDistribution(t, ti, defaultTestTenantKey)

	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 72,
		"1008000:14", "1005000:15", "1004000:8", "1002000:20",
		"1001000:10", "1000000:5",
	)

	ti.TimeSet(1004500)

	ti.Instance.rotateWindow(ti.InternalRequest(defaultTestTenantKey, 1))
	ti.AssertWindowStatus(
		t, defaultTestTenantKey, 72,
		"1004000:37", "1002000:20",
		"1001000:10", "1000000:5",
	)

}
