package goll

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	defaultMaxLoad       = 100
	defaultWindowSize    = time.Duration(10) * time.Second
	defaultSegmentSize   = time.Second
	defaultTestTenantKey = "test"
)

type genericTestableInstance interface {
	LimiterInstance() LoadLimiter
	TimeTravel(diff int64)
	TimeSet(to uint64)
	AssertCurrentTime(t *testing.T, expected uint64)
	AssertWindowStatus(t *testing.T, tenantKey string, load interface{}, segmentsHash ...string)
}

type testableInstance struct {
	Instance    *loadLimiterDefaultImpl
	CurrentTime uint64
}

type compositeTestableInstance struct {
	Instance    *compositeLoadLimiterDefaultImpl
	CurrentTime uint64
}

type testLogger struct {
	Messages []string
}

func (l *testLogger) Debug(text string) {
	l.Messages = append(l.Messages, fmt.Sprintf("[d] %v", text))
}
func (l *testLogger) Info(text string) {
	l.Messages = append(l.Messages, fmt.Sprintf("[i] %v", text))
}
func (l *testLogger) Warning(text string) {
	l.Messages = append(l.Messages, fmt.Sprintf("[w] %v", text))
}
func (l *testLogger) Error(text string) {
	l.Messages = append(l.Messages, fmt.Sprintf("[e] %v", text))
}

func (ti *testableInstance) LimiterInstance() LoadLimiter {
	return ti.Instance
}
func (ti *testableInstance) TimeSet(to uint64) {
	ti.CurrentTime = to
}
func (ti *testableInstance) TimeTravel(diff int64) {
	ti.CurrentTime = uint64(int64(ti.CurrentTime) + diff)
}
func (ti *testableInstance) AssertCurrentTime(t *testing.T, expected uint64) {
	assert.Equal(t, uint64(expected), ti.CurrentTime, "the current time is expected to be %v and is instead %v", expected, ti.CurrentTime)
}
func (ti *testableInstance) HashSegments(tenantKey string) string {
	out := ""
	tenant := ti.Instance.getTenant(tenantKey)
	for i := 0; i < tenant.WindowQueue.Len(); i++ {

		el := tenant.WindowQueue.At(i).(*windowSegment)
		out = out + fmt.Sprintf("%v:%v, ", el.StartTime, el.Value)
	}
	if len(out) > 0 {
		out = strings.TrimRight(out, ", ")
	}
	return out
}
func (ti *testableInstance) AssertWindowStatus(t *testing.T, tenantKey string, load interface{}, segmentsHash ...string) {
	tenant := ti.Instance.getTenant(tenantKey)
	loadConv := toUint64(load)
	assert.Equal(t, loadConv, tenant.WindowTotal)
	assert.Equal(t, strings.Join(segmentsHash, ", "), ti.HashSegments(tenantKey))
}
func (ti *testableInstance) InternalRequest(tenantKey string, load uint64) *submitRequest {
	return ti.Instance.buildLoadRequest(
		ti.Instance.TimeFunc(), tenantKey, load,
	)
}

func buildInstance(t *testing.T, configurer func(config *Config)) *testableInstance {
	ti := testableInstance{
		CurrentTime: 1000000,
	}

	timeFunc := func() time.Time {
		return time.Unix(
			int64(ti.CurrentTime)/int64(1000),
			(int64(ti.CurrentTime)%int64(1000))*int64(1000000),
		)
	}

	sleepFunc := func(d time.Duration) {
		newTime := ti.CurrentTime + uint64(d.Milliseconds())
		fmt.Printf("testable instance is waiting from %v to %v\n", ti.CurrentTime, newTime)
		ti.CurrentTime = newTime
	}

	config := Config{
		MaxLoad:           defaultMaxLoad,
		WindowSize:        defaultWindowSize,
		WindowSegmentSize: defaultSegmentSize,
		TimeFunc:          timeFunc,
		SleepFunc:         sleepFunc,
	}

	if configurer != nil {
		configurer(&config)
	}

	instance, err := New(&config)

	if t != nil {
		assert.NotNil(t, instance)
		assert.Nil(t, err)
	}

	ti.Instance = instance.(*loadLimiterDefaultImpl)

	return &ti
}

func buildDefaultInstance(t *testing.T) *testableInstance {
	return buildInstance(t, nil)
}

func applySingleWindowLoadDistribution(t *testing.T, ti *testableInstance, tenantKey string) {
	// build some starting load distribution

	// starting segment (0/10 - 1000000)
	_, _ = ti.Instance.Submit(tenantKey, 5)

	ti.TimeTravel(1000) // next segment (1/10 - 1001000)
	_, _ = ti.Instance.Submit(tenantKey, 10)

	ti.TimeTravel(1000) // next segment (2/10 - 1002000)
	_, _ = ti.Instance.Submit(tenantKey, 20)

	// skip segment (3/10 - 1003000)

	ti.TimeTravel(2000) // next segment (4/10 - 1004000)
	_, _ = ti.Instance.Submit(tenantKey, 8)

	ti.TimeTravel(1000) // next segment (5/10 - 1005000)
	_, _ = ti.Instance.Submit(tenantKey, 15)

	// skip segment (6/10 - 1006000)
	// skip segment (7/10 - 1007000)

	ti.TimeTravel(3000) // skip two segments (8/10 - 1008000)
	_, _ = ti.Instance.Submit(tenantKey, 10)
	_, _ = ti.Instance.Submit(tenantKey, 4)

	// skip segment (9/10 - 1009000)
	ti.TimeTravel(1000)

	ti.AssertWindowStatus(
		t,
		tenantKey,
		72,
		"1008000:14",
		"1005000:15",
		"1004000:8",
		"1002000:20",
		"1001000:10",
		"1000000:5",
	)
}

func applyMultiWindowConstantLoadDistribution(t *testing.T, ti genericTestableInstance, tenantKey string, perSegment uint64) {

	for i := 0; i < 20; i++ {
		if i > 0 {
			ti.TimeTravel(1000)
		}

		res := ti.LimiterInstance().SubmitUntilWithDetails(tenantKey, perSegment, time.Duration(0))

		assert.Nil(t, res.Error)
		assert.Equal(t, uint64(1), res.AttemptsNumber)
		assert.Equal(t, time.Duration(0), res.WaitedFor)
	}

	ti.AssertCurrentTime(t, 1019000)

	pss := fmt.Sprintf("%v", perSegment)

	isSingle := !ti.LimiterInstance().IsComposite()
	if isSingle {
		ti.AssertWindowStatus(
			t, tenantKey, perSegment*10,
			"1019000:"+pss, "1018000:"+pss, "1017000:"+pss, "1016000:"+pss,
			"1015000:"+pss, "1014000:"+pss, "1013000:"+pss, "1012000:"+pss,
			"1011000:"+pss, "1010000:"+pss,
		)
	}
}

func toUint64(any interface{}) uint64 {
	switch v := any.(type) {
	case uint64:
		return v
	case int:
		return uint64(v)
	default:
		panic("invalid type could not be converted to uint64")
	}
}

func (ti *compositeTestableInstance) LimiterInstance() LoadLimiter {
	return ti.Instance
}
func (ti *compositeTestableInstance) TimeSet(to uint64) {
	ti.CurrentTime = to
}
func (ti *compositeTestableInstance) TimeTravel(diff int64) {
	ti.CurrentTime = uint64(int64(ti.CurrentTime) + diff)
}
func (ti *compositeTestableInstance) AssertCurrentTime(t *testing.T, expected uint64) {
	assert.Equal(t, uint64(expected), ti.CurrentTime, "the current time is expected to be %v and is instead %v", expected, ti.CurrentTime)
}
func (ti *compositeTestableInstance) HashSegments(tenantKey string) string {
	out := ""
	for limiterIndex := 0; limiterIndex < len(ti.Instance.Limiters); limiterIndex++ {
		instance := ti.Instance.Limiters[limiterIndex]
		q := instance.getTenant(tenantKey).WindowQueue
		for i := 0; i < q.Len(); i++ {

			el := q.At(i).(*windowSegment)
			out = out + fmt.Sprintf("%d:%v:%v, ", limiterIndex, el.StartTime, el.Value)
		}
	}

	if len(out) > 0 {
		out = strings.TrimRight(out, ", ")
	}
	return out
}
func (ti *compositeTestableInstance) AssertWindowStatus(t *testing.T, tenantKey string, load interface{}, segmentsHash ...string) {
	num := len(ti.Instance.Limiters)
	loadConv, isMultiple := load.([]uint64)
	if !isMultiple {
		loadConv = make([]uint64, num)
		for i := 0; i < num; i++ {
			loadConv[i] = toUint64(load)
		}
	}

	for limiterIndex := 0; limiterIndex < num; limiterIndex++ {
		assert.Equal(t, loadConv[limiterIndex], ti.Instance.Limiters[limiterIndex].getTenant(tenantKey).WindowTotal)
	}
	assert.Equal(t, strings.Join(segmentsHash, ", "), ti.HashSegments(tenantKey))
}
func (ti *compositeTestableInstance) InternalRequest(load uint64) *submitRequest {
	return &submitRequest{
		RequestedLoad:      load,
		RequestedTimestamp: ti.CurrentTime,
	}
}

func buildCompositeInstance(t *testing.T, configurer func(config *CompositeConfig)) *compositeTestableInstance {
	ti := compositeTestableInstance{
		CurrentTime: 1000000,
	}

	timeFunc := func() time.Time {
		return time.Unix(
			int64(ti.CurrentTime)/int64(1000),
			(int64(ti.CurrentTime)%int64(1000))*int64(1000000),
		)
	}

	sleepFunc := func(d time.Duration) {
		newTime := ti.CurrentTime + uint64(d.Milliseconds())
		fmt.Printf("composite testable instance is waiting from %v to %v\n", ti.CurrentTime, newTime)
		ti.CurrentTime = newTime
	}

	config := CompositeConfig{
		Limiters: []Config{
			{
				MaxLoad:           defaultMaxLoad,
				WindowSize:        defaultWindowSize,
				WindowSegmentSize: defaultSegmentSize,
			},
			{
				MaxLoad:           defaultMaxLoad / 10 * 2,
				WindowSize:        defaultWindowSize / 10,
				WindowSegmentSize: defaultSegmentSize / 10,
			},
		},
		TimeFunc:  timeFunc,
		SleepFunc: sleepFunc,
	}

	if configurer != nil {
		configurer(&config)
	}

	instance, err := NewComposite(&config)

	if t != nil {
		assert.NotNil(t, instance)
		assert.Nil(t, err)
	}

	ti.Instance = instance.(*compositeLoadLimiterDefaultImpl)

	return &ti
}

func buildDefaultCompositeInstance(t *testing.T) *compositeTestableInstance {
	return buildCompositeInstance(t, nil)
}

func noErrors(args ...interface{}) interface{} {
	for _, a := range args {
		if a == nil {
			continue
		}
		if err, isErr := a.(error); isErr {
			panic(fmt.Errorf("no errors were expected but had an error: %w", err))
		}
	}
	return args[0]
}

func submitNoError(args ...interface{}) SubmitResult {
	noErrors(args)
	return args[0].(SubmitResult)
}
