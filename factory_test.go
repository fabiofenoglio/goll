package goll

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestInterfacesAreCorrectlyImplemented(t *testing.T) {

	isLoadLimiter := func(i LoadLimiter) {}
	isStandaloneLoadLimiter := func(i StandaloneLoadLimiter) {}
	isCompositeLoadLimiter := func(i CompositeLoadLimiter) {}
	isSingleTenantLoadLimiter := func(i SingleTenantLoadLimiter) {}
	isSingleTenantStandaloneLoadLimiter := func(i SingleTenantStandaloneLoadLimiter) {}
	isSingleTenantCompositeLoadLimiter := func(i SingleTenantCompositeLoadLimiter) {}

	standaloneInstance, _ := New(&Config{
		MaxLoad:           100,
		WindowSize:        1 * time.Minute,
		WindowSegmentSize: 1 * time.Second,
	})

	compositeInstance, _ := NewComposite(&CompositeConfig{
		Limiters: []Config{
			{
				MaxLoad:           100,
				WindowSize:        1 * time.Minute,
				WindowSegmentSize: 1 * time.Second,
			},
		},
	})

	isStandaloneLoadLimiter(standaloneInstance)
	isLoadLimiter(standaloneInstance)

	isCompositeLoadLimiter(compositeInstance)
	isLoadLimiter(standaloneInstance)

	standaloneSingle := standaloneInstance.AsSingleTenant()
	isSingleTenantLoadLimiter(standaloneSingle)
	isSingleTenantStandaloneLoadLimiter(standaloneSingle)

	compositeSingle := compositeInstance.AsSingleTenant()
	isSingleTenantLoadLimiter(compositeSingle)
	isSingleTenantCompositeLoadLimiter(compositeSingle)
}

func TestFactoryBuilderWithMinimalParams(t *testing.T) {
	instance, err := New(&Config{
		MaxLoad:    1000,
		WindowSize: time.Duration(60) * time.Second,
	})

	assert.Nil(t, err)
	assert.NotNil(t, instance)
}

func TestValidateConfigurationWithOverstepPenalty(t *testing.T) {

	// with penalty factor only
	parsed, err := validateConfiguration(&Config{
		MaxLoad:               1000,
		WindowSize:            time.Duration(60) * time.Second,
		WindowSegmentSize:     time.Duration(5) * time.Second,
		OverstepPenaltyFactor: 0.2,
	}, nil)
	assert.Nil(t, err)

	assert.Equal(t, uint64(12), parsed.NumSegments)
	assert.True(t, parsed.ApplyOverstepPenalty)
	assert.Equal(t, uint64(200), parsed.AbsoluteOverstepPenalty)
	assert.Equal(t, uint64(1), parsed.OverstepPenaltySegmentSpan)

	// now with specific distribution factor
	parsed, err = validateConfiguration(&Config{
		MaxLoad:                           1000,
		WindowSize:                        time.Duration(60) * time.Second,
		WindowSegmentSize:                 time.Duration(5) * time.Second,
		OverstepPenaltyFactor:             0.1,
		OverstepPenaltyDistributionFactor: 0.33,
	}, nil)

	assert.Nil(t, err)

	assert.Equal(t, uint64(12), parsed.NumSegments)
	assert.True(t, parsed.ApplyOverstepPenalty)
	assert.Equal(t, uint64(100), parsed.AbsoluteOverstepPenalty)
	assert.Equal(t, uint64(4), parsed.OverstepPenaltySegmentSpan)

	// now with a distribution factor too small to cover one segment.
	// should fallback to one segment.
	parsed, err = validateConfiguration(&Config{
		MaxLoad:                           1000,
		WindowSize:                        time.Duration(60) * time.Second,
		WindowSegmentSize:                 time.Duration(5) * time.Second,
		OverstepPenaltyFactor:             0.1,
		OverstepPenaltyDistributionFactor: 0.00001,
	}, nil)

	assert.Nil(t, err)

	assert.Equal(t, uint64(12), parsed.NumSegments)
	assert.True(t, parsed.ApplyOverstepPenalty)
	assert.Equal(t, uint64(100), parsed.AbsoluteOverstepPenalty)
	assert.Equal(t, uint64(1), parsed.OverstepPenaltySegmentSpan)
}

func TestValidateConfigurationWithRequestOverheadPenalty(t *testing.T) {

	// with penalty factor only
	parsed, err := validateConfiguration(&Config{
		MaxLoad:                      1000,
		WindowSize:                   time.Duration(60) * time.Second,
		WindowSegmentSize:            time.Duration(5) * time.Second,
		RequestOverheadPenaltyFactor: 0.2,
	}, nil)
	assert.Nil(t, err)

	assert.Equal(t, uint64(12), parsed.NumSegments)
	assert.True(t, parsed.ApplyRequestOverheadPenalty)
	assert.Equal(t, float64(0.2), parsed.RequestOverheadPenaltyFactor)
	assert.Equal(t, uint64(1), parsed.RequestOverheadPenaltySegmentSpan)

	// now with specific distribution factor
	parsed, err = validateConfiguration(&Config{
		MaxLoad:                                  1000,
		WindowSize:                               time.Duration(60) * time.Second,
		WindowSegmentSize:                        time.Duration(5) * time.Second,
		RequestOverheadPenaltyFactor:             0.1,
		RequestOverheadPenaltyDistributionFactor: 0.33,
	}, nil)

	assert.Nil(t, err)

	assert.Equal(t, uint64(12), parsed.NumSegments)
	assert.True(t, parsed.ApplyRequestOverheadPenalty)
	assert.Equal(t, float64(0.1), parsed.RequestOverheadPenaltyFactor)
	assert.Equal(t, uint64(4), parsed.RequestOverheadPenaltySegmentSpan)

	// now with a distribution factor too small to cover one segment.
	// should fallback to one segment.
	parsed, err = validateConfiguration(&Config{
		MaxLoad:                                  1000,
		WindowSize:                               time.Duration(60) * time.Second,
		WindowSegmentSize:                        time.Duration(5) * time.Second,
		RequestOverheadPenaltyFactor:             0.1,
		RequestOverheadPenaltyDistributionFactor: 0.00001,
	}, nil)

	assert.Nil(t, err)

	assert.Equal(t, uint64(12), parsed.NumSegments)
	assert.True(t, parsed.ApplyRequestOverheadPenalty)
	assert.Equal(t, float64(0.1), parsed.RequestOverheadPenaltyFactor)
	assert.Equal(t, uint64(1), parsed.RequestOverheadPenaltySegmentSpan)
}

func TestValidateConfiguration(t *testing.T) {
	parsed, err := validateConfiguration(&Config{
		MaxLoad:           1000,
		WindowSize:        time.Duration(60) * time.Second,
		WindowSegmentSize: time.Duration(5) * time.Second,
	}, nil)

	assert.Nil(t, err)

	assert.False(t, parsed.ApplyOverstepPenalty)

	assert.False(t, parsed.ApplyRequestOverheadPenalty)
	assert.False(t, parsed.SkipRetryInComputing)

	expectFailure(t, &Config{}, "MaxLoad")
	expectFailure(t, &Config{
		MaxLoad: 0,
	}, "MaxLoad")
	expectFailure(t, &Config{
		MaxLoad: 100,
	}, "WindowSize")
	expectFailure(t, &Config{
		MaxLoad:           100,
		WindowSize:        time.Second,
		WindowSegmentSize: 1,
	}, "WindowSegmentSize")
	expectFailure(t, &Config{
		MaxLoad:           100,
		WindowSize:        time.Second,
		WindowSegmentSize: 1001 * time.Millisecond,
	}, "WindowSegmentSize")
	expectFailure(t, &Config{
		MaxLoad:    100,
		WindowSize: 131 * time.Millisecond,
	}, "windowSize is not exactly divisible in segments")
	expectFailure(t, &Config{
		MaxLoad:    100,
		WindowSize: 1 * time.Millisecond,
	}, "windowSize is not exactly divisible in segments")
	expectFailure(t, &Config{
		MaxLoad:           100,
		WindowSize:        time.Second,
		WindowSegmentSize: 2 * time.Second,
	}, "WindowSegmentSize")
	expectFailure(t, &Config{
		MaxLoad:           100,
		WindowSize:        time.Second,
		WindowSegmentSize: time.Duration(1001) * time.Millisecond,
	}, "WindowSegmentSize")
	expectFailure(t, &Config{
		MaxLoad:           100,
		WindowSize:        time.Second,
		WindowSegmentSize: time.Duration(501) * time.Millisecond,
	}, "WindowSegmentSize")
	expectFailure(t, &Config{
		MaxLoad:             100,
		WindowSize:          time.Second,
		WindowSegmentSize:   time.Duration(100) * time.Millisecond,
		MaxPenaltyCapFactor: -0.1,
	}, "MaxPenaltyCapFactor")
	expectFailure(t, &Config{
		MaxLoad:               100,
		WindowSize:            time.Second,
		WindowSegmentSize:     time.Duration(100) * time.Millisecond,
		OverstepPenaltyFactor: -0.1,
	}, "OverstepPenaltyFactor")
	expectFailure(t, &Config{
		MaxLoad:                           100,
		WindowSize:                        time.Second,
		WindowSegmentSize:                 time.Duration(100) * time.Millisecond,
		OverstepPenaltyFactor:             0.2,
		OverstepPenaltyDistributionFactor: -0.1,
	}, "OverstepPenaltyDistributionFactor")
	expectFailure(t, &Config{
		MaxLoad:                           100,
		WindowSize:                        time.Second,
		WindowSegmentSize:                 time.Duration(100) * time.Millisecond,
		OverstepPenaltyFactor:             0.2,
		OverstepPenaltyDistributionFactor: 1.01,
	}, "OverstepPenaltyDistributionFactor")
	expectFailure(t, &Config{
		MaxLoad:                      100,
		WindowSize:                   time.Second,
		WindowSegmentSize:            time.Duration(100) * time.Millisecond,
		RequestOverheadPenaltyFactor: -0.1,
	}, "RequestOverheadPenaltyFactor")
	expectFailure(t, &Config{
		MaxLoad:                                  100,
		WindowSize:                               time.Second,
		WindowSegmentSize:                        time.Duration(100) * time.Millisecond,
		RequestOverheadPenaltyFactor:             0.2,
		RequestOverheadPenaltyDistributionFactor: -0.1,
	}, "RequestOverheadPenaltyDistributionFactor")
	expectFailure(t, &Config{
		MaxLoad:                                  100,
		WindowSize:                               time.Second,
		WindowSegmentSize:                        time.Duration(100) * time.Millisecond,
		RequestOverheadPenaltyFactor:             0.2,
		RequestOverheadPenaltyDistributionFactor: 1.1,
	}, "RequestOverheadPenaltyDistributionFactor")
}

func TestFactoryBuilderHandlesPenaltyCapDefault(t *testing.T) {
	// build the instance with no value
	parsed, err := validateConfiguration(&Config{
		MaxLoad:           1000,
		WindowSize:        time.Duration(60) * time.Second,
		WindowSegmentSize: time.Duration(5) * time.Second,
	}, nil)

	assert.Nil(t, err)

	// check capping has the default value
	assert.True(t, parsed.ApplyPenaltyCapping)
	assert.Equal(t, uint64(1500), parsed.AbsoluteMaxPenaltyCap)

	// now build with specific value
	parsed, err = validateConfiguration(&Config{
		MaxLoad:             1000,
		WindowSize:          time.Duration(60) * time.Second,
		WindowSegmentSize:   time.Duration(5) * time.Second,
		MaxPenaltyCapFactor: 0.10,
	}, nil)

	assert.Nil(t, err)
	assert.True(t, parsed.ApplyPenaltyCapping)
	assert.Equal(t, uint64(1100), parsed.AbsoluteMaxPenaltyCap)
}

func TestFactoryBuilderAcceptsCustomLogger(t *testing.T) {

	customLoggerInstance := testLogger{
		Messages: make([]string, 0),
	}

	instance, err := New(&Config{
		MaxLoad:           1000,
		WindowSize:        time.Duration(60) * time.Second,
		WindowSegmentSize: time.Duration(5) * time.Second,
		Logger:            &customLoggerInstance,
	})

	assert.Nil(t, err)
	assert.NotNil(t, instance)

	typedInstance := instance.(*loadLimiterDefaultImpl)
	assert.Same(t, &customLoggerInstance, typedInstance.Logger)
	assert.NotEmpty(t, customLoggerInstance.Messages)

	// expectations are low
	typedInstance.Logger.Debug("logger does not die on direct usage")
	typedInstance.Logger.Info("logger does not die on direct usage")
	typedInstance.Logger.Warning("logger does not die on direct usage")
	typedInstance.Logger.Error("logger does not die on direct usage")
}

func TestCompositeFactoryBuilderMiminalParameters(t *testing.T) {
	instance, err := NewComposite(
		&CompositeConfig{
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
		})

	assert.Nil(t, err)
	assert.NotNil(t, instance)
}

func TestCompositeFactoryBuilderAcceptsCustomLogger(t *testing.T) {

	customLoggerInstance := testLogger{
		Messages: make([]string, 0),
	}

	instance, err := NewComposite(&CompositeConfig{
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
		Logger: &customLoggerInstance,
	})

	assert.Nil(t, err)
	assert.NotNil(t, instance)

	typedInstance := instance.(*compositeLoadLimiterDefaultImpl)
	assert.Same(t, &customLoggerInstance, typedInstance.Logger)
	assert.NotEmpty(t, customLoggerInstance.Messages)

	// expectations are low
	typedInstance.Logger.Debug("logger does not die on direct usage")
	typedInstance.Logger.Info("logger does not die on direct usage")
	typedInstance.Logger.Warning("logger does not die on direct usage")
	typedInstance.Logger.Error("logger does not die on direct usage")
}

func TestValidateCompositeConfiguration(t *testing.T) {
	parsed, err := validateCompositeConfiguration(&CompositeConfig{
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
	}, nil)

	assert.Nil(t, err)
	assert.NotNil(t, parsed)

	expectCompositeFailure(t, &CompositeConfig{}, "at least one")
	expectCompositeFailure(t, &CompositeConfig{
		Limiters: []Config{
			{},
		},
	}, "at index 0: MaxLoad")
	expectCompositeFailure(t, &CompositeConfig{
		Limiters: []Config{
			{

				MaxLoad:           defaultMaxLoad,
				WindowSize:        defaultWindowSize,
				WindowSegmentSize: defaultSegmentSize,
			},
			{
				MaxLoad: defaultMaxLoad,
			},
		},
	}, "at index 1: WindowSize")
}

func expectFailure(t *testing.T, config *Config, message string) {
	instance, err := New(config)

	assert.Nil(t, instance)
	assert.NotNil(t, err)

	assert.Contains(t, err.Error(), message)
}

func expectCompositeFailure(t *testing.T, config *CompositeConfig, message string) {
	instance, err := NewComposite(config)

	assert.Nil(t, instance)
	assert.NotNil(t, err)

	assert.Contains(t, err.Error(), message)
}
