package goll

import (
	"errors"
	"fmt"
	"math"
	"time"
)

var (
	defaultMaxPenaltyCapFactor = 0.5
)

// Config holds the basic configuration for a load limiter instance
type Config struct {

	// MaxLoad is the absolute maximum amonut of load
	// that you want to allow in the specified time window.
	MaxLoad uint64

	// WindowSize is the width of the time window.
	// It heavily depends on the kind of limiter that you are setting up.
	//
	// WindowSize should be exactly divisible by WindowSegmentSize if the latter is specified.
	WindowSize time.Duration

	// WindowSegmentSize is the width of the segments that the
	// active window is divided in when shifting.
	//
	// The smaller the segment size, the smoother the limiting will be.
	// However, too small segments will increase memory and CPU overhead.
	//
	// WindowSize should be exactly divisible by WindowSegmentSize.
	//
	// When not specified, it is automatically assumed to be 1/20 of the WindowSize.
	WindowSegmentSize time.Duration

	// OverstepPenaltyFactor represents the multiplier applied to
	// the max load when the load limit gets reached.
	OverstepPenaltyFactor float64

	// OverstepPenaltyDistributionFactor must be in the range 0 - 1.0
	// and determines how widely the penalty configured by OverstepPenaltyFactor
	// is spread against the active time window
	OverstepPenaltyDistributionFactor float64

	// RequestOverheadPenaltyFactor represents the multiplier applied to
	// rejected load that gets applied as penalty load.
	RequestOverheadPenaltyFactor float64

	// RequestOverheadPenaltyDistributionFactor must be in the range 0 - 1.0
	// and determines how widely the penalty configured by RequestOverheadPenaltyFactor
	// is spread against the active time window
	RequestOverheadPenaltyDistributionFactor float64

	// MaxPenaltyCapFactor represents the max multiplier
	// applied for penalties.
	// If MaxPenaltyCapFactor > 0, the current load
	// will never be allowed to get penalized above (MaxLoad * (1.0 + MaxPenaltyCapFactor))
	// A good default value is usually in the range 0.30 - 0.50
	//
	// If not provided, a default value of 0.5 is assumed,
	// meaning the virtual load will cap at 150% of the maximum load
	// when unrestricted penalties are applied.
	MaxPenaltyCapFactor float64

	// if SkipRetryInComputing is true,
	// no RetryIn will be computed and RetryInAvailable will always be false.
	// Enable this if you don't need the RetryIn feature and want a slight
	// performance gain.
	SkipRetryInComputing bool

	// SyncAdapter is an implementation used to synchronize
	// the limiter data in a clustered environment.
	//
	// You can provide your own implementation.
	// You can use as example github.com/fabiofenoglio/goll-redis
	// which synchronizes data for multiple instances over a Redis cluster.
	SyncAdapter SyncAdapter

	// Time-related functions can be overriden to allow for easier testing
	// you should usually not override these.
	TimeFunc  func() time.Time
	SleepFunc func(d time.Duration)

	// you can pass your custom logger if you'd like to
	// but it's not required
	Logger Logger
}

type CompositeConfig struct {

	// Limiters is a required parameter holding the configurations
	// of the single limiters you want to compose together.
	Limiters []Config

	// SyncAdapter is an implementation used to synchronize
	// the limiter data in a clustered environment.
	//
	// You can provide your own implementation.
	// You can use as example github.com/fabiofenoglio/goll-redis
	// which synchronizes data for multiple instances over a Redis cluster.
	SyncAdapter SyncAdapter

	// Time-related functions can be overriden to allow for easier testing
	// you should usually not override these.
	TimeFunc  func() time.Time
	SleepFunc func(d time.Duration)

	// you can pass your custom logger if you'd like to
	// but it's not required
	Logger Logger
}

// New returns an instance of goll.LoadLimiter
// built with the specified configuration.
//
// A non-nil error is returned in case of invalid configuration.
func New(config *Config) (StandaloneLoadLimiter, error) {
	effectiveLogger := config.Logger
	if effectiveLogger == nil {
		effectiveLogger = &defaultLogger{}
	} else {
		effectiveLogger.Info("binding provided logger to composite LoadLimiter")
	}

	parsedConfig, err := validateConfiguration(config, effectiveLogger)
	if err != nil {
		return nil, err
	}

	out := loadLimiterDefaultImpl{
		Config:      parsedConfig,
		TenantData:  make(map[string]*loadLimiterDefaultImplTenantData),
		TimeFunc:    config.TimeFunc,
		SleepFunc:   config.SleepFunc,
		Logger:      effectiveLogger,
		SyncAdapter: config.SyncAdapter,
	}

	if out.TimeFunc == nil {
		out.TimeFunc = time.Now
	}
	if out.SleepFunc == nil {
		out.SleepFunc = time.Sleep
	}

	return &out, nil
}

// validateConfiguration will parse the user-provided configuration
// to the required format for runtime while also validating it.
func validateConfiguration(config *Config, logger Logger) (*loadLimiterEffectiveConfig, error) {
	if logger == nil {
		logger = &defaultLogger{}
	}

	out := loadLimiterEffectiveConfig{
		ApplyOverstepPenalty: false,
		ApplyPenaltyCapping:  false,
		SkipRetryInComputing: config.SkipRetryInComputing,
	}

	if config.MaxLoad <= 0 {
		return nil, fmt.Errorf("MaxLoad should be greater than 0 (given: %v)", config.MaxLoad)
	}
	out.MaxLoad = config.MaxLoad

	windowSizeMillis := config.WindowSize.Milliseconds()
	if windowSizeMillis <= 0 {
		return nil, fmt.Errorf("WindowSize should be at least 1ms (given: %v)", config.WindowSize)
	}
	out.WindowSize = uint64(windowSizeMillis)

	if config.MaxPenaltyCapFactor < 0 {
		return nil, fmt.Errorf("MaxPenaltyCapFactor should be zero or positive (given: %v)", config.MaxPenaltyCapFactor)
	} else if config.MaxPenaltyCapFactor > 0 {
		absoluteMaxPenaltyCap := uint64(float64(config.MaxLoad) * (1.0 + config.MaxPenaltyCapFactor))
		out.AbsoluteMaxPenaltyCap = absoluteMaxPenaltyCap
		out.ApplyPenaltyCapping = true
	} else {
		// apply a reasonable default
		out.AbsoluteMaxPenaltyCap = uint64(float64(config.MaxLoad) * (1.0 + defaultMaxPenaltyCapFactor))
		out.ApplyPenaltyCapping = true
	}

	var windowSegmentSizeMillis int64
	if config.WindowSegmentSize == 0 {
		autoSegmentSize, err := pickSegmentSize(windowSizeMillis)
		if err != nil {
			return nil, err
		}
		windowSegmentSizeMillis = autoSegmentSize.Milliseconds()
	} else {
		windowSegmentSizeMillis = config.WindowSegmentSize.Milliseconds()
		if windowSegmentSizeMillis <= 0 {
			return nil, fmt.Errorf("WindowSegmentSize is too small, it should never be less than a millisecond (given: %v)", config.WindowSegmentSize)
		}
	}

	if windowSegmentSizeMillis > windowSizeMillis {
		return nil, fmt.Errorf("WindowSegmentSize should not be greater than WindowSize (given: %v over %v)", config.WindowSegmentSize, config.WindowSize)
	}

	// WindowSize should be exactly divisible by WindowSegmentSize.
	if windowSizeMillis%windowSegmentSizeMillis > 0 {
		return nil, fmt.Errorf("WindowSize should be an exact multiple of WindowSegmentSize (given: %v over %v)", config.WindowSize, config.WindowSegmentSize)
	}

	out.WindowSegmentSize = uint64(windowSegmentSizeMillis)
	numSegments := uint64(windowSizeMillis / windowSegmentSizeMillis)
	out.NumSegments = numSegments

	if config.OverstepPenaltyFactor < 0 {
		return nil, fmt.Errorf("OverstepPenaltyFactor should be zero or positive (given: %v)", config.OverstepPenaltyFactor)
	}
	if config.OverstepPenaltyDistributionFactor < 0 || config.OverstepPenaltyDistributionFactor > 1.0 {
		return nil, fmt.Errorf("OverstepPenaltyDistributionFactor should be valued in the range from 0.0 to 1.0 (given: %v)", config.OverstepPenaltyDistributionFactor)
	}
	if config.OverstepPenaltyFactor > 0 {
		absoluteOverstepPenalty := uint64(float64(config.MaxLoad) * config.OverstepPenaltyFactor)
		overstepPenaltySegmentSpan := uint64(1)

		if config.OverstepPenaltyDistributionFactor > 0 {
			overstepPenaltySegmentSpan = uint64(math.Round(config.OverstepPenaltyDistributionFactor * float64(numSegments)))
			if overstepPenaltySegmentSpan <= 0 {
				overstepPenaltySegmentSpan = 1
				logger.Warning(fmt.Sprintf("the specified OverstepPenaltyDistributionFactor of %v would result in overstep penalty spanning no segments, defaulting to spanning only on the last segment", config.OverstepPenaltyDistributionFactor))
			}
		}

		out.ApplyOverstepPenalty = true
		out.AbsoluteOverstepPenalty = absoluteOverstepPenalty
		out.OverstepPenaltySegmentSpan = overstepPenaltySegmentSpan
	}

	if config.RequestOverheadPenaltyFactor < 0 {
		return nil, fmt.Errorf("RequestOverheadPenaltyFactor should be zero or positive (given: %v)", config.RequestOverheadPenaltyFactor)
	}
	if config.RequestOverheadPenaltyDistributionFactor < 0 || config.RequestOverheadPenaltyDistributionFactor > 1.0 {
		return nil, fmt.Errorf("RequestOverheadPenaltyDistributionFactor should be valued in the range from 0.0 to 1.0 (given: %v)", config.RequestOverheadPenaltyDistributionFactor)
	}
	if config.RequestOverheadPenaltyFactor > 0 {
		requestOverheadPenaltySegmentSpan := uint64(1)
		if config.RequestOverheadPenaltyDistributionFactor > 0 {
			requestOverheadPenaltySegmentSpan = uint64(math.Round(config.RequestOverheadPenaltyDistributionFactor * float64(numSegments)))

			if requestOverheadPenaltySegmentSpan <= 0 {
				requestOverheadPenaltySegmentSpan = 1
				logger.Warning(fmt.Sprintf("the specified RequestOverheadPenaltyDistributionFactor of %v would result in penalty spanning no segments, defaulting to spanning only on the last segment", config.RequestOverheadPenaltyDistributionFactor))
			}
		}

		out.ApplyRequestOverheadPenalty = true
		out.RequestOverheadPenaltyFactor = config.RequestOverheadPenaltyFactor
		out.RequestOverheadPenaltySegmentSpan = requestOverheadPenaltySegmentSpan
	}

	return &out, nil
}

// NewComposite returns an instance of goll.LoadLimiter
// built with the specified configuration, combining multiple
// limiter policies into a single instance.
//
// A non-nil error is returned in case of invalid configuration.
func NewComposite(config *CompositeConfig) (CompositeLoadLimiter, error) {
	effectiveLogger := config.Logger
	if effectiveLogger == nil {
		effectiveLogger = &defaultLogger{}
	} else {
		effectiveLogger.Info("binding provided logger to composite LoadLimiter")
	}

	parsedConfig, err := validateCompositeConfiguration(config, effectiveLogger)
	if err != nil {
		return nil, err
	}

	out := compositeLoadLimiterDefaultImpl{
		Config:      parsedConfig,
		TimeFunc:    config.TimeFunc,
		SleepFunc:   config.SleepFunc,
		Logger:      effectiveLogger,
		SyncAdapter: config.SyncAdapter,
	}

	if out.TimeFunc == nil {
		out.TimeFunc = time.Now
	}
	if out.SleepFunc == nil {
		out.SleepFunc = time.Sleep
	}

	subTimeFunc := func() time.Time {
		return out.TimeFunc()
	}
	subSleepFunc := func(d time.Duration) {
		out.SleepFunc(d)
	}

	limiters := make([]*loadLimiterDefaultImpl, len(config.Limiters))
	for i, config := range config.Limiters {
		// TODO cover with proper unit tests
		if config.TimeFunc != nil {
			return nil, errors.New("cannot specify TimeFunc on a composed limiter. Please specify it on the parent limiter instead")
		}
		config.TimeFunc = subTimeFunc

		if config.SleepFunc != nil {
			return nil, errors.New("cannot specify SleepFunc on a composed limiter. Please specify it on the parent limiter instead")
		}
		config.SleepFunc = subSleepFunc

		if config.SyncAdapter != nil {
			return nil, errors.New("cannot specify SyncAdapter on a composed limiter. Please specify it on the parent limiter instead")
		}

		if config.Logger == nil {
			config.Logger = effectiveLogger
		}

		limiter, err := New(&config)
		if err != nil {
			return nil, fmt.Errorf("error building limiter at index %d: %w", i, err)
		}
		limiters[i] = limiter.(*loadLimiterDefaultImpl)
	}

	out.Limiters = limiters

	return &out, nil
}

// validateCompositeConfiguration will parse the user-provided configuration
// to the required format for runtime while also validating it.
func validateCompositeConfiguration(config *CompositeConfig, logger Logger) (*compositeLoadLimiterEffectiveConfig, error) {
	out := compositeLoadLimiterEffectiveConfig{}

	num := len(config.Limiters)
	if num < 1 {
		return nil, errors.New("composite load limiter requires at least one component configuration")
	}

	return &out, nil
}

func pickSegmentSize(windowSizeMillis int64) (time.Duration, error) {
	if windowSizeMillis <= 0 {
		return 0, errors.New("negative duration is not allowed")
	}
	if windowSizeMillis%20 != 0 {
		return 0, errors.New("the provided windowSize is not exactly divisible in segments. " +
			"Please provide a valid WindowSizeSegment parameter")
	}
	res := windowSizeMillis / 20
	if res < 1 {
		return 0, errors.New("the given WindowSize is too small to allow automatically picking a WindowSegmentSize. " +
			"Please give an explicit WindowSegmentSize or pick a larger WindowSize")
	}
	return time.Duration(res) * time.Millisecond, nil
}
