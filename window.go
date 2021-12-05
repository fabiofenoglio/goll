package goll

import (
	"errors"
	"fmt"
	"time"
)

func (instance *loadLimiterDefaultImpl) locateSegmentStartTime(t uint64) uint64 {

	return (t / instance.Config.WindowSegmentSize) * instance.Config.WindowSegmentSize
}

func (instance *loadLimiterDefaultImpl) rotateWindow(req *submitRequest) {
	tenant := req.TenantData

	// compute the start time of the segment we should be in
	expectedCurrentSegmentStartTime := req.RequestSegmentStartTime
	queue := tenant.WindowQueue
	queueSize := queue.Len()
	// removeBefore := t - instance.Config.WindowSize
	removeBefore := expectedCurrentSegmentStartTime - instance.Config.WindowSize

	// window rotation is not needed if all the following conditions are met:
	// - the queue is not empty
	// - the front element has the correct startTime = expectedCurrentSegmentStartTime
	// - the back element startTime is not <= removeBefore
	if queueSize > 0 &&
		queue.Front().(*windowSegment).StartTime == expectedCurrentSegmentStartTime &&
		queue.Back().(*windowSegment).StartTime > removeBefore {
		// no need to rotate
		return
	}

	dirty := false

	// check if the front of the queue is FUTURE with respect
	// to the current segment start time.
	// this could happen when synchronizing the instance
	// with others on different machines with slightly
	// unsynced clocks
	removedLoadToRestore := uint64(0)
	if queueSize > 0 && queue.Front().(*windowSegment).StartTime > expectedCurrentSegmentStartTime {
		instance.Logger.Warning(
			"time mismatch on top of the window. " +
				"please check that all synchronized instances have an aligned clock.",
		)

		// remove the future buckets
		for {
			frontBucket := queue.Front().(*windowSegment)
			if frontBucket.StartTime <= expectedCurrentSegmentStartTime {
				break
			}
			removedLoadToRestore += frontBucket.Value
			tenant.WindowTotal -= frontBucket.Value
			queue.PopFront()
			queueSize--

			if queueSize < 1 {
				break
			}
		}
	}

	// require the front-most segment to exist and to have the correct startTime
	// if it does not, we create and push a new segment, automatically
	// assuming the previous front-most one was actually older.
	// This is a reasonable assumption as long as times keep moving forward.
	if queueSize == 0 || queue.Front().(*windowSegment).StartTime != expectedCurrentSegmentStartTime {
		queue.PushFront(&windowSegment{
			StartTime: expectedCurrentSegmentStartTime,
			Value:     0,
		})
		queueSize += 1
		dirty = true
	}

	// if needed, we remove obsolete segments older than the
	// lower bound of the window.
	if queueSize > 1 {
		removeBefore := req.RequestedTimestamp - instance.Config.WindowSize
		for queue.Len() > 0 && queue.Back().(*windowSegment).StartTime <= removeBefore {
			removed := queue.PopBack().(*windowSegment)
			if removed.Value != 0 {
				tenant.WindowTotal -= removed.Value
			}
			dirty = true
		}
	}

	// if load was removed for alignment, just add to the recent segment
	if removedLoadToRestore > 0 {
		instance.distributePenalty(req, removedLoadToRestore, 1)
		dirty = true
	}

	if dirty {
		instance.markDirty(req)
	}
}

// ensure that the N most recent segments exist,
// optionally filling missing segments.
//
// This method does not modify the queue if the requirement is already satisfited.
//
// In case of missing segments the queue could be rebuilt completely.
func (instance *loadLimiterDefaultImpl) ensureLatestNSegments(req *submitRequest, numSegments uint64) {
	latestSegmentTime := req.RequestSegmentStartTime
	tenant := req.TenantData
	rebuildQueue := false
	queue := tenant.WindowQueue
	queueLen := uint64(queue.Len())
	validSegments := make(map[uint64]*windowSegment, numSegments)

	for i := uint64(0); i < numSegments; i++ {
		segmentStartTime := latestSegmentTime - (i * instance.Config.WindowSegmentSize)
		if queueLen <= i {
			rebuildQueue = true
		} else {
			segmentAtIndex := queue.At(int(i)).(*windowSegment)
			validSegments[segmentAtIndex.StartTime] = segmentAtIndex

			if segmentAtIndex.StartTime != segmentStartTime {
				rebuildQueue = true
			}
		}
	}

	if !rebuildQueue {
		return
	}

	rebuildSegmentsStartingAt := latestSegmentTime - ((numSegments - 1) * instance.Config.WindowSegmentSize)

	newQueue := instance.newWindowQueue()

	tenant.WindowQueue = newQueue

	for i := uint64(0); i < queueLen; i++ {
		oldSegment := queue.At(int(queueLen - i - 1)).(*windowSegment)
		if oldSegment.StartTime < rebuildSegmentsStartingAt {
			newQueue.PushFront(oldSegment)
		} else {
			break
		}
	}

	for i := int64(numSegments - 1); i >= 0; i-- {
		segmentStartTime := latestSegmentTime - (uint64(i) * instance.Config.WindowSegmentSize)
		inValidCache, isValid := validSegments[segmentStartTime]
		if isValid {
			newQueue.PushFront(inValidCache)
		} else {
			newQueue.PushFront(&windowSegment{
				StartTime: segmentStartTime,
			})
		}
	}
}

// Compute the RetryIn time
// by checking how many segments we need to remove
// before having room for the required load
// and how long it will take for those segments
// to get outside of the lower window bound.
func (instance *loadLimiterDefaultImpl) computeRetryIn(req *submitRequest) (time.Duration, error) {
	if req.RequestedLoad > instance.Config.MaxLoad {
		return 0, fmt.Errorf("requested load of %v is over max window load of %v and will never be allowed", req.RequestedLoad, instance.Config.MaxLoad)
	}
	tenant := req.TenantData

	toFree := int64(req.RequestedLoad) + int64(tenant.WindowTotal) - int64(instance.Config.MaxLoad)

	if toFree <= 0 {
		return 0, nil
	}

	queue := tenant.WindowQueue
	queueLen := queue.Len()
	mostRecentSegmentRemovalTime := uint64(0)

	for i := 0; i < queueLen && toFree > 0; i++ {
		segment := queue.At(queueLen - i - 1).(*windowSegment)
		if segment.Value > 0 {
			toFree -= int64(segment.Value)
		}
		mostRecentSegmentRemovalTime = segment.StartTime
	}

	if mostRecentSegmentRemovalTime == 0 || toFree > 0 {
		// this should never happen.
		return 0, errors.New("could not compute RetryIn because of inconsistent queue data")
	}

	// compute the min time for which the segment starting at mostRecentSegmentRemovalTime will be removed
	minSegmentAvailTime := mostRecentSegmentRemovalTime + instance.Config.WindowSize

	if minSegmentAvailTime < req.RequestedTimestamp {
		panic("inconsistent minSegmentAvailTime result from computing RetryIn (< load request time)")
	}

	return time.Millisecond * time.Duration(minSegmentAvailTime-req.RequestedTimestamp), nil
}

func (instance *loadLimiterDefaultImpl) distributePenalty(req *submitRequest, amount uint64, numSegmentsMax uint64) {
	if amount <= 0 {
		return
	}
	tenant := req.TenantData

	/*
		Penalty distribution samples:

		* 1 over 3 segments: [1 0 0]
		* 2 over 3 segments: [1 1 0]
		* 3 over 3 segments: [1 1 1]
		* 4 over 3 segments: [2 1 1]
		* 5 over 3 segments: [2 2 1]
		* ...
		* 6 over 3 segments: [2 2 2]
		* 11 over 3 segments: [4 4 3]
		* ...
	*/
	amountPerSegment := amount / numSegmentsMax
	if amountPerSegment < 1 {
		numSegmentsMax = amount
		amountPerSegment = 1
	}

	segmentDistribution := make([]uint64, numSegmentsMax)

	for i := uint64(0); i < numSegmentsMax; i++ {
		segmentDistribution[i] = amountPerSegment
	}
	added := amountPerSegment * numSegmentsMax
	rem := amount % numSegmentsMax
	for i := uint64(0); i < rem; i++ {
		segmentDistribution[i]++
		added++
	}

	instance.ensureLatestNSegments(req, numSegmentsMax)
	for i := uint64(0); i < numSegmentsMax; i++ {
		sv := segmentDistribution[i]
		tenant.WindowQueue.At(int(i)).(*windowSegment).Value += sv
		tenant.WindowTotal += sv
	}
}

func (instance *loadLimiterDefaultImpl) removeFromOldestSegments(req *submitRequest, amount uint64) {
	tenant := req.TenantData

	// try to remove from the left
	queue := tenant.WindowQueue
	for queue.Len() > 0 && amount > 0 {
		oldestSegment := queue.Back().(*windowSegment)
		if oldestSegment.Value >= amount {
			// can subtract all from this segment
			oldestSegment.Value -= amount
			tenant.WindowTotal -= amount
			amount = 0
		} else {
			// subtract part from here
			amount -= oldestSegment.Value
			tenant.WindowTotal -= oldestSegment.Value
			oldestSegment.Value = 0
		}
		if oldestSegment.Value <= 0 {
			// segment is now empty, remove it
			queue.PopBack()
		}
	}

	if queue.Len() == 0 {
		// restore latest bucket
		queue.PushFront(&windowSegment{
			StartTime: req.RequestSegmentStartTime,
			Value:     0,
		})
	}

	if amount > 0 {
		// should never happen. just emit a warning
		instance.Logger.Warning("cannot sub excess over max cap starting from oldest entries")
	}
}

func (instance *loadLimiterDefaultImpl) applyCapping(req *submitRequest) {
	if !instance.Config.ApplyPenaltyCapping {
		return
	}
	tenant := req.TenantData

	if tenant.WindowTotal > instance.Config.AbsoluteMaxPenaltyCap {
		overMaxCap := tenant.WindowTotal - instance.Config.AbsoluteMaxPenaltyCap
		instance.removeFromOldestSegments(req, overMaxCap)
	}
}
