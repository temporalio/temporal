package workflow

import (
	commonpb "go.temporal.io/api/common/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/serialization"
)

// userTimers holds the pending user timers of a mutable state. Entries loaded
// from persistence stay encoded until first access, so workflows with many
// pending timers do not pay for decoding entries that are never read. Not
// thread safe, access must be confined by the workflow context lock.
type userTimers struct {
	serializer serialization.Serializer
	logger     log.Logger

	// blobs contains entries as read from persistence that were not decoded yet.
	blobs map[string]*commonpb.DataBlob
	// typed contains decoded DB entries and all timers created or updated in memory.
	typed map[string]*persistencespb.TimerInfo
	// eventIDToID is a reverse index over typed entries only.
	eventIDToID map[int64]string
}

func newUserTimers(
	logger log.Logger,
) *userTimers {
	return &userTimers{
		serializer: serialization.NewSerializer(),
		logger:     logger,
	}
}

// seedTyped takes ownership of infos, which must not be modified by the caller afterwards.
func (t *userTimers) seedTyped(infos map[string]*persistencespb.TimerInfo) {
	t.typed = infos
	t.blobs = nil
	t.eventIDToID = make(map[int64]string)
	for _, info := range infos {
		t.updateEventIDIndex(info)
	}
}

// seedBlobs takes ownership of blobs, which must not be modified by the caller afterwards.
func (t *userTimers) seedBlobs(blobs map[string]*commonpb.DataBlob) {
	t.blobs = blobs
	t.typed = nil
	t.eventIDToID = nil
}

// approximateEncodedSize estimates the persisted size of the entries that are
// still encoded. Decoded entries are accounted for by the caller.
func (t *userTimers) approximateEncodedSize() int {
	size := 0
	for timerID, blob := range t.blobs {
		size += len(blob.GetData()) + len(timerID)
	}
	return size
}

func (t *userTimers) get(timerID string) (*persistencespb.TimerInfo, bool) {
	if info, ok := t.typed[timerID]; ok {
		return info, true
	}
	blob, ok := t.blobs[timerID]
	if !ok {
		return nil, false
	}
	info, err := t.serializer.TimerInfoFromBlob(blob)
	if err != nil {
		// drop the entry so the corruption is not hit again on every access,
		// missing entries are reported as inconsistencies by the callers
		delete(t.blobs, timerID)
		t.logger.Error("unable to decode user timer info",
			tag.String("timerId", timerID),
			tag.Error(err),
		)
		return nil, false
	}
	delete(t.blobs, timerID)
	t.setDecoded(timerID, info)
	return info, true
}

// getByEventID resolves a timer by its start event ID. The reverse index only
// covers decoded entries, so a miss decodes the remaining entries before retrying.
func (t *userTimers) getByEventID(startEventID int64) (*persistencespb.TimerInfo, bool) {
	timerID, ok := t.eventIDToID[startEventID]
	if !ok || !t.has(timerID) {
		t.decodeAll()
		timerID, ok = t.eventIDToID[startEventID]
		if !ok {
			return nil, false
		}
	}
	return t.get(timerID)
}

func (t *userTimers) has(timerID string) bool {
	if _, ok := t.typed[timerID]; ok {
		return true
	}
	_, ok := t.blobs[timerID]
	return ok
}

func (t *userTimers) len() int {
	return len(t.typed) + len(t.blobs)
}

func (t *userTimers) put(info *persistencespb.TimerInfo) {
	timerID := info.GetTimerId()
	delete(t.blobs, timerID)
	if t.typed == nil {
		t.typed = make(map[string]*persistencespb.TimerInfo)
	}
	t.typed[timerID] = info
	t.updateEventIDIndex(info)
}

// updateEventIDIndex maintains the reverse index for entries written into the
// typed map without going through put, e.g. by applyUpdatesToSubStateMachine.
func (t *userTimers) updateEventIDIndex(info *persistencespb.TimerInfo) {
	if t.eventIDToID == nil {
		t.eventIDToID = make(map[int64]string)
	}
	t.eventIDToID[info.GetStartedEventId()] = info.GetTimerId()
}

// delete removes a timer and returns the removed entry. The returned info is
// nil if the entry was found but could not be decoded.
func (t *userTimers) delete(timerID string) (*persistencespb.TimerInfo, bool) {
	info, ok := t.get(timerID)
	if !ok {
		return nil, false
	}
	delete(t.typed, timerID)
	delete(t.eventIDToID, info.GetStartedEventId())
	return info, true
}

// all decodes the remaining encoded entries and returns the typed map. The
// returned map is live, mutations must keep the reverse index in sync via
// updateEventIDIndex.
func (t *userTimers) all() map[string]*persistencespb.TimerInfo {
	t.decodeAll()
	return t.typed
}

// untouchedBlobs hands out the entries that were never accessed since load.
// Ownership of the returned map is transferred to the caller.
func (t *userTimers) untouchedBlobs() map[string]*commonpb.DataBlob {
	blobs := t.blobs
	t.blobs = nil
	return blobs
}

func (t *userTimers) decodeAll() {
	for timerID, blob := range t.blobs {
		info, err := t.serializer.TimerInfoFromBlob(blob)
		if err != nil {
			t.logger.Error("unable to decode user timer info",
				tag.String("timerId", timerID),
				tag.Error(err),
			)
			continue
		}
		t.setDecoded(timerID, info)
	}
	t.blobs = nil
}

func (t *userTimers) setDecoded(timerID string, info *persistencespb.TimerInfo) {
	if t.typed == nil {
		t.typed = make(map[string]*persistencespb.TimerInfo)
	}
	t.typed[timerID] = info
	t.updateEventIDIndex(info)
}
