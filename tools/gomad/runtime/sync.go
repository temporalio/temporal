// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package sim_runtime

import (
	"cmp"
	"math/rand"
	"slices"
	"time"

	"go.temporal.io/server/tools/gomad/util/verify"
)

type (
	// TODO: rename these; SyncTask? SyncAction?
	syncBlock struct {
		pt               SyncPoint
		op               SyncOp
		g                *goroutine
		requireSyncMatch bool
		loc              string // TODO
		delay            time.Duration
		children         []*syncBlock
		onSync           func()
	}
	SyncOp       int
	SyncPoint    interface{}
	Synchronizer struct {
		drng        *rand.Rand
		unsynced    map[SyncPoint]map[SyncOp]map[*goroutine][]*syncBlock
		byGoroutine map[*goroutine][]SyncPoint
	}
)

const (
	rcv SyncOp = iota // receiving from channel
	snd               // sending to channel
	cls               // channel closed
	slc               // select statement
)

func newSynchronizer(drng *rand.Rand) Synchronizer {
	return Synchronizer{
		drng:        drng,
		unsynced:    make(map[SyncPoint]map[SyncOp]map[*goroutine][]*syncBlock),
		byGoroutine: make(map[*goroutine][]SyncPoint),
	}
}

func (s *Synchronizer) blocksToResume(newBlock *syncBlock) (resumeBlocks []*syncBlock) {
	var newUnsyncedBlocks []*syncBlock

	switch op := newBlock.op; op {

	// sync any channel of select
	case slc:
		// collect all cases that could be resumed right now
		var resumableCases []*syncBlock
		syncMatchCandidatesForBlock := make(map[*syncBlock][]*syncBlock)
		for _, block := range newBlock.children {
			if syncMatchCandidates := s.findMatches(block); len(syncMatchCandidates) > 0 {
				// found a sync match!
				syncMatchCandidatesForBlock[block] = syncMatchCandidates
				resumableCases = append(resumableCases, block)
			} else if !block.requireSyncMatch {
				// no sync match, but no sync needed!
				resumableCases = append(resumableCases, block)
			}
		}

		if toResume := s.pickRandom(resumableCases); toResume != nil {
			// there are non-default cases that can be resumed ...
			if resumableCandidateMatches, found := syncMatchCandidatesForBlock[toResume]; found {
				// picked block is a sync match! - resume both blocks
				toResumeToo := s.pickRandom(resumableCandidateMatches)
				resumeBlocks = sndFirst(toResume, toResumeToo) // snd must happen *first* for the rcv to work
			} else {
				// picked block does not need a sync match! - resume block immediately
				resumeBlocks = []*syncBlock{toResume}
			}
		} else {
			// there is no non-default case that can be resumed right now ...
			if newBlock.pt.(*Selector).hasDefaultCase {
				// there is a default case - resume block immediately
				resumeBlocks = []*syncBlock{newBlock}
			} else {
				// there is no default case - enqueue all blocks and wait until one sync matches
				newUnsyncedBlocks = newBlock.children
			}
		}

		verify.T(len(resumeBlocks) <= 2, "more than two blocks were chosen to resume")

	// unblock *all* receivers of the closed channel
	case cls:
		for _, receivingBlocks := range s.unsynced[newBlock.pt][rcv] {
			for _, receivingBlock := range receivingBlocks {
				resumeBlocks = append(resumeBlocks, receivingBlock)
			}
		}
		resumeBlocks = append(resumeBlocks, newBlock)

		// sort to make sure
		// TODO: can this be made unnecessary?
		slices.SortFunc(resumeBlocks, func(a, b *syncBlock) int {
			return cmp.Compare(a.g.id, b.g.id)
		})

	// sync snd/rcv for channel
	// TODO: refactor for re-use above
	default:
		// always attempt to sync match first (even if this is a buffered channel)
		syncMatchCandidates := s.findMatches(newBlock)

		if toResume := s.pickRandom(syncMatchCandidates); toResume != nil {
			// found a sync match! - resume both blocks
			resumeBlocks = sndFirst(newBlock, toResume) // snd must happen *first* for the rcv to work
		} else if !newBlock.requireSyncMatch {
			// no sync match, but no sync needed! - resume block immediately
			resumeBlocks = append(resumeBlocks, newBlock)
		} else {
			// no sync match, but it's needed! - enqueue block for future sync match
			newUnsyncedBlocks = []*syncBlock{newBlock}
		}

		verify.T(len(resumeBlocks) <= 2, "more than two blocks were chosen to resume")
	}

	verify.T(len(resumeBlocks)+len(newUnsyncedBlocks) > 0, "no blocks were chosen to resume or queued for sync")

	// sync match!
	for _, resumeBlock := range resumeBlocks {
		// remove *single* block (ie for channel snd/rcv)
		s.removeBlock(resumeBlock.pt, resumeBlock.op, resumeBlock.g)

		// remove *all* blocks for entire goroutine (ie from select)
		s.removeAllBlocks(resumeBlock.g)
	}

	// no sync match! add new blocks
	for _, block := range newUnsyncedBlocks {
		if _, ok := s.unsynced[block.pt]; !ok {
			s.unsynced[block.pt] = make(map[SyncOp]map[*goroutine][]*syncBlock)
		}
		if _, ok := s.unsynced[block.pt][block.op]; !ok {
			s.unsynced[block.pt][block.op] = make(map[*goroutine][]*syncBlock)
		}
		s.unsynced[block.pt][block.op][block.g] = append(s.unsynced[block.pt][block.op][block.g], block)
		s.byGoroutine[block.g] = append(s.byGoroutine[block.g], block.pt)
	}

	return
}

func (s *Synchronizer) removeBlock(pt SyncPoint, op SyncOp, g *goroutine) {
	if len(s.unsynced[pt][op][g]) == 0 {
		return
	}
	delete(s.unsynced[pt][op], g)
	if len(s.unsynced[pt][op]) == 0 {
		delete(s.unsynced[pt], op)
	}
	if len(s.unsynced[pt]) == 0 {
		delete(s.unsynced, pt)
	}
}

func (s *Synchronizer) removeAllBlocks(g *goroutine) {
	for _, pt := range s.byGoroutine[g] {
		s.removeBlock(pt, snd, g)
		s.removeBlock(pt, rcv, g)
	}
	delete(s.byGoroutine, g)
}

func (s *Synchronizer) findMatches(newBlocker *syncBlock) []*syncBlock {
	var result []*syncBlock

	possibleMatches := s.unsynced[newBlocker.pt][newBlocker.op.matching()]
	for _, blocks := range possibleMatches {
		for _, block := range blocks {
			result = append(result, block)
		}
	}

	slices.SortFunc(result, func(a, b *syncBlock) int {
		return cmp.Compare(a.g.id, b.g.id)
	})

	return result
}

func (s *Synchronizer) pickRandom(candidates []*syncBlock) *syncBlock {
	if len(candidates) == 0 {
		return nil
	}
	return candidates[s.drng.Intn(len(candidates))]
}

func sndFirst(left, right *syncBlock) []*syncBlock {
	if left.op == snd { // always must resume sender first!
		return []*syncBlock{left, right}
	}
	return []*syncBlock{right, left}
}

func (r SyncOp) string() string {
	switch r {
	case rcv:
		return "rcv"
	case snd:
		return "snd"
	case cls:
		return "cls"
	case slc:
		return "slc"
	default:
		return "unknown"
	}
}

func (r SyncOp) matching() SyncOp {
	switch r {
	case rcv:
		return snd
	case snd:
		return rcv
	default:
		panic("unknown")
	}
}
