package sharding

import (
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"

	"github.com/dgryski/go-farm"
)

// ConsistentHashRing implements consistent hashing with virtual nodes for test distribution
type ConsistentHashRing struct {
	virtualNodes    int                    // Number of virtual nodes per physical shard
	ring           map[uint32]int         // Hash -> shard mapping
	sortedHashes   []uint32               // Sorted hash values for binary search
	numShards      int                    // Number of physical shards
	hashFunc       func([]byte) uint32    // Hash function to use
}

// NewConsistentHashRing creates a new consistent hash ring
func NewConsistentHashRing(numShards int, virtualNodes int) *ConsistentHashRing {
	ring := &ConsistentHashRing{
		virtualNodes: virtualNodes,
		ring:        make(map[uint32]int),
		numShards:   numShards,
		hashFunc:    defaultHashFunc,
	}

	ring.rebuildRing()
	return ring
}

// NewConsistentHashRingWithHashFunc creates a ring with a custom hash function
func NewConsistentHashRingWithHashFunc(numShards int, virtualNodes int, hashFunc func([]byte) uint32) *ConsistentHashRing {
	ring := &ConsistentHashRing{
		virtualNodes: virtualNodes,
		ring:        make(map[uint32]int),
		numShards:   numShards,
		hashFunc:    hashFunc,
	}

	ring.rebuildRing()
	return ring
}

// rebuildRing reconstructs the hash ring with virtual nodes
func (c *ConsistentHashRing) rebuildRing() {
	c.ring = make(map[uint32]int)
	c.sortedHashes = nil

	// Create virtual nodes for each shard
	for shardID := 0; shardID < c.numShards; shardID++ {
		for vnodeID := 0; vnodeID < c.virtualNodes; vnodeID++ {
			// Create unique identifier for this virtual node
			vnodeKey := fmt.Sprintf("shard-%d-vnode-%d", shardID, vnodeID)
			hash := c.hashFunc([]byte(vnodeKey))

			// Handle hash collisions by incrementing
			for c.ring[hash] != 0 || (hash == 0 && shardID != 0) {
				hash++
			}

			c.ring[hash] = shardID
			c.sortedHashes = append(c.sortedHashes, hash)
		}
	}

	// Sort hashes for efficient binary search
	sort.Slice(c.sortedHashes, func(i, j int) bool {
		return c.sortedHashes[i] < c.sortedHashes[j]
	})
}

// GetShard returns the shard ID for a given test name using consistent hashing
func (c *ConsistentHashRing) GetShard(testName string, salt string) int {
	key := testName + salt
	hash := c.hashFunc([]byte(key))

	// Find the first hash value >= our hash (clockwise on ring)
	idx := sort.Search(len(c.sortedHashes), func(i int) bool {
		return c.sortedHashes[i] >= hash
	})

	// If no hash is >= our hash, wrap around to the first one
	if idx == len(c.sortedHashes) {
		idx = 0
	}

	ringHash := c.sortedHashes[idx]
	return c.ring[ringHash]
}

// AnalyzeDistribution analyzes the distribution quality of the consistent hash ring
func (c *ConsistentHashRing) AnalyzeDistribution(testNames []string, salt string) *DistributionStats {
	shardCounts := make([]int, c.numShards)

	// Distribute tests using consistent hashing
	for _, testName := range testNames {
		shardID := c.GetShard(testName, salt)
		shardCounts[shardID]++
	}

	// Use existing analyzer to compute stats
	analyzer := NewAnalyzer(testNames, c.numShards)
	return analyzer.CalculateStats(shardCounts)
}

// AddShard simulates adding a new shard and returns the redistribution cost
func (c *ConsistentHashRing) AddShard() *RedistributionAnalysis {
	oldDistribution := c.getTestDistribution()

	// Add new shard
	c.numShards++
	c.rebuildRing()

	newDistribution := c.getTestDistribution()

	return &RedistributionAnalysis{
		OldShards:        c.numShards - 1,
		NewShards:        c.numShards,
		TestsMoved:       c.calculateMoves(oldDistribution, newDistribution),
		RedistributionPct: c.calculateRedistributionPct(oldDistribution, newDistribution),
	}
}

// RemoveShard simulates removing a shard and returns the redistribution cost
func (c *ConsistentHashRing) RemoveShard() *RedistributionAnalysis {
	if c.numShards <= 1 {
		return &RedistributionAnalysis{Error: "Cannot remove last shard"}
	}

	oldDistribution := c.getTestDistribution()

	// Remove shard
	c.numShards--
	c.rebuildRing()

	newDistribution := c.getTestDistribution()

	return &RedistributionAnalysis{
		OldShards:        c.numShards + 1,
		NewShards:        c.numShards,
		TestsMoved:       c.calculateMoves(oldDistribution, newDistribution),
		RedistributionPct: c.calculateRedistributionPct(oldDistribution, newDistribution),
	}
}

// getTestDistribution returns a sample test distribution for analysis
func (c *ConsistentHashRing) getTestDistribution() map[string]int {
	testNames := GenerateRepresentativeTestNames(1000)
	distribution := make(map[string]int)

	for _, testName := range testNames {
		shardID := c.GetShard(testName, "-salt-0")
		distribution[testName] = shardID
	}

	return distribution
}

// calculateMoves counts how many tests moved between distributions
func (c *ConsistentHashRing) calculateMoves(oldDist, newDist map[string]int) int {
	moves := 0
	for testName, oldShard := range oldDist {
		if newShard, exists := newDist[testName]; exists && newShard != oldShard {
			moves++
		}
	}
	return moves
}

// calculateRedistributionPct calculates the percentage of tests that moved
func (c *ConsistentHashRing) calculateRedistributionPct(oldDist, newDist map[string]int) float64 {
	if len(oldDist) == 0 {
		return 0.0
	}
	moves := c.calculateMoves(oldDist, newDist)
	return float64(moves) / float64(len(oldDist)) * 100.0
}

// RedistributionAnalysis contains analysis of shard changes
type RedistributionAnalysis struct {
	OldShards         int
	NewShards         int
	TestsMoved        int
	RedistributionPct float64
	Error             string
}

// String returns a string representation of the analysis
func (r *RedistributionAnalysis) String() string {
	if r.Error != "" {
		return r.Error
	}
	return fmt.Sprintf("Shard change %d->%d: %d tests moved (%.1f%%)",
		r.OldShards, r.NewShards, r.TestsMoved, r.RedistributionPct)
}

// Hash function options
func defaultHashFunc(data []byte) uint32 {
	return farm.Fingerprint32(data)
}

func crc32HashFunc(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// ConsistentHashConfig holds configuration for consistent hashing
type ConsistentHashConfig struct {
	Enabled        bool
	VirtualNodes   int
	HashFunction   string
}

// GetConsistentHashConfig reads configuration from environment
func GetConsistentHashConfig() *ConsistentHashConfig {
	config := &ConsistentHashConfig{
		Enabled:      false,
		VirtualNodes: 150, // Default recommended value
		HashFunction: "farm",
	}

	// Check if consistent hashing is enabled
	if enabled, err := strconv.ParseBool(getEnvWithDefault("TEST_CONSISTENT_HASH", "false")); err == nil && enabled {
		config.Enabled = true
	}

	// Configure virtual nodes
	if vnodes, err := strconv.Atoi(getEnvWithDefault("TEST_CONSISTENT_HASH_VNODES", "150")); err == nil {
		config.VirtualNodes = vnodes
	}

	// Configure hash function
	config.HashFunction = getEnvWithDefault("TEST_CONSISTENT_HASH_FUNC", "farm")

	return config
}


// NewConsistentHashChecker creates a shard checker using consistent hashing
func NewConsistentHashChecker(numShards int) func(string) int {
	config := GetConsistentHashConfig()

	if !config.Enabled {
		// Fall back to the existing hash method
		return func(testName string) int {
			salt := GetOptimalSalt(numShards)
			nameToHash := testName + salt
			return int(farm.Fingerprint32([]byte(nameToHash))) % numShards
		}
	}

	// Use consistent hashing
	var hashFunc func([]byte) uint32
	switch config.HashFunction {
	case "crc32":
		hashFunc = crc32HashFunc
	default:
		hashFunc = defaultHashFunc
	}

	ring := NewConsistentHashRingWithHashFunc(numShards, config.VirtualNodes, hashFunc)
	salt := GetOptimalSalt(numShards)

	return func(testName string) int {
		return ring.GetShard(testName, salt)
	}
}

