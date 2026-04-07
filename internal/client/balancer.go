// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================
// Package client provides the core logic for the MasterDnsVPN client.
// This file (balancer.go) handles connection balancing strategies.
// ==============================================================================
package client

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	BalancingRoundRobinDefault = 0
	BalancingRandom            = 1
	BalancingRoundRobin        = 2
	BalancingLeastLoss         = 3
	BalancingLowestLatency     = 4
)

type Connection struct {
	Domain           string
	Resolver         string
	ResolverPort     int
	ResolverLabel    string
	Key              string
	IsValid          bool
	UploadMTUBytes   int
	UploadMTUChars   int
	DownloadMTUBytes int
	MTUResolveTime   time.Duration
}

type Balancer struct {
	strategy  int
	rrCounter atomic.Uint64
	rngState  atomic.Uint64
	version   atomic.Uint64

	mu          sync.RWMutex
	connections []Connection
	indexByKey  map[string]int
	activeIDs   []int
	inactiveIDs []int
	stats       []*connectionStats
}

type connectionStats struct {
	mu           sync.RWMutex
	sent         uint64
	acked        uint64
	rttMicrosSum uint64
	rttCount     uint64
}

const connectionStatsHalfLifeThreshold = 1000

func NewBalancer(strategy int) *Balancer {
	b := &Balancer{strategy: strategy}
	b.rngState.Store(seedRNG())
	return b
}

func (b *Balancer) SetConnections(connections []*Connection) {
	b.mu.Lock()
	defer b.mu.Unlock()

	size := len(connections)
	b.connections = make([]Connection, 0, size)
	b.indexByKey = make(map[string]int, size)
	b.activeIDs = make([]int, 0, size)
	b.inactiveIDs = make([]int, 0, size)
	b.stats = make([]*connectionStats, 0, size)

	for _, conn := range connections {
		if conn == nil || conn.Key == "" {
			continue
		}
		copied := *conn
		copied.IsValid = false
		copied.UploadMTUBytes = 0
		copied.UploadMTUChars = 0
		copied.DownloadMTUBytes = 0
		copied.MTUResolveTime = 0

		idx := len(b.connections)
		b.connections = append(b.connections, copied)
		b.indexByKey[copied.Key] = idx
		b.inactiveIDs = append(b.inactiveIDs, idx)
		b.stats = append(b.stats, &connectionStats{})
	}

	b.version.Add(1)
}

func (b *Balancer) Activate(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	idx, ok := b.indexByKey[key]
	if !ok || idx < 0 || idx >= len(b.connections) {
		return
	}
	if b.connections[idx].IsValid {
		return
	}

	b.connections[idx].IsValid = true
	b.rebuildStateIndicesLocked()
}

func (b *Balancer) Deactivate(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	idx, ok := b.indexByKey[key]
	if !ok || idx < 0 || idx >= len(b.connections) {
		return
	}
	if !b.connections[idx].IsValid {
		return
	}

	b.connections[idx].IsValid = false
	b.rebuildStateIndicesLocked()
}

func (b *Balancer) ActiveCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.activeIDs)
}

func (b *Balancer) InactiveCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.inactiveIDs)
}

func (b *Balancer) TotalCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.connections)
}

func (b *Balancer) ConnectionCount() int {
	return b.TotalCount()
}

func (b *Balancer) ValidCount() int {
	return b.ActiveCount()
}

func (b *Balancer) GetConnectionByKey(key string) (Connection, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	idx, ok := b.indexByKey[key]
	if !ok || idx < 0 || idx >= len(b.connections) {
		return Connection{}, false
	}
	return b.connections[idx], true
}

func (b *Balancer) SetConnectionValidity(key string, valid bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	idx, ok := b.indexByKey[key]
	if !ok || idx < 0 || idx >= len(b.connections) {
		return false
	}
	if b.connections[idx].IsValid == valid {
		return true
	}

	b.connections[idx].IsValid = valid
	b.rebuildStateIndicesLocked()
	return true
}

func (b *Balancer) SetConnectionMTU(key string, uploadBytes int, uploadChars int, downloadBytes int) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	idx, ok := b.indexByKey[key]
	if !ok || idx < 0 || idx >= len(b.connections) {
		return false
	}

	b.connections[idx].UploadMTUBytes = uploadBytes
	b.connections[idx].UploadMTUChars = uploadChars
	b.connections[idx].DownloadMTUBytes = downloadBytes
	return true
}

func (b *Balancer) ApplyMTUProbeResult(key string, uploadBytes int, uploadChars int, downloadBytes int, resolveTime time.Duration, active bool) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	idx, ok := b.indexByKey[key]
	if !ok || idx < 0 || idx >= len(b.connections) {
		return false
	}

	conn := &b.connections[idx]
	conn.UploadMTUBytes = uploadBytes
	conn.UploadMTUChars = uploadChars
	conn.DownloadMTUBytes = downloadBytes
	conn.MTUResolveTime = resolveTime
	conn.IsValid = active
	b.rebuildStateIndicesLocked()
	return true
}

func (b *Balancer) SnapshotVersion() uint64 {
	return b.version.Load()
}

func (b *Balancer) ReportSend(serverKey string) {
	if stats := b.statsForKey(serverKey); stats != nil {
		stats.mu.Lock()
		stats.sent++
		stats.applyHalfLifeLocked()
		stats.mu.Unlock()
	}
}

func (b *Balancer) ReportSuccess(serverKey string, rtt time.Duration) {
	stats := b.statsForKey(serverKey)
	if stats == nil {
		return
	}

	stats.mu.Lock()
	stats.acked++
	if rtt > 0 {
		stats.rttMicrosSum += uint64(rtt / time.Microsecond)
		stats.rttCount++
	}
	stats.applyHalfLifeLocked()
	stats.mu.Unlock()
}

func (b *Balancer) ResetServerStats(serverKey string) {
	stats := b.statsForKey(serverKey)
	if stats == nil {
		return
	}

	stats.mu.Lock()
	stats.sent = 0
	stats.acked = 0
	stats.rttMicrosSum = 0
	stats.rttCount = 0
	stats.mu.Unlock()
}

func (b *Balancer) SeedConservativeStats(serverKey string) {
	stats := b.statsForKey(serverKey)
	if stats == nil {
		return
	}

	stats.mu.Lock()
	stats.sent = 10
	stats.acked = 8
	stats.rttMicrosSum = 0
	stats.rttCount = 0
	stats.mu.Unlock()
}

func (b *Balancer) GetBestConnection() (Connection, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.activeIDs) == 0 {
		return Connection{}, false
	}

	switch b.strategy {
	case BalancingRandom:
		idx := b.activeIDs[b.nextRandom()%uint64(len(b.activeIDs))]
		return b.connections[idx], true
	case BalancingLeastLoss:
		if !b.hasLossSignalLocked() {
			return b.roundRobinBestConnectionLocked()
		}
		return b.bestScoredConnectionLocked(b.lossScoreLocked)
	case BalancingLowestLatency:
		if !b.hasLatencySignalLocked() {
			return b.roundRobinBestConnectionLocked()
		}
		return b.bestScoredConnectionLocked(b.latencyScoreLocked)
	default:
		return b.roundRobinBestConnectionLocked()
	}
}

func (b *Balancer) GetBestConnectionExcluding(excludeKey string) (Connection, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if len(b.activeIDs) == 0 {
		return Connection{}, false
	}

	switch b.strategy {
	case BalancingRandom:
		ordered := b.rotatedActiveIndicesLocked(1)
		for _, idx := range ordered {
			if b.connections[idx].Key == excludeKey {
				continue
			}
			return b.connections[idx], true
		}
		return Connection{}, false
	case BalancingLeastLoss:
		if !b.hasLossSignalLocked() {
			return b.roundRobinBestConnectionExcludingLocked(excludeKey)
		}
		return b.bestScoredConnectionExcludingLocked(b.lossScoreLocked, excludeKey)
	case BalancingLowestLatency:
		if !b.hasLatencySignalLocked() {
			return b.roundRobinBestConnectionExcludingLocked(excludeKey)
		}
		return b.bestScoredConnectionExcludingLocked(b.latencyScoreLocked, excludeKey)
	default:
		return b.roundRobinBestConnectionExcludingLocked(excludeKey)
	}
}

func (b *Balancer) GetUniqueConnections(requiredCount int) []Connection {
	b.mu.RLock()
	defer b.mu.RUnlock()

	count := normalizeRequiredCount(len(b.activeIDs), requiredCount, 1)
	if count <= 0 {
		return nil
	}

	if count == 1 {
		best, ok := b.getBestConnectionLocked()
		if !ok {
			return nil
		}
		return []Connection{best}
	}

	switch b.strategy {
	case BalancingRandom:
		return b.selectRandomLocked(count)
	case BalancingLeastLoss:
		if !b.hasLossSignalLocked() {
			return b.selectRoundRobinLocked(count)
		}
		return b.selectLowestScoreLocked(count, b.lossScoreLocked)
	case BalancingLowestLatency:
		if !b.hasLatencySignalLocked() {
			return b.selectRoundRobinLocked(count)
		}
		return b.selectLowestScoreLocked(count, b.latencyScoreLocked)
	default:
		return b.selectRoundRobinLocked(count)
	}
}

func (b *Balancer) GetAllActiveConnections() []Connection {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.connectionsByIDsLocked(b.activeIDs)
}

func (b *Balancer) GetAllInactiveConnections() []Connection {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.connectionsByIDsLocked(b.inactiveIDs)
}

func (b *Balancer) ActiveConnections() []Connection {
	return b.GetAllActiveConnections()
}

func (b *Balancer) InactiveConnections() []Connection {
	return b.GetAllInactiveConnections()
}

func (b *Balancer) AllConnections() []Connection {
	b.mu.RLock()
	defer b.mu.RUnlock()

	result := make([]Connection, len(b.connections))
	copy(result, b.connections)
	return result
}

func (b *Balancer) GetAllValidConnections() []Connection {
	return b.GetAllActiveConnections()
}

func (b *Balancer) AverageRTT(serverKey string) (time.Duration, bool) {
	stats := b.statsForKey(serverKey)
	if stats == nil {
		return 0, false
	}

	_, _, sum, count := stats.snapshot()
	if count == 0 {
		return 0, false
	}

	return time.Duration(sum/count) * time.Microsecond, true
}

func (b *Balancer) connectionsByIDsLocked(ids []int) []Connection {
	if len(ids) == 0 {
		return nil
	}
	result := make([]Connection, len(ids))
	for i, idx := range ids {
		if idx < 0 || idx >= len(b.connections) {
			continue
		}
		result[i] = b.connections[idx]
	}
	return result
}

func (b *Balancer) rebuildStateIndicesLocked() {
	b.activeIDs = b.activeIDs[:0]
	b.inactiveIDs = b.inactiveIDs[:0]
	for idx := range b.connections {
		if b.connections[idx].IsValid {
			b.activeIDs = append(b.activeIDs, idx)
		} else {
			b.inactiveIDs = append(b.inactiveIDs, idx)
		}
	}
	b.version.Add(1)
}

func (b *Balancer) statsForKey(serverKey string) *connectionStats {
	b.mu.RLock()
	defer b.mu.RUnlock()

	idx, ok := b.indexByKey[serverKey]
	if !ok || idx < 0 || idx >= len(b.stats) {
		return nil
	}
	return b.stats[idx]
}

func normalizeRequiredCount(validCount, requiredCount, defaultIfInvalid int) int {
	if validCount <= 0 {
		return 0
	}
	if requiredCount <= 0 {
		requiredCount = defaultIfInvalid
	}
	if requiredCount > validCount {
		return validCount
	}
	return requiredCount
}

func (b *Balancer) getBestConnectionLocked() (Connection, bool) {
	switch b.strategy {
	case BalancingRandom:
		idx := b.activeIDs[b.nextRandom()%uint64(len(b.activeIDs))]
		return b.connections[idx], true
	case BalancingLeastLoss:
		if !b.hasLossSignalLocked() {
			return b.roundRobinBestConnectionLocked()
		}
		return b.bestScoredConnectionLocked(b.lossScoreLocked)
	case BalancingLowestLatency:
		if !b.hasLatencySignalLocked() {
			return b.roundRobinBestConnectionLocked()
		}
		return b.bestScoredConnectionLocked(b.latencyScoreLocked)
	default:
		return b.roundRobinBestConnectionLocked()
	}
}

func (b *Balancer) selectRoundRobinLocked(count int) []Connection {
	n := len(b.activeIDs)
	start := roundRobinStartIndex(b.rrCounter.Add(uint64(count))-uint64(count), n)
	selected := make([]Connection, count)
	for i := 0; i < count; i++ {
		selected[i] = b.connections[b.activeIDs[(start+i)%n]]
	}
	return selected
}

func (b *Balancer) selectRandomLocked(count int) []Connection {
	n := len(b.activeIDs)
	if count <= 0 || n == 0 {
		return nil
	}
	if count == 1 {
		idx := b.activeIDs[b.nextRandom()%uint64(n)]
		return []Connection{b.connections[idx]}
	}

	indices := append([]int(nil), b.activeIDs...)
	for i := 0; i < count; i++ {
		j := i + int(b.nextRandom()%uint64(n-i))
		indices[i], indices[j] = indices[j], indices[i]
	}
	return b.connectionsByIndicesLocked(indices[:count])
}

func (b *Balancer) selectLowestScoreLocked(count int, scorer func(int) uint64) []Connection {
	n := len(b.activeIDs)
	if count <= 0 || n == 0 {
		return nil
	}
	if count == 1 {
		conn, ok := b.bestScoredConnectionLocked(scorer)
		if ok {
			return []Connection{conn}
		}
		return nil
	}

	type scoredIdx struct {
		idx   int
		score uint64
	}

	ordered := b.rotatedActiveIndicesLocked(count)
	scored := make([]scoredIdx, n)
	for i, idx := range ordered {
		scored[i] = scoredIdx{idx: idx, score: scorer(idx)}
	}

	for i := 0; i < count && i < n; i++ {
		minIdx := i
		for j := i + 1; j < n; j++ {
			if scored[j].score < scored[minIdx].score {
				minIdx = j
			}
		}
		scored[i], scored[minIdx] = scored[minIdx], scored[i]
	}

	indices := make([]int, count)
	for i := 0; i < count; i++ {
		indices[i] = scored[i].idx
	}
	return b.connectionsByIndicesLocked(indices)
}

func (b *Balancer) connectionsByIndicesLocked(indices []int) []Connection {
	selected := make([]Connection, len(indices))
	for i, idx := range indices {
		if idx < 0 || idx >= len(b.connections) {
			continue
		}
		selected[i] = b.connections[idx]
	}
	return selected
}

func (b *Balancer) bestScoredConnectionLocked(scorer func(int) uint64) (Connection, bool) {
	ordered := b.rotatedActiveIndicesLocked(1)
	bestIndex := -1
	var bestScore uint64
	for _, idx := range ordered {
		score := scorer(idx)
		if bestIndex == -1 || score < bestScore {
			bestIndex = idx
			bestScore = score
		}
	}
	if bestIndex < 0 {
		return Connection{}, false
	}
	return b.connections[bestIndex], true
}

func (b *Balancer) bestScoredConnectionExcludingLocked(scorer func(int) uint64, excludeKey string) (Connection, bool) {
	ordered := b.rotatedActiveIndicesLocked(1)
	bestIndex := -1
	var bestScore uint64
	for _, idx := range ordered {
		if b.connections[idx].Key == excludeKey {
			continue
		}
		score := scorer(idx)
		if bestIndex == -1 || score < bestScore {
			bestIndex = idx
			bestScore = score
		}
	}
	if bestIndex < 0 {
		return Connection{}, false
	}
	return b.connections[bestIndex], true
}

func (b *Balancer) roundRobinBestConnectionLocked() (Connection, bool) {
	if len(b.activeIDs) == 0 {
		return Connection{}, false
	}
	pos := roundRobinStartIndex(b.rrCounter.Add(1)-1, len(b.activeIDs))
	return b.connections[b.activeIDs[pos]], true
}

func (b *Balancer) roundRobinBestConnectionExcludingLocked(excludeKey string) (Connection, bool) {
	if len(b.activeIDs) == 0 {
		return Connection{}, false
	}
	for _, idx := range b.rotatedActiveIndicesLocked(1) {
		if b.connections[idx].Key == excludeKey {
			continue
		}
		return b.connections[idx], true
	}
	return Connection{}, false
}

func (b *Balancer) rotatedActiveIndicesLocked(step int) []int {
	if len(b.activeIDs) == 0 {
		return nil
	}
	if step < 1 {
		step = 1
	}

	start := roundRobinStartIndex(b.rrCounter.Add(uint64(step))-uint64(step), len(b.activeIDs))
	ordered := make([]int, len(b.activeIDs))
	for i := range b.activeIDs {
		ordered[i] = b.activeIDs[(start+i)%len(b.activeIDs)]
	}
	return ordered
}

func roundRobinStartIndex(counter uint64, n int) int {
	if n <= 0 {
		return 0
	}
	return int(counter % uint64(n))
}

func (b *Balancer) hasLossSignalLocked() bool {
	for _, idx := range b.activeIDs {
		stats := b.stats[idx]
		if stats == nil {
			continue
		}
		sent, _, _, _ := stats.snapshot()
		if sent >= 5 {
			return true
		}
	}
	return false
}

func (b *Balancer) hasLatencySignalLocked() bool {
	for _, idx := range b.activeIDs {
		stats := b.stats[idx]
		if stats == nil {
			continue
		}
		_, _, _, count := stats.snapshot()
		if count >= 5 {
			return true
		}
	}
	return false
}

func (b *Balancer) lossScoreLocked(idx int) uint64 {
	if idx < 0 || idx >= len(b.stats) || b.stats[idx] == nil {
		return 500
	}
	sent, acked, _, _ := b.stats[idx].snapshot()
	if sent < 5 {
		return 500
	}
	if acked >= sent {
		return 0
	}
	return (sent - acked) * 1000 / sent
}

func (b *Balancer) latencyScoreLocked(idx int) uint64 {
	if idx < 0 || idx >= len(b.stats) || b.stats[idx] == nil {
		return 999000
	}
	_, _, sum, count := b.stats[idx].snapshot()
	if count < 5 {
		return 999000
	}
	return sum / count
}

func (s *connectionStats) snapshot() (sent uint64, acked uint64, rttMicrosSum uint64, rttCount uint64) {
	if s == nil {
		return 0, 0, 0, 0
	}

	s.mu.RLock()
	sent = s.sent
	acked = s.acked
	rttMicrosSum = s.rttMicrosSum
	rttCount = s.rttCount
	s.mu.RUnlock()
	return sent, acked, rttMicrosSum, rttCount
}

func (s *connectionStats) applyHalfLifeLocked() {
	if s == nil {
		return
	}
	if s.sent <= connectionStatsHalfLifeThreshold &&
		s.acked <= connectionStatsHalfLifeThreshold &&
		s.rttCount <= connectionStatsHalfLifeThreshold {
		return
	}

	s.sent /= 2
	s.acked /= 2
	s.rttMicrosSum /= 2
	s.rttCount /= 2
}

func (b *Balancer) nextRandom() uint64 {
	for {
		current := b.rngState.Load()
		next := xorshift64(current)
		if b.rngState.CompareAndSwap(current, next) {
			return next
		}
	}
}

func seedRNG() uint64 {
	seed := uint64(time.Now().UnixNano())
	if seed == 0 {
		return 0x9e3779b97f4a7c15
	}
	return seed
}

func xorshift64(v uint64) uint64 {
	if v == 0 {
		v = 0x9e3779b97f4a7c15
	}
	v ^= v << 13
	v ^= v >> 7
	v ^= v << 17
	return v
}
