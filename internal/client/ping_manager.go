package client

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const (
	pingAggressiveInterval = 300 * time.Millisecond // Normal/Busy: 0.3s
	pingLazyInterval       = 1 * time.Second        // Idle > 5s: 1s
	pingCoolDownInterval   = 3 * time.Second        // Idle > 10s: 3s
	pingColdInterval       = 30 * time.Second       // Idle > 35s: 30s
)

// PingManager handles the adaptive keep-alive ping logic.
// It adjusts frequency based on real data traffic (non-PING/PONG).
type PingManager struct {
	client           *Client
	lastDataActivity atomic.Int64 // UnixNano
	lastPingTime     atomic.Int64 // UnixNano

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	wakeCh chan struct{}
}

func newPingManager(client *Client) *PingManager {
	now := time.Now().UnixNano()
	p := &PingManager{
		client: client,
		wakeCh: make(chan struct{}, 1),
	}
	p.lastDataActivity.Store(now)
	p.lastPingTime.Store(now)
	return p
}

// Start starts the autonomous ping loop.
func (p *PingManager) Start(parentCtx context.Context) {
	p.Stop() // Ensure old one is stopped

	p.ctx, p.cancel = context.WithCancel(parentCtx)
	p.wg.Add(1)
	go p.pingLoop()
}

// Stop stops the ping loop.
func (p *PingManager) Stop() {
	if p.cancel != nil {
		p.cancel()
		p.wg.Wait()
		p.cancel = nil
	}
}

// NotifyDataActivity tells the manager that real business data was exchanged.
// This forces the manager into Aggressive mode and wakes it up.
func (p *PingManager) NotifyDataActivity() {
	if p == nil {
		return
	}
	p.lastDataActivity.Store(time.Now().UnixNano())

	// Wake up the loop immediately if it's sleeping for a long interval.
	select {
	case p.wakeCh <- struct{}{}:
	default:
	}
}

func (p *PingManager) pingLoop() {
	defer p.wg.Done()

	p.client.log.Debugf("\U0001F3D3 <cyan>Ping Manager loop started</cyan>")
	timer := time.NewTimer(pingAggressiveInterval)
	defer timer.Stop()

	for {
		select {
		case <-p.ctx.Done():
			return
		case <-p.wakeCh:
			// Woken up by data activity! Re-evaluate immediately.
		case <-timer.C:
			// Timer fired.
		}

		now := time.Now()
		lastActivity := time.Unix(0, p.lastDataActivity.Load())
		idleTime := now.Sub(lastActivity)

		// 1. Determine the interval based on idle time.
		var interval time.Duration
		switch {
		case idleTime < 5*time.Second:
			interval = pingAggressiveInterval
		case idleTime < 10*time.Second:
			interval = pingLazyInterval
		case idleTime < 35*time.Second:
			interval = pingCoolDownInterval
		default:
			interval = pingColdInterval
		}

		// 2. Check if it's time to ping.
		lastPing := time.Unix(0, p.lastPingTime.Load())
		if now.Sub(lastPing) >= interval {
			if p.client.SessionReady() {
				// We use stream0's existing QueuePing to put the packet into ARQ.
				if p.client.stream0Runtime != nil && p.client.stream0Runtime.QueuePing() {
					p.lastPingTime.Store(now.UnixNano())
				}
			}
		}

		// 3. Reset timer for next check.
		// If we are idle, we check less frequently to save CPU.
		checkInterval := interval / 2
		if checkInterval < 100*time.Millisecond {
			checkInterval = 100 * time.Millisecond
		}
		if checkInterval > 1*time.Second {
			checkInterval = 1 * time.Second
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(checkInterval)
	}
}
