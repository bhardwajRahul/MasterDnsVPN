// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package client

import (
	"context"
	"net"
	"sync"

	DnsParser "masterdnsvpn-go/internal/dnsparser"
)

type localDNSRequest struct {
	packet []byte
	addr   *net.UDPAddr
}

func (c *Client) RunLocalDNSListener(ctx context.Context) error {
	if c == nil || !c.cfg.LocalDNSEnabled {
		return nil
	}

	conn, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.ParseIP(c.cfg.LocalDNSIP),
		Port: c.cfg.LocalDNSPort,
	})
	if err != nil {
		return err
	}
	defer conn.Close()

	c.log.Infof(
		"📡 <green>Local DNS Listener Ready</green> <magenta>|</magenta> <blue>Addr</blue>: <cyan>%s:%d</cyan> <magenta>|</magenta> <blue>Workers</blue>: <magenta>%d</magenta>",
		c.cfg.LocalDNSIP,
		c.cfg.LocalDNSPort,
		c.cfg.LocalDNSWorkers,
	)

	queue := make(chan localDNSRequest, c.cfg.LocalDNSQueueSize)
	var workerWG sync.WaitGroup
	for range c.cfg.LocalDNSWorkers {
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			c.localDNSWorker(ctx, conn, queue)
		}()
	}

	go func() {
		<-ctx.Done()
		_ = conn.Close()
	}()

	buffer := make([]byte, EDnsSafeUDPSize)
	for {
		n, addr, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			return err
		}

		packet := append([]byte(nil), buffer[:n]...)
		select {
		case queue <- localDNSRequest{packet: packet, addr: addr}:
		case <-ctx.Done():
			close(queue)
			workerWG.Wait()
			return nil
		default:
			response, responseErr := DnsParser.BuildRefusedResponse(packet)
			if responseErr == nil {
				_, _ = conn.WriteToUDP(response, addr)
			}
		}
	}

	close(queue)
	workerWG.Wait()
	return nil
}

func (c *Client) localDNSWorker(ctx context.Context, conn *net.UDPConn, queue <-chan localDNSRequest) {
	for {
		select {
		case <-ctx.Done():
			return
		case req, ok := <-queue:
			if !ok {
				return
			}
			response, err := DnsParser.BuildRefusedResponse(req.packet)
			if err == nil {
				_, _ = conn.WriteToUDP(response, req.addr)
			}
		}
	}
}
