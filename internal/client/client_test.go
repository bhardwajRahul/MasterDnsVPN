// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package client

import (
	"encoding/binary"
	"testing"

	"masterdnsvpn-go/internal/compression"
	"masterdnsvpn-go/internal/config"
	Enums "masterdnsvpn-go/internal/enums"
	VpnProto "masterdnsvpn-go/internal/vpnproto"
)

func TestBuildConnectionMap(t *testing.T) {
	cfg := config.ClientConfig{
		ProtocolType: "SOCKS5",
		Domains: []string{
			"a.example.com",
			"b.example.com",
		},
		Resolvers: []config.ResolverAddress{
			{IP: "8.8.8.8", Port: 53},
			{IP: "2001:4860:4860::8888", Port: 5353},
		},
	}

	c := New(cfg, nil, nil)
	c.BuildConnectionMap()

	if got, want := len(c.Connections()), 4; got != want {
		t.Fatalf("unexpected connection count: got=%d want=%d", got, want)
	}

	first := c.Connections()[0]
	if first.Domain == "" || first.Resolver == "" || first.Key == "" {
		t.Fatalf("connection fields should be populated: %+v", first)
	}
	if !first.IsValid {
		t.Fatalf("connections should start valid")
	}
	if first.Resolver == "2001:4860:4860::8888" && first.ResolverLabel != "[2001:4860:4860::8888]:5353" {
		t.Fatalf("unexpected ipv6 resolver label: got=%q", first.ResolverLabel)
	}
	if c.Balancer().ValidCount() != 4 {
		t.Fatalf("unexpected valid connection count: got=%d want=%d", c.Balancer().ValidCount(), 4)
	}
}

func TestResetRuntimeState(t *testing.T) {
	c := New(config.ClientConfig{}, nil, nil)
	c.sessionID = 11
	c.sessionCookie = 22
	c.enqueueSeq = 33

	c.ResetRuntimeState(false)
	if c.sessionID != 0 || c.enqueueSeq != 0 {
		t.Fatalf("reset should clear session id and enqueue seq: sid=%d enqueue=%d", c.sessionID, c.enqueueSeq)
	}
	if c.sessionCookie != 22 {
		t.Fatalf("reset without cookie reset should preserve session cookie: got=%d", c.sessionCookie)
	}

	c.ResetRuntimeState(true)
	if c.sessionCookie != 0 {
		t.Fatalf("reset with cookie reset should clear session cookie: got=%d", c.sessionCookie)
	}
}

func TestSetConnectionValidityKeepsClientAndBalancerInSync(t *testing.T) {
	cfg := config.ClientConfig{
		Domains: []string{"a.example.com"},
		Resolvers: []config.ResolverAddress{
			{IP: "8.8.8.8", Port: 53},
		},
	}

	c := New(cfg, nil, nil)
	c.BuildConnectionMap()
	key := c.Connections()[0].Key

	if !c.SetConnectionValidity(key, false) {
		t.Fatal("SetConnectionValidity returned false")
	}
	if c.Connections()[0].IsValid {
		t.Fatal("client connection validity was not updated")
	}
	if got := c.Balancer().ValidCount(); got != 0 {
		t.Fatalf("unexpected valid count after disable: got=%d want=0", got)
	}

	if !c.SetConnectionValidity(key, true) {
		t.Fatal("SetConnectionValidity returned false when re-enabling")
	}
	if !c.Connections()[0].IsValid {
		t.Fatal("client connection validity was not restored")
	}
	if got := c.Balancer().ValidCount(); got != 1 {
		t.Fatalf("unexpected valid count after enable: got=%d want=1", got)
	}
}

func TestBuildSessionInitPayloadLayout(t *testing.T) {
	c := New(config.ClientConfig{
		BaseEncodeData:          true,
		UploadCompressionType:   2,
		DownloadCompressionType: 1,
	}, nil, nil)
	c.syncedUploadMTU = 150
	c.syncedDownloadMTU = 200

	payload, useBase64, verifyCode, err := c.buildSessionInitPayload()
	if err != nil {
		t.Fatalf("buildSessionInitPayload returned error: %v", err)
	}
	if !useBase64 {
		t.Fatal("expected base64 response mode")
	}
	if len(payload) != 10 {
		t.Fatalf("unexpected payload len: got=%d want=10", len(payload))
	}
	if payload[0] != 1 {
		t.Fatalf("unexpected response mode byte: got=%d want=1", payload[0])
	}
	if payload[1] != 0x21 {
		t.Fatalf("unexpected compression pair: got=%#x want=%#x", payload[1], 0x21)
	}
	if got := int(binary.BigEndian.Uint16(payload[2:4])); got != 150 {
		t.Fatalf("unexpected upload mtu: got=%d want=150", got)
	}
	if got := int(binary.BigEndian.Uint16(payload[4:6])); got != 200 {
		t.Fatalf("unexpected download mtu: got=%d want=200", got)
	}
	if string(payload[6:10]) != string(verifyCode[:]) {
		t.Fatalf("unexpected verify code bytes: got=%v want=%v", payload[6:10], verifyCode)
	}
}

func TestValidateServerPacketAllowsPreSessionResponses(t *testing.T) {
	c := New(config.ClientConfig{}, nil, nil)
	if !c.validateServerPacket(VpnProto.Packet{PacketType: Enums.PACKET_MTU_UP_RES}) {
		t.Fatal("pre-session mtu-up response should be accepted")
	}
	if !c.validateServerPacket(VpnProto.Packet{PacketType: Enums.PACKET_MTU_DOWN_RES}) {
		t.Fatal("pre-session mtu-down response should be accepted")
	}
	if !c.validateServerPacket(VpnProto.Packet{PacketType: Enums.PACKET_SESSION_ACCEPT}) {
		t.Fatal("pre-session session-accept should be accepted")
	}
}

func TestValidateServerPacketRequiresMatchingSessionCookie(t *testing.T) {
	c := New(config.ClientConfig{}, nil, nil)
	c.sessionID = 7
	c.sessionCookie = 55

	valid := VpnProto.Packet{
		SessionID:     7,
		SessionCookie: 55,
		PacketType:    Enums.PACKET_PONG,
	}
	if !c.validateServerPacket(valid) {
		t.Fatal("matching session packet should be accepted")
	}

	wrongCookie := valid
	wrongCookie.SessionCookie = 66
	if c.validateServerPacket(wrongCookie) {
		t.Fatal("packet with wrong session cookie should be rejected")
	}

	wrongSession := valid
	wrongSession.SessionID = 8
	if c.validateServerPacket(wrongSession) {
		t.Fatal("packet with wrong session id should be rejected")
	}
}

func TestApplySessionCompressionPolicyDisablesSmallMTUDirections(t *testing.T) {
	c := New(config.ClientConfig{
		CompressionMinSize: compression.DefaultMinSize,
	}, nil, nil)
	c.syncedUploadMTU = compression.DefaultMinSize
	c.syncedDownloadMTU = compression.DefaultMinSize - 1
	c.uploadCompression = compression.TypeZLIB
	c.downloadCompression = compression.TypeZLIB

	c.applySessionCompressionPolicy()

	if c.uploadCompression != compression.TypeOff {
		t.Fatalf("upload compression should be disabled, got=%d", c.uploadCompression)
	}
	if c.downloadCompression != compression.TypeOff {
		t.Fatalf("download compression should be disabled, got=%d", c.downloadCompression)
	}
}

func TestApplySessionCompressionPolicyKeepsLargeMTUDirections(t *testing.T) {
	c := New(config.ClientConfig{
		CompressionMinSize: compression.DefaultMinSize,
	}, nil, nil)
	c.syncedUploadMTU = compression.DefaultMinSize + 1
	c.syncedDownloadMTU = compression.DefaultMinSize + 50
	c.uploadCompression = compression.TypeZLIB
	c.downloadCompression = compression.TypeOff

	c.applySessionCompressionPolicy()

	if c.uploadCompression != compression.TypeZLIB {
		t.Fatalf("upload compression should stay enabled, got=%d", c.uploadCompression)
	}
	if c.downloadCompression != compression.TypeOff {
		t.Fatalf("download compression should stay off, got=%d", c.downloadCompression)
	}
}

func TestNewKeepsLocalDNSDefaults(t *testing.T) {
	c := New(config.ClientConfig{
		LocalDNSEnabled:   true,
		LocalDNSIP:        "127.0.0.1",
		LocalDNSPort:      5353,
		LocalDNSWorkers:   2,
		LocalDNSQueueSize: 512,
	}, nil, nil)

	if !c.cfg.LocalDNSEnabled {
		t.Fatal("expected local dns listener to stay enabled in config")
	}
	if c.cfg.LocalDNSIP != "127.0.0.1" || c.cfg.LocalDNSPort != 5353 {
		t.Fatalf("unexpected local dns bind config: %s:%d", c.cfg.LocalDNSIP, c.cfg.LocalDNSPort)
	}
}
