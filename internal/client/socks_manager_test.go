package client

import (
	"io"
	"net"
	"testing"

	"masterdnsvpn-go/internal/config"
)

func TestSupportsSOCKS4Policy(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.ClientConfig
		want bool
	}{
		{
			name: "auth disabled supports socks4",
			cfg:  config.ClientConfig{SOCKS5Auth: false},
			want: true,
		},
		{
			name: "auth enabled with username and password disables socks4",
			cfg:  config.ClientConfig{SOCKS5Auth: true, SOCKS5User: "user", SOCKS5Pass: "pass"},
			want: false,
		},
		{
			name: "auth enabled with username only supports socks4",
			cfg:  config.ClientConfig{SOCKS5Auth: true, SOCKS5User: "user"},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{cfg: tt.cfg}
			if got := c.supportsSOCKS4(); got != tt.want {
				t.Fatalf("supportsSOCKS4() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSendSocks4ReplyFormatsResponse(t *testing.T) {
	c := &Client{}
	server, clientConn := net.Pipe()
	defer server.Close()
	defer clientConn.Close()

	done := make(chan error, 1)
	go func() {
		done <- c.sendSocks4Reply(server, true)
	}()

	reply := make([]byte, 8)
	if _, err := io.ReadFull(clientConn, reply); err != nil {
		t.Fatalf("failed to read SOCKS4 reply: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("sendSocks4Reply returned error: %v", err)
	}

	want := []byte{0x00, SOCKS4_REPLY_GRANTED, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	for i := range want {
		if reply[i] != want[i] {
			t.Fatalf("reply[%d] = 0x%02x, want 0x%02x", i, reply[i], want[i])
		}
	}
}
