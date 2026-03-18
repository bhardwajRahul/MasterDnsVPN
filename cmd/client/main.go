// ==============================================================================
// MasterDnsVPN
// Author: MasterkinG32
// Github: https://github.com/masterking32
// Year: 2026
// ==============================================================================

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"masterdnsvpn-go/internal/client"
)

func main() {
	app, err := client.Bootstrap("client_config.toml")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Client startup failed: %v\n", err)
		os.Exit(1)
	}

	cfg := app.Config()
	log := app.Logger()
	log.Infof("🚀 <green>Client Configuration Loaded</green>")
	log.Infof(
		"🧭 <green>Client Mode</green> <magenta>|</magenta> <blue>Protocol</blue>: <cyan>%s</cyan> <magenta>|</magenta> <blue>Encryption</blue>: <magenta>%d</magenta>",
		cfg.ProtocolType,
		cfg.DataEncryptionMethod,
	)
	log.Infof(
		"⚖️ <green>Resolver Balancing</green> <magenta>|</magenta> <blue>Strategy</blue>: <magenta>%d</magenta>",
		cfg.ResolverBalancingStrategy,
	)
	log.Infof(
		"🌐 <green>Configured Domains</green> <magenta>|</magenta> <magenta>%d</magenta>",
		len(cfg.Domains),
	)
	log.Infof(
		"📡 <green>Loaded Resolvers</green> <magenta>|</magenta> <magenta>%d</magenta> <blue>endpoints</blue>",
		len(cfg.Resolvers),
	)
	log.Infof(
		"🧭 <green>Local DNS Listener</green> <magenta>|</magenta> <blue>Enabled</blue>: <yellow>%t</yellow> <magenta>|</magenta> <blue>Addr</blue>: <cyan>%s:%d</cyan>",
		cfg.LocalDNSEnabled,
		cfg.LocalDNSIP,
		cfg.LocalDNSPort,
	)
	log.Infof(
		"🗂️ <green>Connection Catalog</green> <magenta>|</magenta> <magenta>%d</magenta> <blue>domain-resolver pairs</blue>",
		len(app.Connections()),
	)
	log.Infof(
		"✅ <green>Active Connections</green> <magenta>|</magenta> <magenta>%d</magenta>",
		app.Balancer().ValidCount(),
	)

	if err := app.RunInitialMTUTests(); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Initial MTU testing failed: %v\n", err)
		os.Exit(1)
	}

	log.Infof(
		"📏 <green>Initial MTU Sync Completed</green> <magenta>|</magenta> <blue>Upload</blue>: <cyan>%d</cyan> <magenta>|</magenta> <blue>Download</blue>: <cyan>%d</cyan>",
		app.SyncedUploadMTU(),
		app.SyncedDownloadMTU(),
	)

	if err := app.InitializeSession(10); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Session initialization failed: %v\n", err)
		os.Exit(1)
	}

	log.Infof(
		"🤝 <green>Session Established</green> <magenta>|</magenta> <blue>ID</blue>: <cyan>%d</cyan> <magenta>|</magenta> <blue>Cookie</blue>: <magenta>%d</magenta>",
		app.SessionID(),
		app.SessionCookie(),
	)
	log.Infof("🎯 <green>Client Bootstrap Ready</green>")

	if !cfg.LocalDNSEnabled {
		return
	}

	runCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := app.RunLocalDNSListener(runCtx); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Local DNS listener failed: %v\n", err)
		os.Exit(1)
	}
}
