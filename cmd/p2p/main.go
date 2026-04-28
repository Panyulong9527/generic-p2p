package main

import (
	"fmt"
	"os"

	"generic-p2p/internal/logging"
)

type peerDiscoveryOptions struct {
	contentID         string
	explicitPeer      string
	explicitPeers     string
	explicitUDPPeer   string
	explicitUDPPeers  string
	lanEnabled        bool
	lanAddr           string
	trackerURL        string
	stunServer        string
	selfListenAddr    string
	selfUDPListenAddr string
}

func main() {
	logger := logging.NewJSONLogger(os.Stdout, logging.ParseLevel(os.Getenv("P2P_LOG_LEVEL")))

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	command := os.Args[1]
	args := os.Args[2:]

	var err error
	switch command {
	case "share":
		err = runShare(logger, args)
	case "tracker":
		err = runTracker(logger, args)
	case "serve":
		err = runServe(logger, args)
	case "get":
		err = runGet(logger, args)
	case "status":
		err = runStatus(args)
	case "tracker-status":
		err = runTrackerStatus(args)
	default:
		err = fmt.Errorf("unknown command %q", command)
	}

	if err != nil {
		logger.Error("command_failed", "command", command, "error", err.Error())
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`p2p commands:
  p2p share --path ./file.bin [--piece-size 1048576] [--data-dir .p2p]
  p2p tracker --listen 127.0.0.1:7000 [--state-file .p2p-tracker-state.json] [--web-data-dir .p2p-web] [--web-users-file users.json] [--peer-ttl 10s] [--cleanup-interval 2s]
  p2p tracker-status --tracker http://127.0.0.1:7000 [--pretty] [--watch] [--interval 1s]
  p2p serve --path ./file.bin [--listen 127.0.0.1:9001] [--udp-listen 127.0.0.1:9003] [--stun stun.l.google.com:19302] [--data-dir .p2p] [--lan] [--tracker http://127.0.0.1:7000]
  p2p get --manifest .p2p/<contentId>/manifest.json --store-dir .p2p-store --out ./out.bin [--peer 127.0.0.1:9001] [--udp-peer 127.0.0.1:9003]
  p2p get --manifest .p2p/<contentId>/manifest.json --store-dir .p2p-store --out ./out.bin --listen 127.0.0.1:9002 [--udp-listen 127.0.0.1:9004] [--stun stun.l.google.com:19302] [--seed-after-download] [--peers ...] [--udp-peers ...] [--lan] [--tracker http://127.0.0.1:7000]
  p2p status --manifest .p2p/<contentId>/manifest.json --store-dir .p2p-store [--pretty] [--watch] [--interval 1s]`)
}
