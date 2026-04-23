package net

import "generic-p2p/internal/core"

type PeerState struct {
	PeerID       string           `json:"peerId"`
	Addrs        []string         `json:"addrs"`
	IsLAN        bool             `json:"isLan"`
	RelayAllowed bool             `json:"relayAllowed"`
	HaveRanges   []core.HaveRange `json:"haveRanges"`
	Score        float64          `json:"score"`
	LastSeenAt   int64            `json:"lastSeenAt"`
}
