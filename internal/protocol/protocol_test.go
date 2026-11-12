package protocol

import "testing"

func TestIsStructural(t *testing.T) {
	structural := []OpCode{
		"REGISTER",
		"HEARTBEAT",
		"QUIESCE",
		"QUIESCED",
		"DEPLOY",
		"GET_CLUSTER_STATUSES",
		"START_INSTANCE",
		"STOP_INSTANCE",
		"RESTART_INSTANCE",
		"REMOVE_INSTANCE",
		"SYNC_INSTANCES",
		"GET_INSTANCE_STATUSES",
	}
	for _, op := range structural {
		if !IsStructural(op) {
			t.Errorf("IsStructural(%q) = false, want true", op)
		}
	}

	notStructural := []OpCode{
		"REFRESH_WHITELABEL_GUILDS",
		"CLOSE_ALL_TICKETS",
		"GET_GUILD_LIST",
	}
	for _, op := range notStructural {
		if IsStructural(op) {
			t.Errorf("IsStructural(%q) = true, want false", op)
		}
	}
}
