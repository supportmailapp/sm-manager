---
sidebar:
  order: 8
---

# Deployment (systemd)

How to run `sm-manager` as a long-lived service on a Linux server. For what every config key means, see Configuration and Running — this page is the server-setup runbook.

## 1. Build a release binary

The repo is cloned directly into the deploy directory, `~/smmanager` — checkout, built binary, and `config.json` all live side by side there (§2). `/sm-manager` is gitignored, so building in place is clean, and safe even while `sm-manager` is currently running: `go build -o` writes the new binary then renames it into place, so the running process keeps its old inode open until the next restart.

As the `suppmail` user:

```bash
cd ~/smmanager
git pull
go build -o sm-manager ./cmd/sm-manager
```

`go build` automatically embeds the git commit into the binary (read back via `debug.ReadBuildInfo` in `cmd/sm-manager/main.go`) — no `-ldflags` needed, and this is used as the Sentry `release` when `sentryRelease` isn't set in `config.json`.

## 2. Lay out the server directory

The shipped unit file (`deploy/sm-manager.service`) assumes this layout under a dedicated `suppmail` user:

```txt
/home/suppmail/smmanager/
  .git/, cmd/, internal/, ...   # the repo checkout
  sm-manager                    # the built binary (gitignored)
  config.json                   # this environment's config — see Configuration and Running (gitignored)
```

One-time setup, as root:

```bash
sudo useradd --system --home /home/suppmail --create-home suppmail
```

Then, as `suppmail`:

```bash
git clone <repo-url> ~/smmanager   # first time only
cd ~/smmanager
cp config.example.json config.json   # first time only
go build -o sm-manager ./cmd/sm-manager
```

Edit `/home/suppmail/smmanager/config.json` for this environment — at minimum `spawnCommand`/`spawnDir` (pointing at the bot checkout), `clusterCount`/`shardsPerCluster`, `allowedBusinessOps`, and — if the bot needs Mongo, hence the unit's `mongod.service` dependency — whatever the spawned cluster process itself requires (`sm-manager` has no DB dependency of its own; only the child bot process does). To send errors/logs/metrics to Sentry, also set `sentryDsn` and `sentryEnvironment` (`"production"`) here — no separate env file needed, `sentryDsn` empty just disables Sentry.

## 3. Install the systemd unit

```bash
sudo cp deploy/sm-manager.service /etc/systemd/system/sm-manager.service
sudo systemctl daemon-reload
sudo systemctl enable --now sm-manager
```

Check it came up:

```bash
sudo systemctl status sm-manager
sudo journalctl -u sm-manager -f
```

You should see the spawn + register lines described in Configuration and Running.

## 4. What to do when X changes

Different parts of the system need different update paths — using the wrong one either does nothing or causes an unnecessary outage. Find the section that matches what changed.

### `sm-manager`'s own Go code

1. `cd ~/smmanager && git pull`
2. `go build -o sm-manager ./cmd/sm-manager` (§1 — safe to run while the old process is still up; the rename swaps the file, it doesn't touch the running process's open inode)
3. `sudo systemctl restart sm-manager`

A hard restart — every running cluster child dies with the manager (they run under its `ctx`) and respawns fresh. There is no live-upgrade path for the manager's own binary.

Save as `~/redeploy.sh` (as `suppmail`, `chmod +x` once) to do all three in one shot:

```bash
#!/usr/bin/env bash
set -euo pipefail

cd ~/smmanager
git pull
go build -o sm-manager ./cmd/sm-manager
sudo systemctl restart sm-manager
sudo systemctl status sm-manager --no-pager
```

### `config.json` values

Topology, `allowedBusinessOps`, status-WS/rate-limit knobs, `sentryDsn`/`sentryEnvironment`, …

1. Edit `/home/suppmail/smmanager/config.json`
2. `sudo systemctl restart sm-manager`

Config is only read once at process start (`config.Load()`); nothing re-reads the file at runtime. Same hard restart as above — no rebuild needed since nothing but data changed.

### The bot/cluster code (e.g. `v2`'s `cluster.ts`)

Trigger `DEPLOY` — don't touch `sm-manager` at all. See Generations, Readiness Gating, and DEPLOY.

This is exactly what `DEPLOY` is for: a zero-downtime, cluster-by-cluster rollout to a new generation. Restarting `sm-manager` for a bot-only change is unnecessary and drops every cluster at once instead of rolling them.

### The systemd unit file (`deploy/sm-manager.service`)

1. `sudo cp deploy/sm-manager.service /etc/systemd/system/sm-manager.service`
2. `sudo systemctl daemon-reload`
3. `sudo systemctl restart sm-manager`

`daemon-reload` is required before systemd picks up unit-file changes — a bare `restart` alone keeps using the old unit in memory.

### `PROTOCOL.md` / the op-code contract

1. Update `PROTOCOL.md` and its matching struct/type file in **all three** repos (`sm-manager`, `v2`/`supportmail`, `sm-api`) in the same sitting
2. Rebuild and redeploy `sm-manager` per the Go-code section above
3. Redeploy `v2` and `sm-api` per their own processes

There's no codegen or shared package for this yet — the contract is hand-synced. A mismatch surfaces as `{ok:false, error:"unknown op"}` rejections at the manager, not a build error, so all three must move together.

## Notes

- The unit's `ExecStart` runs the binary directly with no shell, so `spawnCommand` in `config.json` (not the unit file) is where the bot's own process is launched — see the env contract in Process Supervision.
- `Restart=always` covers `sm-manager` itself exiting for any reason (an explicit `systemctl stop` is still respected); it's separate from `sm-manager`'s own crash-restart of the _cluster_ child processes (see Process Supervision) — the two operate at different levels.
- `mongod` is `Wants=`, not `Requires=`: with `Requires=`, a `systemctl stop mongod` (e.g. a package upgrade) stops `sm-manager` and the whole bot, and a later `start mongod` does not bring it back. Mongoose reconnects on its own. Same reason `sm-api.service` should only `Wants=` `sm-manager`.
