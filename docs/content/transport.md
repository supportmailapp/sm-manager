---
sidebar:
  order: 2
---

# How the Transport Works

The manager listens on a **Unix domain socket** — a special file on disk, not a network port. `net.Listen("unix", path)` creates that file; any process that opens the same path is talking to the manager. It behaves like TCP but is local-only: the kernel moves bytes between processes, no network stack, and access is controlled by filesystem permissions rather than a firewall.

Every peer connects to the **same socket path**:

```txt
          /tmp/sm-manager.sock   (the rendezvous file)
                    │
   ┌────────────────┼─────────────────┐
   │                │                 │
 cluster 0       cluster 1          sm-api
 (bot proc)      (bot proc)        (client)
```

- **Manager** creates + listens on the path.
- **Clusters** (`cluster.ts`) `connect()` to it and send `REGISTER`.
- **API** connects to it as a plain client and sends business ops.

All three must name the **same path** — that path is the whole address, which is exactly why it lives in config (see Configuration) instead of being hardcoded: it must match on every side, and it differs per environment (WSL dev vs. systemd prod vs. a Docker volume mount).

On startup the manager runs `os.Remove(path)` before binding, to clear a stale socket file left behind by a previous crash (otherwise `bind` fails with "address in use"). On clean shutdown it removes the file again.

## Message framing

Each message is a length-prefixed JSON frame: a **4-byte big-endian uint32 length** followed by that many bytes of JSON. Frames are capped at 16 MiB (`internal/protocol/wire.go`) so a bad/hostile length prefix can't force an unbounded allocation. Both directions use the same framing.

## Request / response correlation

Two frame shapes on the wire (see `PROTOCOL.md`):

- **Request** — `{ id, op, target?, payload }`
- **Response** — `{ id, ok, data?, error? }`

The manager tells them apart cheaply: a frame with an `op` field is a request, otherwise it's a response (`FrameKind` probe in `internal/protocol`). Responses are matched back to the request that caused them by `id`.

When a client (e.g. the API) sends a business op, the manager records `pending[id] → originating conn`, forwards the frame to the target cluster untouched, and when the cluster replies with the same `id`, routes that reply back to the original client. If a peer disconnects, its pending entries are purged (their replies can never arrive).

## Target routing

`resolveTarget` (`internal/server/hub.go`) picks the destination cluster in priority order — **no stored shard table**, the shard→cluster step is computed from topology:

1. `target.clusterId` — explicit, used as-is.
2. `target.shardId` — mapped to its owning cluster via `shardId / shardsPerCluster` (clusters own contiguous ranges: cluster _c_ owns shards `[c·shardsPerCluster, (c+1)·shardsPerCluster)`).
3. `target.guildId` — mapped to a shard with Discord's gateway formula `(guildID >> 22) % totalShards`, then that shard → cluster as above.

A target that resolves to no connected cluster is rejected to the sender with `{ok:false, error}`.
