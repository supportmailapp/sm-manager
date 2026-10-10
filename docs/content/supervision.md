---
sidebar:
  order: 5
---

# Process Supervision

The manager spawns and babysits the cluster processes (`internal/supervisor`). On startup it launches `clusterCount` children via `exec.CommandContext(spawnCommand...)` in `spawnDir`, each getting the shard range for its id.

## Env contract

Injected per child (this is the whole interface to the bot's `cluster.ts` — it reads these):

| Env var             | Example        | Meaning                                                                                                                                 |
| ------------------- | -------------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `CLUSTER_ID`        | `1`            | This cluster's id                                                                                                                       |
| `SHARD_LIST`        | `2,3`          | Comma-separated shard ids this cluster hosts                                                                                            |
| `TOTAL_SHARDS`      | `4`            | Full shard count across every cluster                                                                                                   |
| `SM_MANAGER_SOCKET` | `/tmp/...sock` | Socket path to connect back to (= `socketPath`)                                                                                         |
| `GENERATION`        | `0`            | Epoch this cluster was spawned under. The cluster echoes it back in its `REGISTER` payload so the manager can tag it — see Generations. |

These five are **appended to the manager's own `os.Environ()`** — they are the only
variables the manager sets. Everything else the bot needs (`BOT_TOKEN`,
`MONGODB_URI`, `NODE_ENV`, …) has to reach the child some other way, and that is a
sharp edge worth stating plainly:

- The manager does **not** set `NODE_ENV`. Bun picks its env file from `NODE_ENV`,
  so a bare `bun run src/cluster.ts` loads `.env.development` — while the bot's
  `src/config.ts` reads `NODE_ENV` directly and treats anything that isn't the
  literal `"development"` as production. An unset `NODE_ENV` therefore pairs
  `prod.config.json` with the *dev* `.env` file, which usually shows up as a
  gateway connect failure (a wrong or stale `BOT_TOKEN`) rather than a config error.
- So `spawnCommand` must load the env file itself —
  `["bun", "run", "--env-file=.env.production", "src/cluster.ts"]` — and that file
  must contain `NODE_ENV=production`. `--env-file` replaces Bun's automatic
  resolution entirely (plain `.env` is not merged in), so it is the single source
  of truth for the child. Exporting `NODE_ENV` in the manager's own environment
  (systemd `Environment=`) works too, since the child inherits it.

The bot refuses to boot when `NODE_ENV` is unset or `BOT_TOKEN`/`MONGODB_URI` are
missing (`src/cluster.ts`), so a misconfigured spawn exits 1 with the reason named
instead of idling.

Child stdout/stderr is captured and re-logged line-prefixed (`[cluster N out]` / `[cluster N err]`), so interleaved output stays attributable.

## Crash handling

`cmd.Wait()` returns on any exit (crash or clean). The child is restarted with **exponential backoff** — `1s`, doubling to a `30s` cap. A process that stays up past `healthyRuntime` (60s) is treated as healthy and its backoff resets, so a genuine crash loop backs off but an occasional restart doesn't. Because children run under the manager's `ctx` (`CommandContext`), a manager shutdown (SIGINT/SIGTERM) tears down every child; the manager then waits for them before exiting.
