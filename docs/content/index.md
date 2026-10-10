---
title: SM Manager Documentation
description: Go process supervisor + IPC hub for the SupportMail sharding system.
---

# SM Manager Documentation

Welcome traveller, hope you're having a lovely day. Welcome to the SM Manager documentation!

## What is it?

Go process supervisor + IPC hub for the SupportMail sharding system. It supervises cluster processes (each hosting several discord.js gateway shards), routes messages between clusters and clients (the API) over a Unix socket, drives zero-downtime soft restarts, and hosts the public statuspage WebSocket.

The manager is **bot-agnostic on purpose**. It only understands a handful of structural ops; everything bot-specific is either config or lives in the cluster/API. A second bot is a different config, not a code change. See `PROTOCOL.md` for the wire contract.

## Documentation map

- **Transport** — the Unix socket, message framing, request/response correlation, target routing
- **Two tiers of ops** — the structural/business split that makes the manager reusable, and where validation lives
- **Configuration and running** — every config key + env override, and how to run it
- **Process supervision** — spawning, the env contract to the bot, crash backoff
- **Generations** — epoch tagging, readiness gating, and the batched `DEPLOY` rollout
- **Public statuspage WebSocket** — the internet-facing status feed, its auth, and rate-limiting
- **Layout** — package/file map of the repo
