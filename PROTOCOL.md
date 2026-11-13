# PROTOCOL.md has moved

The op-code wire contract now has one canonical copy, in the `shared` repo (`@supportmailapp/shared`). Do not copy it back into this repo.

- Local workspace: [`../shared/PROTOCOL.md`](../shared/PROTOCOL.md)
- GitHub: <https://github.com/supportmailapp/shared/blob/main/PROTOCOL.md>

Go cannot import the package, so `internal/protocol/protocol.go` stays a hand-mirror of that doc.
