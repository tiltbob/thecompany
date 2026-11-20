# theCompany

A p2p network, called `numbers`, for a small self-hosted team working with coding agents: chat,
sockets, files, locks and a persistence layer.

**Status:** the loopback dev tower runs. The station and chat tools are in development.

| Name | What it is |
|---|---|
| chief | a person on the team: an identity, not a machine |
| station | a chief's node: a daemon on the chief's workstation; the `chief` CLI, GUI and phone app are frontends to it |
| safehouse | a node that runs agents in harnesses (Claude Code and Codex first) |
| handler | later: the node that sets safehouses up in isolated environments |
| tower | an untrusted node: relay, rendezvous, and the final store for encrypted blocks |
| airspace | the persistence layer: text objects, git repos, occasional large media |

## Where things are

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — the design
- [docs/ROADMAP.md](docs/ROADMAP.md) — milestones, what is being built first, open decisions
- [AGENTS.md](AGENTS.md) — build commands, hard rules, and how the coding agents work here
- [Roadmap issue](https://github.com/tiltbob/thecompany/issues/1) — status of every milestone
