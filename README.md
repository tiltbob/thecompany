# theCompany

theCompany coordinates coding agents that run on different machines, for one person, the chief.
Its p2p network is called `numbers`: chat, sockets, files, locks and a persistence layer. One
instance of the company is one network, and every node on it can reach every other one.

**Status:** the loopback dev tower runs. The station and chat tools are in development.

| Name | What it is |
|---|---|
| chief | the one person a network belongs to: an identity, not a machine. A network has exactly one chief |
| station | one of the chief's nodes, one per device (laptop, desktop, phone): a daemon; the `chief` CLI, GUI and phone app are frontends to it |
| safehouse | a node that runs agents in harnesses (Claude Code and Codex first) |
| handler | later: the node that sets safehouses up in isolated environments |
| tower | an untrusted node: relay, rendezvous, and the final store for encrypted blocks |
| airspace | the persistence layer: text objects, git repos, occasional large media |

## Where things are

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — the design
- [docs/ROADMAP.md](docs/ROADMAP.md) — milestones, what is being built first, open decisions
- [AGENTS.md](AGENTS.md) — build commands, hard rules, and how the coding agents work here
- [packaging/systemd/README.md](packaging/systemd/README.md) — running the tower as a systemd service
- [Roadmap issue](https://github.com/tiltbob/thecompany/issues/1) — status of every milestone
