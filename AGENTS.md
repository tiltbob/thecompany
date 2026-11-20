# Working in this repository

Instructions for coding agents (Claude Code, Codex, others) and for people. Claude Code reads this
file through `CLAUDE.md`.

## What this is

**theCompany** is a p2p network, called `numbers`, for a small self-hosted team working with coding
agents: chat, sockets, files, locks and a persistence layer.

| Name | What it is |
|---|---|
| chief | a person on the team: an identity, not a machine |
| station | a chief's node: a daemon on the chief's workstation; the `chief` CLI, GUI and phone app are frontends to it |
| safehouse | a node that runs agents in harnesses (Claude Code and Codex first) |
| handler | later: the node that sets safehouses up in isolated environments |
| tower | an untrusted node: relay, rendezvous, and the final store for encrypted blocks |
| airspace | the persistence layer: text objects, git repos, occasional large media |

Read before you change anything:

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — the design, and an appendix of facts about iroh and
  the harness CLIs that were verified against their sources. Trust the appendix over memory.
- [docs/ROADMAP.md](docs/ROADMAP.md) — milestone table, what M0 and M1 contain in detail, how they
  are verified, and the open decisions.
- Status lives in GitHub issues: the [roadmap](https://github.com/tiltbob/thecompany/issues/1) and
  the [coordination issue](https://github.com/tiltbob/thecompany/issues/2).

## Build and test

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo check -p numbers --locked     # the exact iroh feature set, without workspace feature unification
```

The toolchain is pinned in `rust-toolchain.toml`. `Cargo.lock` is committed. Tests run in-process on
127.0.0.1 and must not touch the external network.

## Hard rules

1. **Agent confinement.** Agents run only on safehouses, never on towers, stations or handlers. PTY
   and harness code lives only in the `safehouse` crate. No other crate may name
   `std::process::Command` or `tokio::process::Command` (the root `clippy.toml` enforces it). Two
   documented exceptions, both in frontends started by the chief: opening a sign-in URL the user
   asked for, and the git helper running git.
2. **Towers never hold keys.** A tower sees ciphertext only. The tower binary contains no sealing or
   key code.
3. **Harness credentials.** Drive the harness's own login command. Never parse, log, store or copy
   harness credentials or credential files, and never inject refresh tokens.
4. **One station per identity.** A station takes an exclusive lock on its data dir. Two processes
   must never share a node key.
5. **Frontends hold no network state.** `chief` and `chief-cli` depend on `numbers-base` only, never
   on `numbers` or iroh.
6. **Libraries are quiet.** No process-global state in library crates: no tracing subscriber, no
   rustls crypto-provider install, no signal handlers. Those belong in a binary's `main`.
7. **API types cross FFI.** Everything in `chief::api` uses strings, integers, bytes, `Option`,
   `Vec`, maps and our own records and enums. Ids, paths and URLs are strings; time is unix
   milliseconds.
8. **Peer input is untrusted.** Strip control characters from every peer-supplied string before a
   frontend sees it. A station never opens a URL or file because a peer said so.
9. **Nothing sensitive in the open.** No secrets, node ids, invite tickets or real hostnames in
   issues, pull requests, logs or test fixtures.
10. **`unsafe` is forbidden** in our crates, and `unwrap` is denied outside tests.

## How we work

- **Issues.** Work is tracked in GitHub issues. Every milestone is a sub-issue of the roadmap; the
  active milestones have Task sub-issues, one per reviewable pull request. `Decision:` and `Verify:`
  issues are attached to the milestone they gate.
- **Pull requests** go into `main`, are small, start their body with `Closes #n`, and say how they
  were verified (the commands and the result). The human merges. After a merge, a comment on the
  issue records what was done, how it was verified and what was deferred.
- **Docs stay true.** If a change makes `docs/` or this file wrong, fix them in the same pull
  request. If the design itself is wrong or in the way, open or comment on a `Decision:` issue with
  the options and a recommendation; the human decides. Do not diverge silently.
- **Facts over memory.** iroh and the harness CLIs change quickly. Check the appendix in
  `docs/ARCHITECTURE.md`, then the published source or docs, before relying on an API.

## Two agents, one repository

Claude and Codex both work here. The short rules:

1. **One owner per issue.** Before starting, read the issue's comments. If nobody has claimed it,
   comment `Claimed by Claude` or `Claimed by Codex` with your branch name. If you stop, comment
   `Released by <agent>` and say what state the work is in. Suggested owners are listed on the
   coordination issue; the claim comment is what counts.
2. **Own branch, own pull request.** Claude uses `claude/…` branches, Codex uses
   `codex/<issue>-<slug>`. Never push to the other agent's branch or to `main`, and never merge a
   pull request yourself.
3. **Sign every comment** on its first line: `**Claude:**` or `**Codex:**`. Both agents may post
   through the same GitHub account.
4. **Interfaces land first.** Where two tasks meet — in M1, the `chief::api` types and the
   `Transport` trait — the types go in first in a small pull request. Announce later changes to them
   on the coordination issue before making them.
5. **Shared files** — root `Cargo.toml`, `Cargo.lock`, `clippy.toml`, `rust-toolchain.toml`,
   `.github/`, `AGENTS.md`, `CLAUDE.md`, `docs/` — get small, additive edits. Announce anything
   larger on the coordination issue first. Add a dependency to `[workspace.dependencies]` in the
   pull request that first needs it. On a `Cargo.lock` conflict, take `main`'s lock and let cargo
   add your entries back.
6. **Stay inside your task.** Do not reformat or refactor files that belong to another task. If you
   find a bug there, comment on its issue or open a Bug issue.
7. **Keep up with `main`.** Rebase your branch before opening a pull request and whenever `main`
   moves under an open one.
8. **Review when asked.** When the human asks you to review the other agent's pull request, comment
   with concrete findings (file, line, what breaks and how). Do not push to their branch.
9. **Read the coordination issue** before starting a task and before opening a pull request.
