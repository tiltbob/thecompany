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
bash scripts/check-deps.sh          # hard rule 5, and the iroh features the libraries must not enable
bash scripts/check-lints.sh         # the lints behind hard rules 1 and 10 are still switched on
bash scripts/check-units.sh         # systemd-analyze verify on the tower's systemd unit and its drop-in
cargo doc --workspace --no-deps --locked   # with RUSTDOCFLAGS="-D warnings"; broken doc links fail
```

`bash scripts/check.sh` runs all of them in order and stops at the first failure.

The toolchain is pinned in `rust-toolchain.toml`. `Cargo.lock` is committed. Tests run in-process on
127.0.0.1 and must not touch the external network.

## Hard rules

1. **Agent confinement.** Agents run only on safehouses, never on towers, stations or handlers. PTY
   and harness code lives only in the `safehouse` crate. No other crate may name
   `std::process::Command` or `tokio::process::Command` (the root `clippy.toml` enforces it). One
   documented exception, in a frontend started by the chief: the git helper running git.
2. **Towers never hold keys.** A tower sees ciphertext only. The tower binary contains no sealing or
   key code.
3. **Harness credentials.** Harnesses authenticate with API keys only. Never drive, relay or
   automate a subscription sign-in (claude.ai, ChatGPT), never parse, copy or move a harness's own
   credential files, and never inject tokens. An API key is entered by its chief and passed to the
   safehouse that chief names: never log it, echo it, return it from an API or put it in an error or
   an event, and never give it to a tower, another chief or airspace.
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
  were verified (the commands and the result). After a merge, a comment on the issue records what
  was done, how it was verified and what was deferred.
- **Docs stay true.** If a change makes `docs/` or this file wrong, fix them in the same pull
  request. If the design itself is wrong or in the way, open or comment on a `Decision:` issue with
  the options and a recommendation. Do not diverge silently.
- **Facts over memory.** iroh and the harness CLIs change quickly. Check the appendix in
  `docs/ARCHITECTURE.md`, then the published source or docs, before relying on an API.

## Two agents, one repository

Claude and Codex both work here, and the owner has asked them to keep going without waiting for the
owner. The review, merge and decision rules below are what makes that safe.

### Claiming and branches

1. **One owner per issue.** Before starting, read the issue's comments. If nobody has claimed it,
   comment `Claimed by Claude` or `Claimed by Codex` with your branch name. If you stop, comment
   `Released by <agent>` and say what state the work is in. Suggested owners are listed on the
   coordination issue; the claim comment is what counts.
2. **Own branch, own pull request.** Claude uses `claude/…` branches, Codex uses
   `codex/<issue>-<slug>`. Never push to the other agent's branch or to `main`.
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
7. **Keep up with `main`.** Bring your branch up to date before asking for review and whenever
   `main` moves under an open pull request.

### Review and merge

The owner has authorised merging without them, on these terms:

- **The agent that did not write a pull request reviews it and merges it.** Nobody merges their own
  pull request.
- The author asks for review with a comment on the pull request: `ready for review at <sha>`. From
  then on the author pushes to it only to address review findings.
- The reviewer reads the whole diff, runs the checks where it can, and either comments with concrete
  findings (file, line, what breaks and how) or comments `reviewed <sha>, merging` with what it
  checked.
- Merge only when all of this holds: CI is green on the reviewed commit; that commit is still the
  head; every finding is fixed or answered; the change stays inside its task and breaks no hard
  rule.
- Merge with a **merge commit**, not squash or rebase, so work stacked on the merged branch stays
  valid.
- **`main` stays green.** If a merge breaks it, fixing or reverting comes before anything else, for
  whoever notices first.
- Reviews are real. Do not wave a change through to unblock yourself. If you cannot judge part of a
  change, say so on the pull request.

### Decisions

- An open `Decision:` issue that blocks work may be settled without the owner **only when both
  agents agree**. The agent that needs it comments `Proposal: option X` with its reasoning. The
  other agent judges it independently and answers `Agree: option X`, or `Disagree` with reasons.
  Do not agree just to unblock.
- On agreement: comment `Decided by both agents: X` on the issue, post
  `Decided without the owner: #n, option X` on the coordination issue, and update `docs/` in the
  pull request that implements it. The owner can overturn it, so prefer the option that is easiest
  to change later.
- On disagreement: leave it open, post `Waiting for the owner: #n` on the coordination issue with
  both positions, and work on something that does not depend on it.

### Delegating

- Both top-level agents run on expensive models. Each keeps for itself what needs judgment —
  splitting work, writing briefs, reading results, review verdicts, merges and decisions — and
  hands as much of the rest as it can to cheaper sub-agents: implementation, tests, research,
  mechanical edits, issue housekeeping. The owner names the models in each agent's own
  instructions.
- A sub-agent gets a written brief: the issue, the files it owns, the hard rules that apply and the
  commands that must pass. The top-level agent reads the result before it is pushed and stays
  responsible for it.
- Fit the reasoning effort to the task, on both levels: low for mechanical work and housekeeping,
  medium for routine implementation and tests, high for tricky logic, security-sensitive code and
  reviews. Do not default to the maximum.

### Keeping going

- When you finish a task, take the next unclaimed task of the active milestone. When none is left,
  draft Task sub-issues for the next milestone, one per reviewable pull request with a suggested
  owner each, and post the split on the coordination issue. Start on yours once the other agent has
  acknowledged the split.
- Anything that needs the owner — hardware, hosts, accounts, a manual demo — is skipped and posted
  as `Waiting for the owner: …` on the coordination issue. It does not stop the rest.
- Stop and wait for the owner when a change would break a hard rule; when the agents disagree on a
  decision that blocks everything that is left; when `main` cannot be made green; or when something
  needs a secret, a payment or a destructive action such as rewriting shared history.
- Check the coordination issue and the open pull requests at the start of every working session and
  after every task. A pull request waiting for your review comes before your own next task.
