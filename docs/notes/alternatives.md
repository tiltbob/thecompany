# Network layer: alternatives considered

Checked on 2026-11-20 against each project's own documentation. Points marked (U) were not read
first-hand.

## The question

Should the network and membership layers be an off-the-shelf overlay — Tailscale, self-hosted
Headscale, or Nebula — instead of our own `numbers` layer on iroh?

## What an overlay would and would not replace

It would replace NAT traversal and relays, enrolling and removing nodes, directional access rules,
socket forwarding, and shells on safehouses (Tailscale SSH). In roadmap terms: the network half of
M1, and M2, M3 and M10.

It would not replace chat, locks, airspace, harness sessions, the login flow, the GUI or the phone
app. Those stay ours whichever network layer is underneath, and they are most of the roadmap.

## Against the requirements

| Requirement | Tailscale (hosted) | Headscale (self-hosted) | Nebula | `numbers` on iroh (the plan) |
|---|---|---|---|---|
| Always-on infrastructure is untrusted | Partly. With Tailnet Lock the coordination server cannot add nodes, but it still controls policy and availability and sees the network's metadata. Relays cannot read traffic. | No. Headscale has no Tailnet Lock, so the server can add nodes and must be trusted. | Yes. Lighthouses and relays hold no signing key. | Yes. Towers hold no keys; membership is a roster signed by admin keys. |
| Enrolling a node needs a hardware token | No. Tailnet Lock keys are Ed25519 and live on a node; there is no PKCS#11 option. | No. | Yes. The CA key can sit on a PKCS#11 token (P-256) and signs every host certificate. | Yes, planned for M3. |
| Fully self-hosted | No. The coordination server is closed and run by the vendor. | Yes. | Yes. | Yes. |
| Rust daemons; a phone app with no separate VPN app | The official Rust crate is experimental (no SSH, no private relays, no iOS). Otherwise every machine runs the system daemon. A Swift kit can embed a node in an iOS app but is incomplete. | Same clients. | Go only, so a sidecar on every node; the phone uses the system VPN app. | Yes. iroh is a Rust library with iOS support. |
| Directional rules | Yes | Yes | Yes (U) | Yes (the gate's caller table) |
| Every action attributable; a person removable | Yes | Yes | Partly. Identity is a certificate name; removal means pushing a blocklist to every host or using short-lived certificates. | Yes, planned for M3. |

## Conclusion

Stay on iroh.

- Tailscale cannot meet the three trust requirements together. Hosted, it adds a third-party
  coordinator. Self-hosted, the coordinator has to be trusted. Neither can tie enrollment to the
  hardware token.
- Nebula does meet them, and its design (an offline or token-held CA signs every node) is the same
  idea as our enrollment key. But it is a Go system VPN: a sidecar on every node, a VPN app on the
  phone, weaker removal, and still none of the application.
- The saving would be real but bounded: three milestones and part of a fourth, out of sixteen.

What would change this: dropping hardware-gated enrollment and accepting a trusted coordinator. Then
Tailscale would give the network, membership, access rules and shells on day one and would be the
faster path.

Nothing here stops a team from also running an overlay for ordinary admin access to its machines.
iroh's direct paths work over any IP network, a tailnet included.

## Sources

- Tailnet Lock: <https://tailscale.com/kb/1226/tailnet-lock>, <https://tailscale.com/kb/1230/tailnet-lock-whitepaper>
- Relays: <https://tailscale.com/kb/1232/derp-servers>, <https://tailscale.com/kb/1118/custom-derp-servers>
- Headscale features and the open Tailnet Lock issue: <https://headscale.net/stable/about/features/>, <https://github.com/juanfont/headscale/issues/1307>
- Embedding: <https://tailscale.com/kb/1244/tsnet>, <https://github.com/tailscale/libtailscale>, <https://docs.rs/crate/tailscale/latest>
- Tailscale SSH and grants: <https://tailscale.com/kb/1193/tailscale-ssh>, <https://tailscale.com/kb/1324/grants>
- Nebula PKI and relays: <https://nebula.defined.net/docs/config/pki>, <https://nebula.defined.net/docs/config/relay>, <https://github.com/slackhq/nebula/releases>
