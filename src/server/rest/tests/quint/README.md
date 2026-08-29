<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors

SPDX-License-Identifier: LGPL-2.1-only
-->

# ctl — the control plane, and what it does to the data path

Creating an export makes a namespace reachable over NFS. Deleting one is
supposed to make it unreachable. Both calls return a 2xx, and nothing in the
API's own answers distinguishes a server that got the second one right from
one that only wrote it down — the difference shows up in what the *next* NFS
request sees. A suite that tested the endpoints alone would be testing the
paperwork.

So this model holds the administrative state *and* the filehandles a client is
holding, and a step is either a REST call or an NFS request. Everything
interesting is in the interleaving.

## Why it lives here and not in `ext/specs`

The other models — POSIX, NFSv3, NFSv4, SMB2 — describe published protocols.
They are worth stating independently of any implementation, and they are useful
to anyone testing a server, which is why they live in a submodule shared
between projects. Chimera's administrative API is chimera's own, and there is
no second implementation for its specification to be shared with. The practical
consequence is that the corpus is generated at build time from the `.qnt` files
here rather than fetched as a prebuilt bundle, so the replay ctest is
registered only where quint is installed (the devcontainer image has it).

## What the model requires

Almost everything the control plane must get right is one of three things.

**A collection behaves like a collection.** After a create the resource is
readable and listed; after a delete it is neither. A second create is a
conflict, not a silent overwrite — an overwrite would change an export's access
mode or re-point a mount under a running client, and the caller would have no
way to know it had replaced rather than added. A second delete is a not-found,
because "there is no such resource" and "I removed it for you" are different
facts and a configuration tool acts differently on them.

**References are honoured.** A mount is the target of exports, shares and
buckets; a filesystem is the target of mounts. Removing one out from under its
referrers is refused. This is the property that cannot be checked one endpoint
at a time, and it is why the model holds all the collections at once.

**Deleting an export revokes the filehandles minted under it.** This is what
makes the exports API an access control rather than a directory listing, and it
is the reason this suite exists. See below.

Plus one cross-check that costs nothing and catches a whole class of drift:
`GET /api/v1/config` is a second, independent rendering of the same facts as
the four listings, produced by different code. `--paranoid` requires them to
agree after every step.

## The central claim

> An operation on a filehandle whose export no longer exists is
> `NFS3ERR_STALE`.

Chimera does not do this. It resolves a handle's stamped export id only to
*apply* policy and lets an unknown id through (`nfs_common.h`,
`chimera_nfs_fh_decode`, whose comment says so and says what closing it would
take). Removing an export unlinks the export record and unmounts nothing, so
the inner VFS handle still resolves.

The consequence is not that the handle merely keeps working. Read-only, squash
and the security-flavor list all live on the record the id no longer finds, so
the handle keeps working **with no access policy at all**:

| after `DELETE /api/v1/exports/<name>` | chimera | Linux knfsd |
|---|---|---|
| a request on a held handle | `NFS3_OK` | ESTALE |
| a write through a held **read-only** handle | `NFS3_OK` | ESTALE |
| a write through a held **squash=all** handle | `NFS3_OK`, **lands as uid 0** | ESTALE |

Linux resolves the export on every request (`nfsd_set_fh_dentry` →
`rqst_exp_find`), which is what makes `exportfs -u` a revocation. The model
states the requirement; `ctl_mbt_replay.c` carries it in its known-deviation
registry so the suite is green today and goes red the moment chimera is fixed,
and `ctl_proto_probe.c` pins all four faces of it directly, where they are
deterministic.

## What is deliberately not modelled

* **An export deleted and re-created at a *different* path.** A filehandle
  carries its export's id and nothing about the path, so an id that came back
  pointing somewhere else would revive handles into the old namespace. Linux is
  immune because its handle carries the filesystem's fsid rather than an index
  into a table. Fixing that is a question about the wire format, not about the
  control plane, so each export name here is pinned to one path and the corpus
  never asks.

* **Re-created with the same path and id** *is* modelled, and is required to
  revive the handles: that is a configuration reload (`exportfs -r`), and a
  server that broke every client's handles on one would be unusable. The new
  policy applies to them, because policy is read per request — which is the
  same rule stated once, not a special case.

* **The SMB and S3 protocol surfaces.** Shares and buckets are modelled as
  administrative objects with the same contract as the others, and they pin the
  mount underneath them. What a share's deletion does to a live SMB tree
  connect needs an SMB client on this harness's event loop; the pieces are
  there (`ctl_http.h` borrows a caller-owned loop precisely so a probe can
  attach it to `smb2_mbt_common.h`'s), and it is the obvious next step.

* **REST authentication.** A profile dimension in which every request is 401
  until a login would add a constant to every trace and test one fact. It
  belongs in a probe, and `rest_auth_test.c` already has it.

* **Export id collision.** The model pins one id per export name, so two names
  can never claim one id and the 409 is unreachable from the corpus. It is
  covered directly by `rest_exports_test.c`.

## Files

| file | what is in it |
|---|---|
| `ctl_api.qnt` | the resources, and what each REST call does to them |
| `ctl.qnt` | the state machine: the API plus the handles a client holds |
| `ctl_run.qnt` | the profile |
| `ctlTest.qnt` | deterministic self-checks, run as a build gate |
| `coverage.py` | the corpus gate |
| `ctl_http.h` | the HTTP client, over a caller-owned evpl loop |
| `ctl_mbt_common.h` | a server for the control-plane-only probes |
| `ctl_smoke_probe.c` | the API surface and the scrape endpoint |
| `ctl_proto_probe.c` | the control plane's effect on NFS |
| `ctl_mbt_replay.c` | the corpus replayer |

## Corpus flavours

One walk cannot weight everything at once, so there are four.

* **`step`** — administrative calls and NFS requests interleaved. The suite's
  reason for existing.
* **`stepProtoHeavy`** — two thirds protocol, with the administrative third
  restricted to the calls that change what a handle sees. Long runs of requests
  across an export's lifetime.
* **`stepAdminOnly`** — every step an API call, so the referential refusals
  come up several times as often as in a mixed walk.
* **`stepPolicy`** — never touches mounts or filesystems, so a handle survives
  long enough for an export's read-only and squash settings to be observed
  through it. Weighted towards read-write exports, because a read-only or
  deleted one is answered before the caller's identity is ever consulted.

The last two exist because `coverage.py` said so: the mixed walks left the
referential refusals and every squashed write uncovered.

## Nothing binds a port

The REST server and the Prometheus scrape endpoint follow the server's
configured transport flavor, so under `CHIMERA_TCP_FLAVOR_INPROC` a "port" is
only the name of an inproc endpoint. The whole HTTP surface — request line,
headers, JSON body, status code — is reached from the same process with nothing
bound, exactly as the NFS harness reaches nfsd over inproc rpc2.

That is what makes the interleaving testable at all: the HTTP client and the
rpc2 client share one event loop, so "delete the export, then present the
handle" happens on a real server in the order the model says it does. It also
means every test here runs fully parallel on any host, where the older
curl-based REST tests need a network namespace and a ctest resource lock.

Thirty-two traces, about sixteen hundred steps, `--paranoid`: two seconds.
