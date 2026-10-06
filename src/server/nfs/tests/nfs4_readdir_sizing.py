# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
# SPDX-License-Identifier: Unlicense
"""READDIR attribute storage, wire budgets, and whole-compound retry."""
import os

from nfs4_compound_boundaries import op, require, nfsace4
from nfs4lib import FancyNFS4Packer
from xdrdef.nfs4_const import *
from xdrdef.nfs4_type import dirlist4


def wire_size(listing):
    packer = FancyNFS4Packer()
    packer.pack_READDIR4resok(listing.resok4)
    return len(packer.get_buffer())


def readdir_sizing(p):
    first = 0 if p.args.minor == 0 else 1
    files = [p.create(f"sizing-{i:02d}") for i in range(40)]
    p.call([op.putfh(files[0][1]), op.setattr(files[0][2],
            {FATTR4_OWNER: b"4294967294", FATTR4_OWNER_GROUP: b"4294967293"})])
    names = {name for name, _, _ in files}
    handles = {name: fh for name, fh, _ in files}
    # Every readable word-0/1 attribute, including long OWNER/GROUP and FH,
    # then the v4.2 word containing OPEN_ARGUMENTS (40 bytes by itself).
    broad = (1 << 56) - 1
    broad &= ~((1 << FATTR4_TIME_ACCESS_SET) | (1 << FATTR4_TIME_MODIFY_SET))
    if p.args.minor >= 2:
        broad |= (1 << FATTR4_OPEN_ARGUMENTS) | (1 << FATTR4_CHANGE_ATTR_TYPE)
    no_acl = broad & ~(1 << FATTR4_ACL)

    def page(label, mask, budget=65536, cookie=0, verifier=b"", expected=NFS4_OK):
        result = p.call([op.putfh(p.directory), op.readdir(cookie, verifier, budget, budget, mask), op.getfh()],
                        "sizing_" + label, expected, runs=[(first, 3)])
        if expected != NFS4_OK:
            require(len(result.resarray) == 2, "failed READDIR executed GETFH")
            return None
        require(result.resarray[-1].object == p.directory, "READDIR changed the current handle")
        listing = result.resarray[1]
        require(wire_size(listing) <= budget, "READDIR exceeded maxcount on the wire")
        return listing

    def check_entries(entries):
        for entry in entries:
            require(entry.name in names and entry.attrs[FATTR4_FILEHANDLE] == handles[entry.name],
                    "attribute encoding corrupted an entry name or handle")
            require(FATTR4_TIME_MODIFY in entry.attrs, "attribute encoding lost trailing fields")

    listing = page("broad_no_acl", no_acl)
    require(listing.reply.eof and {e.name for e in listing.reply.entries} == names,
            "broad fixed attributes lost directory entries")
    check_entries(listing.reply.entries)
    listing = page("broad_mode_acl", broad)
    check_entries(listing.reply.entries)
    require(all(FATTR4_ACL in e.attrs for e in listing.reply.entries), "mode ACL was omitted")
    packer = FancyNFS4Packer()
    packer.pack_dirlist4(dirlist4(listing.reply.entries[:1], False))
    exact_budget = 8 + len(packer.get_buffer())
    exact = page("exact_entry", broad, exact_budget)
    require(len(exact.reply.entries) == 1 and wire_size(exact) == exact_budget,
            "a wire-sized budget did not fit exactly one entry")
    page("below_entry", broad, exact_budget - 1, expected=NFS4ERR_TOOSMALL)

    # Stored ACLs must fit when requested, and must not consume page/arena
    # space when the request only wants FILEHANDLE and TYPE.
    if os.environ["CHIMERA_COMPOUND_FEATURE"] == "readdir":
        acl = [nfsace4(ACE4_ACCESS_ALLOWED_ACE_TYPE, 0, 0x1F01FF, b"OWNER@")]
        acl += [nfsace4(ACE4_ACCESS_ALLOWED_ACE_TYPE, 0, ACE4_READ_DATA,
                       str(12000 + i).encode()) for i in range(24)]
        for _, fh, sid in files:
            p.call([op.putfh(fh), op.setattr(sid, {FATTR4_ACL: acl})])
        listing = page("unrequested_acl", (1 << FATTR4_FILEHANDLE) | (1 << FATTR4_TYPE), 8192)
        require(listing.reply.eof and len(listing.reply.entries) == len(names),
                "unrequested ACLs consumed the page or arena")

    # An actual 2048-byte wire page is much smaller than the safe temporary
    # attribute reservation. Verify progress and exact continuation anyway.
    seen, cookie, verifier = set(), 0, b""
    for index in range(len(names) + 1):
        listing = page(f"small_{index}", broad, 2048, cookie, verifier)
        require(listing.reply.entries, "small attribute page stalled")
        check_entries(listing.reply.entries)
        for entry in listing.reply.entries:
            require(entry.name not in seen and entry.cookie != cookie, "page repeated an entry or cookie")
            require(FATTR4_ACL in entry.attrs, "requested ACL was dropped to make the entry fit")
            if os.environ["CHIMERA_COMPOUND_FEATURE"] == "readdir":
                require(len(entry.attrs[FATTR4_ACL]) >= 25, "stored ACL was truncated")
            seen.add(entry.name)
            cookie = entry.cookie
        verifier = listing.cookieverf
        if listing.reply.eof:
            break
    require(seen == names and listing.reply.eof, "small pages lost entries or never reached EOF")
    page("too_small", broad, 16, expected=NFS4ERR_TOOSMALL)

    # Both pages and an intervening staged GETATTR must survive rejection
    # without sharing or overwriting each other's encoding arena allocations.
    # Host disk usage may change between calls; compare stable attributes.
    stable = broad
    for attribute in (FATTR4_FILES_AVAIL, FATTR4_FILES_FREE, FATTR4_SPACE_AVAIL, FATTR4_SPACE_FREE):
        stable &= ~(1 << attribute)
    result = p.call([op.putfh(p.directory), op.readdir(0, b"", 8192, 8192, stable),
                     op.getattr(no_acl), op.readdir(0, b"", 8192, 8192, stable), op.getfh()],
                    "sizing_two_pages", runs=[(first, 5)])
    left, right = result.resarray[1], result.resarray[3]
    require(repr(left) == repr(right), "retry or a later result overwrote an earlier page")
    require(wire_size(left) <= 8192 and result.resarray[-1].object == p.directory,
            "combined pages exceeded their budget or lost the directory cursor")
    check_entries(left.reply.entries)

    _, guard, sid = files[0]
    p.call([op.putfh(guard), op.write(sid, 0, FILE_SYNC4, b"unchanged")])
    result = p.call([op.putfh(p.directory), op.readdir(0, b"", 16, 16, broad),
                     op.putfh(guard), op.write(sid, 0, FILE_SYNC4, b"BAD")],
                    "sizing_failed_mutation", NFS4ERR_TOOSMALL, runs=[(first, 4)])
    require(len(result.resarray) == 2 and p.read(guard, sid) == b"unchanged",
            "failed READDIR ran its mutation suffix")
