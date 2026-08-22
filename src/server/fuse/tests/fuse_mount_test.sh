#!/bin/bash
# SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
#
# SPDX-License-Identifier: Unlicense
#
# End-to-end smoke of the FUSE protocol server: start a daemon serving a
# backend with a FUSE mountpoint, exercise the filesystem through real kernel
# syscalls with ordinary shell tools, verify the daemon idles without
# spinning, then unmount and shut down cleanly.

BIN=${1:?usage: fuse_mount_test.sh <chimera_binary> <backend>}
BACKEND=${2:?usage: fuse_mount_test.sh <chimera_binary> <backend>}

. "$(dirname "$0")/fuse_test_common.sh"

fuse_test_start "$BIN" "$BACKEND"

cd "$MNT" || fail "cannot enter mountpoint"

# --- files: create, write, read back, append ---

echo "hello" > file || fail "create/write"
[ "$(cat file)" = "hello" ] || fail "read back"

echo "again" >> file || fail "append"
[ "$(cat file)" = "$(printf 'hello\nagain')" ] || fail "append content"

# --- stat / chmod ---

[ "$(stat -c %s file)" = "12" ] || fail "size after append ($(stat -c %s file))"

chmod 640 file || fail "chmod"
[ "$(stat -c %a file)" = "640" ] || fail "mode after chmod"

# --- directories and a large readdir ---

mkdir dir || fail "mkdir"
mkdir dir/sub || fail "nested mkdir"

( cd dir/sub && seq 1 1000 | xargs touch ) || fail "creating 1000 files"
[ "$(ls -1 dir/sub | wc -l)" = "1000" ] || fail "readdir count"

# --- rename, rename over, hardlink, symlink ---

mv file dir/renamed || fail "rename into subdir"
echo "other" > victim
mv dir/renamed victim || fail "rename over existing"
[ "$(head -1 victim)" = "hello" ] || fail "rename-over content"

ln victim hardlink || fail "hardlink"
[ "$(stat -c %h victim)" = "2" ] || fail "nlink after hardlink"

ln -s victim sym || fail "symlink"
[ "$(readlink sym)" = "victim" ] || fail "readlink"
[ "$(head -1 sym)" = "hello" ] || fail "read through symlink"

# --- unlink / rmdir / negative checks ---

rm sym hardlink || fail "unlink"
rmdir dir 2> /dev/null && fail "rmdir of non-empty dir succeeded"
rm -r dir || fail "recursive remove"
[ ! -e dir ] || fail "dir still present"

cat nonexistent 2> /dev/null && fail "read of missing file succeeded"

# --- statfs ---

df -P . > /dev/null || fail "statfs"

# --- truncate and bulk data ---

truncate -s 100000 victim || fail "truncate up"
[ "$(stat -c %s victim)" = "100000" ] || fail "size after truncate"

dd if=/dev/urandom of=big bs=1M count=8 status=none || fail "8MB write"
cp big big2 || fail "copy"
cmp big big2 || fail "data integrity"

dd if=/dev/zero of=synced bs=64k count=4 conv=fsync status=none || fail "fsync write"

# --- idle CPU: a forgotten readiness-latch clear shows up as a spin ---

read -r _ _ _ _ _ _ _ _ _ _ _ _ _ before_utime before_stime _ < "/proc/$PID/stat"
sleep 1
read -r _ _ _ _ _ _ _ _ _ _ _ _ _ after_utime after_stime _ < "/proc/$PID/stat"

busy=$(( (after_utime - before_utime) + (after_stime - before_stime) ))

# 1s idle should burn well under half a second of CPU across all threads.
[ "$busy" -lt 50 ] || fail "daemon burned $busy ticks while idle (event-loop spin?)"

cd / || fail "cannot leave mountpoint"

fuse_test_stop

echo "PASS: FUSE mount over $BACKEND served kernel syscalls and idled quietly"
exit 0
