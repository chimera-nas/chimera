// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* Test the real backend while deliberately holding an ancestor inode mutex.
 * No production test hook or private-structure ABI is exported. */
#include <pthread.h>
#include <stdatomic.h>
#include <errno.h>

static pthread_mutex_t observation_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t observation_cond = PTHREAD_COND_INITIALIZER;
static pthread_mutex_t *watched_mutex;
static int observed;
static atomic_int rendezvous_moves;
static pthread_barrier_t moves_barrier;

static int test_mutex_trylock(pthread_mutex_t *mutex);
static int test_rwlock_wrlock(pthread_rwlock_t *lock);
#define pthread_mutex_trylock test_mutex_trylock
#define pthread_rwlock_wrlock test_rwlock_wrlock
#define vfs_memfs test_vfs_memfs
#include "memfs.c"
#undef vfs_memfs
#undef pthread_mutex_trylock
#undef pthread_rwlock_wrlock
#undef NDEBUG
#include <assert.h>

static atomic_uint completions;

/* Interpose only within the included backend: observing a real failed trylock
 * lets the controller create the opposing parent->child wait edge exactly,
 * without scheduling sleeps or production instrumentation. */
static int
test_mutex_trylock(pthread_mutex_t *mutex)
{
    int result = pthread_mutex_trylock(mutex);
    if (result == EBUSY && mutex == watched_mutex) {
        pthread_mutex_lock(&observation_lock);
        observed = 1;
        pthread_cond_signal(&observation_cond);
        pthread_mutex_unlock(&observation_lock);
    }
    return result;
}

static int
test_rwlock_wrlock(pthread_rwlock_t *lock)
{
    if (atomic_load(&rendezvous_moves)) pthread_barrier_wait(&moves_barrier);
    return pthread_rwlock_wrlock(lock);
}

static void
wait_contention(void)
{
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 10;
    pthread_mutex_lock(&observation_lock);
    while (!observed) assert(!pthread_cond_timedwait(&observation_cond, &observation_lock, &deadline));
    pthread_mutex_unlock(&observation_lock);
}

static void
watch_contention(pthread_mutex_t *mutex)
{
    watched_mutex = mutex;
    observed = 0;
}

static void
rename_test_complete(struct chimera_vfs_request *request)
{
    (void) request;
    completions++;
}

static void
rename_test_request(struct chimera_vfs_request *request,
                    const uint8_t *parent, uint32_t parent_len,
                    const uint8_t *destination, uint32_t destination_len,
                    const char *name, const uint8_t *source, uint32_t source_len)
{
    memset(request, 0, sizeof(*request));
    request->complete = rename_test_complete;
    request->opcode = CHIMERA_VFS_OP_RENAME_AT;
    memcpy(request->fh, parent, parent_len);
    request->fh_len = parent_len;
    request->rename_at.name = name;
    request->rename_at.namelen = strlen(name);
    request->rename_at.name_hash = chimera_vfs_hash(name, strlen(name));
    request->rename_at.new_fh = destination;
    request->rename_at.new_fhlen = destination_len;
    request->rename_at.new_name = "new";
    request->rename_at.new_namelen = 3;
    request->rename_at.new_name_hash = chimera_vfs_hash("new", 3);
    request->rename_at.flags = CHIMERA_VFS_RENAME_NOREPLACE | CHIMERA_VFS_RENAME_MATCH_SOURCE_FH;
    memcpy(request->rename_at.match_source_fh, source, source_len);
    request->rename_at.match_source_fh_len = source_len;
}

static void
check_existing_cases(void)
{
    struct memfs_shared shared = { 0 };
    struct memfs_inode nodes[5] = { 0 };
    struct memfs_inode *block = nodes;
    struct memfs_inode_list list = { .num_blocks = 1, .inode = &block };
    struct memfs_fs fs = { .shared = &shared, .inode_list = &list, .num_inode_list = 1 };
    struct memfs_thread thread = { .shared = &shared };
    pthread_rwlock_init(&fs.rename_lock, NULL);
    uint8_t mount_id[CHIMERA_VFS_MOUNT_ID_SIZE] = { 0 };
    uint8_t fh[5][CHIMERA_VFS_FH_SIZE];
    uint32_t lengths[5];
    struct chimera_vfs_request *request = calloc(1, sizeof(*request));
    assert(request);
    for (unsigned i = 1; i < 5; i++) {
        nodes[i].inum = (uint64_t) i << CHIMERA_MEMFS_INODE_LIST_SHIFT;
        nodes[i].gen = 1;
        nodes[i].nlink = 2;
        nodes[i].mode = (i == 3 ? S_IFREG : S_IFDIR) | 0700;
        nodes[i].dir.parent_inum = nodes[i == 1 ? 1 : (i == 4 ? 2 : 1)].inum;
        nodes[i].dir.parent_gen = 1;
        pthread_mutex_init(&nodes[i].lock, NULL);
        rb_tree_init(&nodes[i].dir.dirents);
        lengths[i] = chimera_vfs_encode_fh_inum_parent(mount_id, nodes[i].inum, 1, fh[i]);
    }
    struct memfs_dirent *file = memfs_dirent_alloc(&thread, nodes[3].inum, 1, false,
        chimera_vfs_hash("file", 4), "file", 4);
    rb_tree_insert(&nodes[2].dir.dirents, hash, file);
    struct memfs_dirent *parent = memfs_dirent_alloc(&thread, nodes[2].inum, 1, true,
        chimera_vfs_hash("parent", 6), "parent", 6);
    rb_tree_insert(&nodes[1].dir.dirents, hash, parent);

    /* A regular rename in B must never lock its ancestor A. Holding A on this
     * same thread turns the old inversion into a deterministic test timeout. */
    rename_test_request(request, fh[2], lengths[2], fh[2], lengths[2], "file", fh[3], lengths[3]);
    pthread_mutex_lock(&nodes[1].lock);
    memfs_rename_at(&thread, &fs, request, NULL);
    assert(completions == 1 && request->status == CHIMERA_VFS_OK);
    pthread_mutex_unlock(&nodes[1].lock);
    struct memfs_dirent *renamed;
    rb_tree_query_exact(&nodes[2].dir.dirents, chimera_vfs_hash("new", 3), hash, renamed);
    assert(renamed && !renamed->is_dir && renamed->inum == nodes[3].inum);

    /* Moving B into its own descendant C must reject without locking B:
     * directory-cycle detection must still precede any source child lock. */
    rename_test_request(request, fh[1], lengths[1], fh[4], lengths[4], "parent", fh[2], lengths[2]);
    pthread_mutex_lock(&nodes[2].lock);
    memfs_rename_at(&thread, &fs, request, NULL);
    assert(completions == 2 && request->status == CHIMERA_VFS_EINVAL);
    pthread_mutex_unlock(&nodes[2].lock);
    free(renamed);
    free(parent);
    while (thread.free_dirent) {
        struct memfs_dirent *entry = thread.free_dirent;
        LL_DELETE(thread.free_dirent, entry);
        free(entry);
    }
    for (unsigned i = 1; i < 5; i++) pthread_mutex_destroy(&nodes[i].lock);
    free(request);
    pthread_rwlock_destroy(&fs.rename_lock);
}

/* Small real inode table. Identity slots stay allocated exactly as production
 * inode blocks do; dirents carry a reference-independent immutable type. */
struct rename_fixture {
    struct memfs_shared shared;
    struct memfs_fs fs;
    struct memfs_inode_list list;
    struct memfs_inode nodes[9];
    struct memfs_inode *block;
    struct memfs_thread thread;
    uint8_t fh[9][CHIMERA_VFS_FH_SIZE];
    uint32_t len[9];
};

static void
fixture_init(struct rename_fixture *f)
{
    memset(f, 0, sizeof(*f));
    f->block = f->nodes;
    f->list.num_blocks = 1;
    f->list.inode = &f->block;
    f->fs.shared = &f->shared;
    f->fs.inode_list = &f->list;
    f->fs.num_inode_list = 1;
    f->thread.shared = &f->shared;
    pthread_rwlock_init(&f->fs.rename_lock, NULL);
    uint8_t mount_id[CHIMERA_VFS_MOUNT_ID_SIZE] = { 0 };
    for (unsigned i = 1; i < 9; i++) {
        struct memfs_inode *n = &f->nodes[i];
        n->fs = &f->fs;
        n->inum = (uint64_t) i << CHIMERA_MEMFS_INODE_LIST_SHIFT;
        n->gen = 1;
        n->mode = (i >= 7 ? S_IFREG : S_IFDIR) | 0700;
        n->nlink = i >= 7 ? 1 : 2;
        n->refcnt = 1;
        n->link_ref = 1;
        if (S_ISDIR(n->mode)) {
            n->dir.parent_inum = f->nodes[1].inum;
            n->dir.parent_gen = 1;
            rb_tree_init(&n->dir.dirents);
        }
        pthread_mutex_init(&n->lock, NULL);
        f->len[i] = chimera_vfs_encode_fh_inum_parent(mount_id, n->inum, n->gen, f->fh[i]);
    }
}

static void
fixture_add(struct rename_fixture *f, unsigned parent, unsigned child, const char *name)
{
    struct memfs_inode *p = &f->nodes[parent], *n = &f->nodes[child];
    struct memfs_dirent *entry = memfs_dirent_alloc(&f->thread, n->inum, n->gen,
        S_ISDIR(n->mode), chimera_vfs_hash(name, strlen(name)), name, strlen(name));
    rb_tree_insert(&p->dir.dirents, hash, entry);
    if (S_ISDIR(n->mode)) {
        p->nlink++;
        n->dir.parent_inum = p->inum;
        n->dir.parent_gen = p->gen;
    }
}

static struct memfs_dirent *
fixture_find(struct rename_fixture *f, unsigned parent, const char *name)
{
    struct memfs_dirent *entry;
    rb_tree_query_exact(&f->nodes[parent].dir.dirents, chimera_vfs_hash(name, strlen(name)), hash, entry);
    return entry;
}

static void
thread_free_dirents(struct memfs_thread *thread)
{
    while (thread->free_dirent) {
        struct memfs_dirent *entry = thread->free_dirent;
        LL_DELETE(thread->free_dirent, entry);
        free(entry);
    }
}

static void
fixture_free(struct rename_fixture *f)
{
    for (unsigned i = 1; i < 9; i++) {
        if (S_ISDIR(f->nodes[i].mode))
            rb_tree_destroy(&f->nodes[i].dir.dirents, memfs_dirent_release, NULL);
        pthread_mutex_destroy(&f->nodes[i].lock);
    }
    thread_free_dirents(&f->thread);
    pthread_rwlock_destroy(&f->fs.rename_lock);
}

struct rename_worker {
    struct rename_fixture *fixture;
    struct memfs_thread thread;
    struct chimera_vfs_request *request;
};

static void
worker_init(struct rename_worker *w, struct rename_fixture *f,
            unsigned parent, unsigned destination, unsigned source, const char *name)
{
    memset(w, 0, sizeof(*w));
    w->fixture = f;
    w->thread.shared = &f->shared;
    w->request = calloc(1, sizeof(*w->request));
    assert(w->request);
    rename_test_request(w->request, f->fh[parent], f->len[parent],
        f->fh[destination], f->len[destination], name, f->fh[source], f->len[source]);
}

static void *
worker_run(void *private_data)
{
    struct rename_worker *w = private_data;
    memfs_rename_at(&w->thread, &w->fixture->fs, w->request, NULL);
    return NULL;
}

static void
worker_free(struct rename_worker *w)
{
    thread_free_dirents(&w->thread);
    free(w->request);
}

static void
check_ancestor_contention(unsigned change)
{
    struct rename_fixture f;
    struct rename_worker w;
    pthread_t worker;
    fixture_init(&f);
    fixture_add(&f, 1, 2, "ancestor");
    fixture_add(&f, 2, 3, "destination");
    fixture_add(&f, 1, 4, "source-parent");
    fixture_add(&f, 4, 5, "move");
    worker_init(&w, &f, 4, 3, 5, "move");
    watch_contention(&f.nodes[2].lock);
    pthread_mutex_lock(&f.nodes[2].lock);
    assert(!pthread_create(&worker, NULL, worker_run, &w));
    wait_contention();
    /* This is the lock sequence a lookup/remove in the ancestor follows.
     * Blocking rename used to retain destination/source locks and deadlock. */
    pthread_mutex_lock(&f.nodes[3].lock);
    pthread_mutex_lock(&f.nodes[4].lock);
    if (change == 1) {
        struct memfs_dirent *old = fixture_find(&f, 4, "move");
        rb_tree_remove(&f.nodes[4].dir.dirents, &old->node);
        free(old);
        fixture_add(&f, 4, 6, "move");
    } else if (change == 2) {
        struct memfs_dirent *old = fixture_find(&f, 2, "destination");
        rb_tree_remove(&f.nodes[2].dir.dirents, &old->node);
        free(old);
        f.nodes[2].nlink--;
        f.nodes[3].nlink = 0; /* removed but pinned destination dirfd */
    }
    pthread_mutex_unlock(&f.nodes[4].lock);
    pthread_mutex_unlock(&f.nodes[3].lock);
    pthread_mutex_unlock(&f.nodes[2].lock);
    assert(!pthread_join(worker, NULL));
    watched_mutex = NULL;
    assert(w.request->status == (change == 1 ? CHIMERA_VFS_ESTALE :
        change == 2 ? CHIMERA_VFS_ENOENT : CHIMERA_VFS_OK));
    if (!change) {
        assert(!fixture_find(&f, 4, "move"));
        assert(fixture_find(&f, 3, "new")->inum == f.nodes[5].inum);
        assert(f.nodes[5].dir.parent_inum == f.nodes[3].inum);
        assert(f.nodes[4].nlink == 2 && f.nodes[3].nlink == 3);
    } else {
        assert(fixture_find(&f, 4, "move"));
        assert(!fixture_find(&f, 3, "new"));
    }
    worker_free(&w);
    fixture_free(&f);
}

static void
check_second_parent_contention(void)
{
    struct rename_fixture f;
    struct rename_worker w;
    pthread_t worker;
    fixture_init(&f);
    fixture_add(&f, 1, 2, "first");
    fixture_add(&f, 1, 3, "second");
    fixture_add(&f, 2, 7, "file");
    worker_init(&w, &f, 2, 3, 7, "file");
    watch_contention(&f.nodes[3].lock);
    pthread_mutex_lock(&f.nodes[3].lock);
    assert(!pthread_create(&worker, NULL, worker_run, &w));
    wait_contention();
    pthread_mutex_lock(&f.nodes[2].lock);
    pthread_mutex_unlock(&f.nodes[2].lock);
    pthread_mutex_unlock(&f.nodes[3].lock);
    assert(!pthread_join(worker, NULL));
    watched_mutex = NULL;
    assert(w.request->status == CHIMERA_VFS_OK);
    assert(fixture_find(&f, 3, "new")->inum == f.nodes[7].inum);
    worker_free(&w);
    fixture_free(&f);
}

static void
check_regular_parallel(void)
{
    struct rename_fixture f;
    struct rename_worker blocked, other;
    pthread_t worker;
    fixture_init(&f);
    fixture_add(&f, 1, 2, "first");
    fixture_add(&f, 1, 3, "second");
    fixture_add(&f, 2, 7, "file");
    fixture_add(&f, 3, 8, "file");
    worker_init(&blocked, &f, 2, 2, 7, "file");
    worker_init(&other, &f, 3, 3, 8, "file");
    watch_contention(&f.nodes[7].lock);
    pthread_mutex_lock(&f.nodes[7].lock);
    assert(!pthread_create(&worker, NULL, worker_run, &blocked));
    wait_contention();
    worker_run(&other); /* must complete while first rename still waits */
    assert(other.request->status == CHIMERA_VFS_OK);
    pthread_mutex_unlock(&f.nodes[7].lock);
    assert(!pthread_join(worker, NULL));
    watched_mutex = NULL;
    assert(blocked.request->status == CHIMERA_VFS_OK);
    worker_free(&blocked);
    worker_free(&other);
    fixture_free(&f);
}

static void
check_replacement_contention(void)
{
    struct rename_fixture f;
    struct rename_worker w;
    pthread_t worker;
    fixture_init(&f);
    fixture_add(&f, 1, 2, "directory");
    fixture_add(&f, 2, 7, "file");
    fixture_add(&f, 2, 8, "new");
    f.nodes[8].refcnt++; /* open replacement target remains alive unlinked */
    worker_init(&w, &f, 2, 2, 7, "file");
    w.request->rename_at.flags = CHIMERA_VFS_RENAME_MATCH_SOURCE_FH;
    watch_contention(&f.nodes[8].lock);
    pthread_mutex_lock(&f.nodes[8].lock);
    assert(!pthread_create(&worker, NULL, worker_run, &w));
    wait_contention();
    pthread_mutex_lock(&f.nodes[2].lock);
    pthread_mutex_lock(&f.nodes[7].lock); /* source child must also be released */
    pthread_mutex_unlock(&f.nodes[7].lock);
    pthread_mutex_unlock(&f.nodes[2].lock);
    pthread_mutex_unlock(&f.nodes[8].lock);
    assert(!pthread_join(worker, NULL));
    watched_mutex = NULL;
    assert(w.request->status == CHIMERA_VFS_OK);
    assert(!fixture_find(&f, 2, "file"));
    assert(fixture_find(&f, 2, "new")->inum == f.nodes[7].inum);
    assert(f.nodes[8].nlink == 0 && f.nodes[8].refcnt == 1);
    worker_free(&w);
    fixture_free(&f);
}

static void
check_opposing_moves(void)
{
    struct rename_fixture f;
    struct rename_worker a, b;
    pthread_t workers[2];
    fixture_init(&f);
    fixture_add(&f, 1, 2, "a");
    fixture_add(&f, 1, 3, "b");
    worker_init(&a, &f, 1, 3, 2, "a");
    worker_init(&b, &f, 1, 2, 3, "b");
    pthread_barrier_init(&moves_barrier, NULL, 2);
    atomic_store(&rendezvous_moves, 1);
    assert(!pthread_create(&workers[0], NULL, worker_run, &a));
    assert(!pthread_create(&workers[1], NULL, worker_run, &b));
    assert(!pthread_join(workers[0], NULL));
    assert(!pthread_join(workers[1], NULL));
    atomic_store(&rendezvous_moves, 0);
    pthread_barrier_destroy(&moves_barrier);
    assert((a.request->status == CHIMERA_VFS_OK && b.request->status == CHIMERA_VFS_EINVAL) ||
           (b.request->status == CHIMERA_VFS_OK && a.request->status == CHIMERA_VFS_EINVAL));
    unsigned moved = a.request->status == CHIMERA_VFS_OK ? 2 : 3;
    unsigned parent = moved == 2 ? 3 : 2;
    assert(f.nodes[moved].dir.parent_inum == f.nodes[parent].inum);
    assert(f.nodes[parent].dir.parent_inum == f.nodes[1].inum);
    assert(f.nodes[1].nlink == 3 && f.nodes[parent].nlink == 3);
    assert(fixture_find(&f, parent, "new")->inum == f.nodes[moved].inum);
    worker_free(&a);
    worker_free(&b);
    fixture_free(&f);
}

int
main(void)
{
    check_existing_cases();
    check_ancestor_contention(0);
    check_ancestor_contention(1);
    check_ancestor_contention(2);
    check_second_parent_contention();
    check_regular_parallel();
    check_replacement_contention();
    check_opposing_moves();
    return 0;
}
