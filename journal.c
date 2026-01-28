/*
 * Journaling implementation for Project 2.
 *
 * implemented a 3-stage pipeline with 3 threads and 3 bounded buffers:
 *  - Thread 1 (journal-metadata-write-thread): takes write_ids from
 *    request_buffer, issues data + journal metadata writes, waits until
 *    all four complete using phase1_done, then puts the write_id into
 *    metadata_done_buffer.
 *
 *  - Thread 2 (journal-commit-write-thread): takes from
 *    metadata_done_buffer, issues the commit block (txe), waits for
 *    completion using phase2_done, then puts the write_id into
 *    commit_done_buffer.
 *
 *  - Thread 3 (checkpoint-metadata-thread): takes from
 *    commit_done_buffer, bitmap + inode checkpoint writes,
 *    waits using phase3_done, then calls write_complete(write_id).
 *
 * Each write_id has its own write_state_t with a mutex and condition
 * variables. This means no race conditions on flags and ensures
 * that threads only block when their input buffer is empty, when they
 * are waiting for their I/O to complete, or when their output buffer
 * is full.
 */


#include <stdio.h>
#include "journal.h"
#include <pthread.h>
static pthread_t thread1;
static pthread_t thread2;
static pthread_t thread3;

static void *journal_metadata_thread(void *arg);
static void *journal_commit_thread(void *arg);
static void *checkpoint_thread(void *arg);

typedef struct {
    int buf[BUFFER_SIZE];
    int in;
    int out;
    int count;
    pthread_mutex_t mutex;
    pthread_cond_t not_full;
    pthread_cond_t not_empty;
} bb_t;
static bb_t request_buffer;        // Stage 1 input buffer
static bb_t metadata_done_buffer;  // Stage 2 input buffer
static bb_t commit_done_buffer;    // Stage 3 input buffer

static void bb_init(bb_t *b) {
    b->in = 0;
    b->out = 0;
    b->count = 0;
    pthread_mutex_init(&b->mutex, NULL);
    pthread_cond_init(&b->not_full, NULL);
    pthread_cond_init(&b->not_empty, NULL);
}

// Put an item in buffer, wait if full
static void bb_put(bb_t *b, int id) {
    pthread_mutex_lock(&b->mutex);
    while (b->count == BUFFER_SIZE) {
        printf("thread stuck because of full buffer\n");
        fflush(stdout);
        pthread_cond_wait(&b->not_full, &b->mutex);
    }
    b->buf[b->in] = id;
    b->in = (b->in + 1) % BUFFER_SIZE;
    b->count++;
    pthread_cond_signal(&b->not_empty);
    pthread_mutex_unlock(&b->mutex);
}

// Get an item from buffer, wait if empty
static int bb_get(bb_t *b) {
    int id;
    pthread_mutex_lock(&b->mutex);
    while (b->count == 0) {
        pthread_cond_wait(&b->not_empty, &b->mutex);
    }
    id = b->buf[b->out];
    b->out = (b->out + 1) % BUFFER_SIZE;
    b->count--;
    pthread_cond_signal(&b->not_full);
    pthread_mutex_unlock(&b->mutex);
    return id;
}

#define MAX_WRITES 1024

typedef struct {
    int data_done;
    int j_txb_done;
    int j_bitmap_done;
    int j_inode_done;
    int j_txe_done;
    int w_bitmap_done;
    int w_inode_done;

    pthread_mutex_t lock;
    pthread_cond_t phase1_done;  // data + txb + bitmap + inode
    pthread_cond_t phase2_done;  // txe
    pthread_cond_t phase3_done;  // checkpoint bitmap + inode
} write_state_t;

static write_state_t writes[MAX_WRITES];

static void init_write_state(write_state_t *w) {
    w->data_done = 0;
    w->j_txb_done = 0;
    w->j_bitmap_done = 0;
    w->j_inode_done = 0;
    w->j_txe_done = 0;
    w->w_bitmap_done = 0;
    w->w_inode_done = 0;

    pthread_mutex_init(&w->lock, NULL);
    pthread_cond_init(&w->phase1_done, NULL);
    pthread_cond_init(&w->phase2_done, NULL);
    pthread_cond_init(&w->phase3_done, NULL);
}
/* This function can be used to initialize the buffers and threads.
 */
void init_journal() {
	// initialized buffers and threads here
    bb_init(&request_buffer);
    bb_init(&metadata_done_buffer);
    bb_init(&commit_done_buffer);

    // init per-write state
    for (int i = 0; i < MAX_WRITES; i++) {
        init_write_state(&writes[i]);
}
pthread_create(&thread1, NULL, journal_metadata_thread, NULL);
    pthread_create(&thread2, NULL, journal_commit_thread, NULL);
    pthread_create(&thread3, NULL, checkpoint_thread, NULL);
    }
/* This function is called by the file system to request writing data to
 * persistent storage.
 *
 * This simple version does not correctly deal with concurrency. It issues
 * all writes in the correct order, but it assumes issued writes always
 * complete immediately and therefore, it doesn't wait for each phase 
 * to complete.
 */
void request_write(int write_id) {
    if (write_id < 0 || write_id >= MAX_WRITES) {
        fprintf(stderr, "ERROR: write_id %d out of range\n", write_id);
        return;
    }

    // reset this write's state
    write_state_t *w = &writes[write_id];
    pthread_mutex_lock(&w->lock);
    w->data_done     = 0;
    w->j_txb_done    = 0;
    w->j_bitmap_done = 0;
    w->j_inode_done  = 0;
    w->j_txe_done    = 0;
    w->w_bitmap_done = 0;
    w->w_inode_done  = 0;
    pthread_mutex_unlock(&w->lock);

    // send into stage 1 of the pipeline
    bb_put(&request_buffer, write_id);
}

/* This function is called by the block service when writing the txb block
 * to persistent storage is complete (e.g., it is physically written to 
 * disk).
 */
void write_data_complete(int write_id) {
    write_state_t *w = &writes[write_id];
    pthread_mutex_lock(&w->lock);
    w->data_done = 1;
    if (w->data_done && w->j_txb_done &&
        w->j_bitmap_done && w->j_inode_done) {
        pthread_cond_signal(&w->phase1_done);
    }
    pthread_mutex_unlock(&w->lock);
}

void journal_txb_complete(int write_id) {
    write_state_t *w = &writes[write_id];
    pthread_mutex_lock(&w->lock);
    w->j_txb_done = 1;
    if (w->data_done && w->j_txb_done &&
        w->j_bitmap_done && w->j_inode_done) {
        pthread_cond_signal(&w->phase1_done);
    }
    pthread_mutex_unlock(&w->lock);
}

void journal_bitmap_complete(int write_id) {
    write_state_t *w = &writes[write_id];
    pthread_mutex_lock(&w->lock);
    w->j_bitmap_done = 1;
    if (w->data_done && w->j_txb_done &&
        w->j_bitmap_done && w->j_inode_done) {
        pthread_cond_signal(&w->phase1_done);
    }
    pthread_mutex_unlock(&w->lock);
}

void journal_inode_complete(int write_id) {
    write_state_t *w = &writes[write_id];
    pthread_mutex_lock(&w->lock);
    w->j_inode_done = 1;
    if (w->data_done && w->j_txb_done &&
        w->j_bitmap_done && w->j_inode_done) {
        pthread_cond_signal(&w->phase1_done);
    }
    pthread_mutex_unlock(&w->lock);
}


void journal_txe_complete(int write_id) {
    write_state_t *w = &writes[write_id];
    pthread_mutex_lock(&w->lock);
    w->j_txe_done = 1;
    pthread_cond_signal(&w->phase2_done);
    pthread_mutex_unlock(&w->lock);
}

void write_bitmap_complete(int write_id) {
    write_state_t *w = &writes[write_id];
    pthread_mutex_lock(&w->lock);
    w->w_bitmap_done = 1;
    if (w->w_bitmap_done && w->w_inode_done) {
        pthread_cond_signal(&w->phase3_done);
    }
    pthread_mutex_unlock(&w->lock);
}

void write_inode_complete(int write_id) {
    write_state_t *w = &writes[write_id];
    pthread_mutex_lock(&w->lock);
    w->w_inode_done = 1;
    if (w->w_bitmap_done && w->w_inode_done) {
        pthread_cond_signal(&w->phase3_done);
    }
    pthread_mutex_unlock(&w->lock);
}

static void *journal_metadata_thread(void *arg) {
    (void)arg;
    while (1) {
        int id = bb_get(&request_buffer);
        write_state_t *w = &writes[id];

        // reset flags
        pthread_mutex_lock(&w->lock);
        w->data_done     = 0;
        w->j_txb_done    = 0;
        w->j_bitmap_done = 0;
        w->j_inode_done  = 0;
        pthread_mutex_unlock(&w->lock);

        // issue data + metadata journal writes
        issue_write_data(id);
        issue_journal_txb(id);
        issue_journal_bitmap(id);
        issue_journal_inode(id);

        // wait until all are complete
        pthread_mutex_lock(&w->lock);
        while (!(w->data_done &&
                 w->j_txb_done &&
                 w->j_bitmap_done &&
                 w->j_inode_done)) {
            pthread_cond_wait(&w->phase1_done, &w->lock);
        }
        pthread_mutex_unlock(&w->lock);

        
        bb_put(&metadata_done_buffer, id);
    }
    return NULL;
}


static void *journal_commit_thread(void *arg) {
    (void)arg;
    while (1) {
        int id = bb_get(&metadata_done_buffer);
        write_state_t *w = &writes[id];

        pthread_mutex_lock(&w->lock);
        w->j_txe_done = 0;
        pthread_mutex_unlock(&w->lock);

        issue_journal_txe(id);

        pthread_mutex_lock(&w->lock);
        while (!w->j_txe_done) {
            pthread_cond_wait(&w->phase2_done, &w->lock);
        }
        pthread_mutex_unlock(&w->lock);

        
        bb_put(&commit_done_buffer, id);
    }
    return NULL;
}

static void *checkpoint_thread(void *arg) {
    (void)arg;
    while (1) {
        int id = bb_get(&commit_done_buffer);
        write_state_t *w = &writes[id];

        pthread_mutex_lock(&w->lock);
        w->w_bitmap_done = 0;
        w->w_inode_done  = 0;
        pthread_mutex_unlock(&w->lock);

        issue_write_bitmap(id);
        issue_write_inode(id);

        pthread_mutex_lock(&w->lock);
        while (!(w->w_bitmap_done && w->w_inode_done)) {
            pthread_cond_wait(&w->phase3_done, &w->lock);
        }
        pthread_mutex_unlock(&w->lock);

        // tell filesystem write is complete
        write_complete(id);
    }
    return NULL;
}

