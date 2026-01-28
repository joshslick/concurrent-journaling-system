#include <stdio.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include "journal.h"

// flag to make the first txe slow
static int first_txe = 1;
static pthread_mutex_t txe_lock = PTHREAD_MUTEX_INITIALIZER;

// thread function for delayed txe completion
static void *txe_delayed_thread(void *arg) {
    int id = *(int *)arg;
    free(arg);
    sleep(1);  // simulate slow disk write
    journal_txe_complete(id);
    return NULL;
}

void issue_journal_txb(int write_id) {
    printf("issue journal txb %d\n", write_id);
    journal_txb_complete(write_id);
}

void issue_journal_bitmap(int write_id) {
    printf("issue journal bitmap %d\n", write_id);
    journal_bitmap_complete(write_id);
}

void issue_journal_inode(int write_id) {
    printf("issue journal inode %d\n", write_id);
    journal_inode_complete(write_id);
}

void issue_write_data(int write_id) {
    printf("issue write data %d\n", write_id);
    write_data_complete(write_id);
}

void issue_journal_txe(int write_id) {
    printf("issue journal txe %d\n", write_id);

    pthread_mutex_lock(&txe_lock);
    int slow = first_txe;
    if (first_txe) {
        first_txe = 0;
    }
    pthread_mutex_unlock(&txe_lock);

    if (slow) {
        // first txe is slow: do it in a separate thread with 1s delay
        pthread_t t;
        int *id_ptr = malloc(sizeof(int));
        if (id_ptr == NULL) {
            // fallback: just complete immediately if malloc fails
            journal_txe_complete(write_id);
            return;
        }
        *id_ptr = write_id;
        pthread_create(&t, NULL, txe_delayed_thread, id_ptr);
        pthread_detach(t);
    } else {
        // later txe writes complete immediately
        journal_txe_complete(write_id);
    }
}

void issue_write_bitmap(int write_id) {
    printf("issue write bitmap %d\n", write_id);
    write_bitmap_complete(write_id);
}

void issue_write_inode(int write_id) {
    printf("issue write inode %d\n", write_id);
    write_inode_complete(write_id);
}

