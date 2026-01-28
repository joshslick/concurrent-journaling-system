#include <stdio.h>
#include "journal.h"

int is_write_data_complete;
int is_journal_txb_complete;
int is_journal_bitmap_complete;
int is_journal_inode_complete;
int is_journal_txe_complete;
int is_write_bitmap_complete;
int is_write_inode_complete;

/* This function can be used to initialize the buffers and threads.
 */
void init_journal() {
	// initialize buffers and threads here
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

        // write data and journal metadata
        is_write_data_complete = 0;
        is_journal_txb_complete = 0;
        is_journal_bitmap_complete = 0;
        is_journal_inode_complete = 0;
        issue_write_data(write_id);
        issue_journal_txb(write_id);
        issue_journal_bitmap(write_id);
        issue_journal_inode(write_id);

        // normally we should wait here for the 4 issues to complete,
        // but this simple implementation doesn’t have concurrency,
        // so instead we will just call it an error if anything isn’t
        // complete in this version of the code
        if (!is_write_data_complete || !is_journal_txb_complete
                || !is_journal_bitmap_complete
                || !is_journal_inode_complete) {
                printf("ERROR: writing data or journal failed!");
                return;
        }


        // commit transaction by writing txe
        is_journal_txe_complete = 0;
        issue_journal_txe(write_id);

        // normally we should wait here for the issue to complete,
        // but this simple implementation doesn’t have concurrency,
        // so instead we will just call it an error if it isn’t
        // complete in this version of the code
        if (!is_journal_txe_complete) {
                printf("ERROR: writing txe to journal failed!");
                return;
        }

        // checkpoint by writing metadata
        is_write_bitmap_complete = 0;
        is_write_inode_complete = 0;
        issue_write_bitmap(write_id);
        issue_write_inode(write_id);

        // normally we should wait here for the 2 issues to complete,
        // but this simple implementation doesn’t have concurrency,
        // so instead we will just call it an error if anything isn’t
        // complete in this version of the code
        if (!is_write_bitmap_complete || !is_write_inode_complete) {
                printf("ERROR: writing metadata failed!");
                return;
        }


        // tell the file system that the write is complete
        write_complete(write_id);
}

/* This function is called by the block service when writing the txb block
 * to persistent storage is complete (e.g., it is physically written to 
 * disk).
 */
void journal_txb_complete(int write_id) {
        is_journal_txb_complete = 1;
}

void journal_bitmap_complete(int write_id) {
        is_journal_bitmap_complete = 1;
}

void journal_inode_complete(int write_id) {
        is_journal_inode_complete = 1;
}

void write_data_complete(int write_id) {
        is_write_data_complete = 1;
}

void journal_txe_complete(int write_id) {
        is_journal_txe_complete = 1;
}

void write_bitmap_complete(int write_id) {
        is_write_bitmap_complete = 1;
}

void write_inode_complete(int write_id) {
        is_write_inode_complete = 1;
}

