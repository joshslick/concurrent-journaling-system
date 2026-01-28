#include <stdio.h>
#include "journal.h"

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
	journal_txe_complete(write_id);
}

void issue_write_bitmap(int write_id) {
	printf("issue write bitmap %d\n", write_id);
	write_bitmap_complete(write_id);
}

void issue_write_inode(int write_id) {
	printf("issue write inode %d\n", write_id);
	write_inode_complete(write_id);
}
