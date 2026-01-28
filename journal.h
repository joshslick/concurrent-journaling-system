#ifndef JOURNAL_H
#define JOURNAL_H

#define BUFFER_SIZE 8 

// Implemented in journaling layer.
// YOU IMPLEMENT THESE FUNCTIONS
void init_journal();
void request_write(int write_id);
void write_complete(int write_id);
void journal_txb_complete(int write_id);
void journal_bitmap_complete(int write_id);
void journal_inode_complete(int write_id);
void write_data_complete(int write_id);
void journal_txe_complete(int write_id);
void write_bitmap_complete(int write_id);
void write_inode_complete(int write_id);

// Implemented in block layer.
// YOU USE THESE FUNCTIONS
void issue_journal_txb(int write_id);
void issue_journal_bitmap(int write_id);
void issue_journal_inode(int write_id);
void issue_write_data(int write_id);
void issue_journal_txe(int write_id);
void issue_write_bitmap(int write_id);
void issue_write_inode(int write_id);

#endif // JOURNAL_H
