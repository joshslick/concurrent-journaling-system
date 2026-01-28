#include <stdio.h>
#include <unistd.h>
#include <semaphore.h>
#include "journal.h"

sem_t sem;

void write_complete(int write_id) {
    printf("test write complete %d\n", write_id);
    if (write_id == 15) {
        sem_post(&sem);
    }
}

int main(int argc, char *argv[]) {
    sem_init(&sem, 0, 0);
    init_journal();
    for (int i=0; i<16; i++) {
        printf("requesting test write %d\n", i);
        request_write(i);
    }
    sem_wait(&sem);

    return 0;
}
