#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <semaphore.h>
#include <stdbool.h>
#include <time.h> 
#include <pthread.h>
#include "ring_buffer.h"

#define RB_SIZE (1000)
#define THREAD_MAX (5)

pthread_t tid[THREAD_MAX];
bool game_over = false;


extern char **str_split(const char *in, size_t in_len, char delm, size_t *num_elm, size_t max);
extern void str_split_free(char **in, size_t num_elm);


typedef struct {
	char name[25];
	Buffer_t *rb;
	FILE *file;
	pthread_t *readers;
	size_t reader_cnt;
}Thread_arg;



Buffer_t* Buffer_create(int n) {
	Buffer_t *buffer = (Buffer_t*) malloc(sizeof(Buffer_t));
	buffer->records = calloc(n,sizeof(char*));
	buffer->n = n;
	buffer->count = 0;
	buffer->front = buffer->rear = 0;

	//clear out any existing files from old runs	
	sem_unlink("./sem_mutex");
	sem_unlink("./sem_slots");
	sem_unlink("./sem_items");
	

	buffer->mutex = sem_open("./sem_mutex",O_CREAT,0644,1);		
	buffer->slots = sem_open("./sem_slots",O_CREAT,0644,n);		
	buffer->items = sem_open("./sem_items",O_CREAT,0644,0);		
	return buffer;
}

void Buffer_delete(Buffer_t *buffer) {
	if(!buffer)
		return;
	for(int i=0;i<buffer->count;i++) {
		if(buffer->records[i]!=NULL)
			free(buffer->records[i]);	
	}
	sem_close(buffer->mutex);
	sem_close(buffer->slots);
	sem_close(buffer->items);
	
	sem_unlink("./sem_mutex");
	sem_unlink("./sem_slots");
	sem_unlink("./sem_items");
	
	free(buffer->records);
	free(buffer);
}


bool  Buffer_insert(Buffer_t *buffer,char *record) {
	if(!buffer) {
		printf("\nBuffer is NULL");
		return false;
	}

	sem_wait(buffer->slots);
	sem_wait(buffer->mutex);
	buffer->records[(++buffer->rear)%(buffer->n)] = record;
	buffer->count = (++buffer->count)%(buffer->n);
	sem_post(buffer->mutex);
	sem_post(buffer->items);
	#ifdef DEBUG 
	printf("\nInsert - Number of items in buffer : %zu",buffer->count);
	#endif
	return true;
}

char * Buffer_remove(Buffer_t *buffer) {
	if(!buffer)
		return NULL;

	char *item;
	
	if(sem_trywait(buffer->items)==-1 || game_over) {
		return NULL;
	}
	sem_wait(buffer->mutex);
	item = buffer->records[(++buffer->front)%(buffer->n)];
	if(buffer->count>0)
		buffer->count--;
	sem_post(buffer->mutex);
	sem_post(buffer->slots);

	#ifdef DEBUG
	printf("\nRemove - Number of items in buffer : %zu",buffer->count);
	#endif
	return item;
}

bool Buffer_empty(Buffer_t *buffer) {
	size_t count;
	sem_wait(buffer->mutex);
	count = buffer->count;
	sem_post(buffer->mutex);
	
	printf("\nIs Buffer Empty : Number of items in buffer : %zu",buffer->count);
	if(count==0) {
		return true;
	}
	return false; 
}

void* rb_write(void* arg) {
	FILE *file = (FILE*) ((Thread_arg*)arg)->file;
	Buffer_t *rb  = (Buffer_t*) ((Thread_arg*)arg)->rb;
	char buf[256];// (char*) malloc(256);
    struct timespec sleep; 
	sleep.tv_sec = 1;
    sleep.tv_nsec = 500;	
	char *line;	

	printf("\nwriter thread started");
    while (fgets(buf, 256, file)) {
		line = strdup(buf);
		printf("\nwriting line %s",line);
		Buffer_insert(rb,line);
	}
	printf("\nReached EOF,writer thread exiting");
	while(!Buffer_empty(rb));
	game_over = true;
	return NULL;
}

void* rb_read(void* arg) {
	Buffer_t *rb  = (Buffer_t*) ((Thread_arg*)arg)->rb;
	char *thread_name = (char*)((Thread_arg*)arg)->name;
	char *line = NULL;
	char *name,*brkb,*reading,**tokens;
	float value;
	size_t tok_count=0;
   	int len;
	struct timespec sleep; 
	sleep.tv_sec = 0;
    sleep.tv_nsec = 150;	

	printf("\nreader thread %s started",thread_name);
	while(!game_over) {; 
		line  = Buffer_remove(rb);
		if(line != NULL) {
			printf("\n\tread line %s",line);
			len = strlen(line);
			tokens = str_split(line,len,';',&tok_count,len);
			if(tok_count < 2) {
				printf("\nseperator not found, unable to split string, %s",line);
				continue;  
			}
			name = tokens[0];
			reading = tokens[1];
			if(!name || !reading) {
				fprintf(stderr,"\nInvalid reading from line %s",line);
				continue;
			}
			value = strtof(reading,NULL);    
			printf("\n\tinserting %s : %f", name,value);
			//if(get(ht,name) == NULL)	{
		    //	insert(ht,name,value);	 
			free(line);
			line = NULL;
		}
		else {
			//if nothing was read, take a short nap
			nanosleep(&sleep,&sleep);
		}
	}
	#ifdef DEBUG
	printf("\nreader thread %s exited",thread_name);
	#endif
	return NULL;
}


void* multi_thread_test(void) {
	const char* filename = "data.txt";
    FILE* file = fopen(filename,"r"); 
	int error;
	Buffer_t *rb = Buffer_create(RB_SIZE);
 
	
	if(rb==NULL||file==NULL){
		printf("\nError, unable to start");
		return NULL;
	}	
	
	printf("\nopened file and created ring buffer");
	
	Thread_arg *writer = malloc(sizeof(Thread_arg));
	strcpy(writer->name,"writer");
	writer->rb = rb;
	writer->file = file;
	
	error = pthread_create(&(tid[0]), NULL, &rb_write, writer); 
    if (error != 0){
		fprintf(stderr,"failed to create writer thread");
 		return NULL;
	}

	
	for(int i=1;i<THREAD_MAX;i++) {
		Thread_arg *reader = malloc(sizeof(Thread_arg));
		sprintf(reader->name,"reader_%d",i);
		reader->rb = rb;
		reader->file = NULL;

		error = pthread_create(&(tid[i]), NULL, &rb_read, reader); 
		if (error != 0){
			fprintf(stderr,"failed to create writer thread_%d",i);
			return NULL;
		}
	}

	for(int i=0;i<THREAD_MAX;i++) {
		pthread_join(tid[i],NULL);
	}
  	Buffer_delete(rb);
    fclose(file); 
	return NULL;	 
}
int main() {
	multi_thread_test();
		return 0;
}	
