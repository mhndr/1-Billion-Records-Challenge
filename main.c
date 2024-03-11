#include<stdio.h>
#include<strings.h>
#include<stdlib.h>
#include<pthread.h>
#include<stdbool.h>
#include <time.h> 
#include "ring_buffer.h"


#define HT_SIZE 50000
#define RB_SIZE 1000


extern char **str_split(const char *in, size_t in_len, char delm, size_t *num_elm, size_t max);
extern void str_split_free(char **in, size_t num_elm);

typedef struct {
	char *key;
	int count;
	int sum;
	float min;
	float max;
	float mean;
}Record;

typedef struct node {
	struct node *next;
	Record *record;
} Node;

typedef struct list {
	Node *head;
	pthread_mutex_t lock;
} List;

typedef struct {
	size_t nentries;
	size_t table_sz;
	List **list;	
}Hashtable;


long hash(char* key) {
	long hashVal = 0;
	while (*key != '\0') {
		hashVal = (hashVal << 4) + *(key++);
		long g = hashVal & 0xF0000000L;
		if (g != 0) hashVal ^= g >> 24;
		hashVal &= ~g;
	}
	return hashVal;
}


int insert(Hashtable *ht, char* key, float value) {
	size_t key_hash = hash(key);
	size_t idx = key_hash % HT_SIZE;
	Record *record;
	Node *node;

	if(!key) 
		return -1;

	if(ht->list[idx] != NULL) {
		//search list
		pthread_mutex_lock(&ht->list[idx]->lock);
		Node *curr = ht->list[idx]->head;
		pthread_mutex_unlock(&ht->list[idx]->lock);
		while(curr) {
			if(!curr->record || !curr->record->key)
				return -1;

			if(!strcmp(curr->record->key,key)){
				//found, update values
				record = curr->record;
				record->count++;
				record->sum += value;
				if(record->max < value)
					record->max = value;
				if(record->min > value)
					record->min = value;	
				return 0;
			}	
			curr = curr->next;
		}	
	}

	record = (Record*)malloc(sizeof(Record));
	record->key = (char*)malloc(strlen(key)+1);
	strncpy(record->key, key,strlen(key));
	record->count = 1;
	record->min = value;
	record->max = value;
	record->sum = value;

	node = (Node*) malloc(sizeof(Node));
	node->record = record;
	node->next = ht->list[idx]->head ;

	pthread_mutex_lock(&ht->list[idx]->lock);
 	ht->list[idx]->head = node;
	pthread_mutex_unlock(&ht->list[idx]->lock);
	
	ht->nentries++;
	return 0;
}

Record* get(Hashtable *ht, char* key) {
	if(ht==NULL || key==NULL)
		return NULL;
	
	size_t key_hash = hash(key);
	size_t idx = key_hash % HT_SIZE;

	if(ht->list[idx] == NULL) {
		return NULL;
	}
	
	pthread_mutex_lock(&ht->list[idx]->lock);
	Node *curr = ht->list[idx]->head;
	pthread_mutex_unlock(&ht->list[idx]->lock);
	if(!curr) {
		//fprintf(stderr);
		return NULL;
	}
	while(curr) {
		if(!strcmp(curr->record->key,key)) {
			return curr->record;
		}
		curr = curr->next;
	}
	return NULL;
}


Hashtable* create_hashtable() {
	Hashtable *ht = (Hashtable*)malloc(sizeof(Hashtable));
	ht->nentries = 0;
	ht->table_sz = HT_SIZE;
	ht->list = (List**) malloc(sizeof(List*)*HT_SIZE);

	for(int i=0;i<HT_SIZE;i++) {
		ht->list[i] = (List*) malloc(sizeof(List));
		ht->list[i]->head = NULL;
		pthread_mutex_init(&ht->list[i]->lock, NULL); 
	}
	return ht;
}


void delete_hashtable(Hashtable *ht) {
	if(!ht)
		return;

	Node *next;
	for(int i=0;i<HT_SIZE;i++) {
		if(ht->list[i]->head != NULL) {
			Node *curr = ht->list[i]->head;
			while(curr) {
				next = curr->next;
				free(curr->record);
				free(curr);
				curr = next;
			}
		}
	}
	free(ht->list);
	free(ht);
	return;
}

int print_ht(Hashtable* ht) {
	if(!ht)
		return -1;

	printf("\nno of entries in hashtable = %zu",ht->nentries);
	printf("\nPRINT HASHTABLE");
	for(int i=0;i<HT_SIZE;i++) {
		if(ht->list[i]->head != NULL) {
			Node *curr = ht->list[i]->head;
			if(!curr) {
				printf("\nNULL");
				continue;
			}
			printf("\n");
			while(curr) {
				printf(" %s ",curr->record->key);
				curr = curr->next;
			}
		}
	}
	return 0;
}

char** get_keys(Hashtable *ht, size_t *count) {
	if(!ht)
		return NULL;

	char **keys = (char**) malloc((sizeof(char*))*(ht->nentries));
	size_t cnt = 0;	
	for(int i=0;i<HT_SIZE;i++) {
		if(ht->list[i]->head != NULL) {
			Node *curr = ht->list[i]->head;
			while(curr) {
				keys[cnt] = (char*) malloc(sizeof(strlen(curr->record->key)+1));
				strcpy(keys[cnt],curr->record->key);
				curr = curr->next;
				cnt++;
			}
		}
	}
	*count = cnt;
	return keys;
}


int calculate_mean_and_print_result(Hashtable *ht) {
	if(!ht)
		return -1;	
	
	size_t key_count;
	char **keys=get_keys(ht,&key_count);
	printf("\n\nKEYS-get(key)");
	for(int i=0;i<key_count;i++) {
		Record *record = get(ht,keys[i]);
		if(record)
			printf("\n%s - %f",keys[i],(float)record->sum/(float)record->count);
		else
			printf("\n%s - Not Found" ,keys[i]);
	}
	return 0;
}


typedef struct {
	char **buffer;
	size_t buf_sz;
	size_t n_elems;
	size_t head;
	size_t tail;
	pthread_mutex_t rlock;
	pthread_mutex_t wlock;	
} RingBuffer;

RingBuffer* create_ring_buffer() {
	RingBuffer *rb = (RingBuffer*) malloc(sizeof(RingBuffer));
	rb->buf_sz = RB_SIZE;
	rb->buffer = (char**)malloc(sizeof(char*));
	rb->n_elems = 0;
	rb->head = 0;
	rb->tail = 0;
	pthread_mutex_init(&rb->rlock,NULL);
	pthread_mutex_init(&rb->wlock,NULL);
	return rb;
}

//TODO:
void delete_ring_buffer(void);

bool is_buffer_empty(RingBuffer *rb) {
	if(!rb)
		return false;
	if((rb->head+1)%RB_SIZE == rb->tail) {
		return true;
	}
	return false;
}

bool is_buffer_full(RingBuffer *rb) {
	if(!rb)
		return false;
		
	if((rb->tail+1)%RB_SIZE==rb->head){
		return true;
	}
	return false;
}

void* read_buffer(RingBuffer *rb,struct timespec *sleep) {
	if(!rb)
		return NULL;

	char *data=NULL;

	if(!is_buffer_empty(rb)) {
		pthread_mutex_lock(&rb->rlock);
		data = rb->buffer[rb->head];
		rb->head = (rb->head+1)%RB_SIZE;
		pthread_mutex_unlock(&rb->rlock);
		
		return data;
	}	
	printf("\nBuffer is Empty!!");
//	nanosleep(sleep,sleep);
	return NULL;
}

bool write_buffer(RingBuffer *rb, char* data,struct timespec *sleep) {
	if(!rb)
		return false;

	if(!is_buffer_full(rb)) {
		pthread_mutex_lock(&rb->wlock);
		rb->buffer[rb->tail] = data;
		rb->tail = (rb->tail+1)%RB_SIZE;
		pthread_mutex_unlock(&rb->wlock);

		return true;
	}	
	printf("\nBuffer is Full!!");
//	nanosleep(sleep,sleep);
	char *line;
	while(1) {
		if(is_buffer_empty(rb)) {
			break;
		}
		line = (char*) read_buffer(rb,sleep);
        if(line != NULL) {
            printf("\n\tread line %s",line);
		}
	}
	return false;
}


typedef struct {
	char name[25];
	//RingBuffer *rb;
	Buffer_t *rb;
	Hashtable *ht;
	FILE *file;
    struct timespec sleep; 
}Thread_arg;

bool game_over = false;

void* rb_write(void* arg) {
	FILE *file = (FILE*) ((Thread_arg*)arg)->file;
	//RingBuffer *rb = (RingBuffer*)((Thread_arg*)arg)->rb;
	Buffer_t *rb  = (Buffer_t*) ((Thread_arg*)arg)->rb;
	char *line =(char*) malloc(256);
	struct timespec sleep = ((Thread_arg*)arg)->sleep;
	bool written;

	printf("\nwriter thread started");
	while((line = fgets(line, 256, file))) {
		if(!line)
			break; 
		printf("\nwriting line %s",line);
		Buffer_insert(rb,line);
		//written = false;
		//while(!written){
		//		written = write_buffer(rb,line,&sleep);
			//if(!written)
			//	nanosleep(&sleep,&sleep);
		//}
	}
	printf("\nReached EOF,writer thread exiting");
	while(!Buffer_empty(rb));//wait for readers to finish
	game_over = true;
	return NULL;
}

void* rb_read(void* arg) {
	//RingBuffer *rb = (RingBuffer*)((Thread_arg*)arg)->rb;
	Buffer_t *rb  = (Buffer_t*) ((Thread_arg*)arg)->rb;
	char *thread_name = (char*)((Thread_arg*)arg)->name;
	char *line = NULL;
	char *name,*brkb,*reading,**tokens;
	float value;
	size_t tok_count=0;
	Hashtable *ht = (Hashtable *)((Thread_arg*)arg)->ht;
	struct timespec sleep = ((Thread_arg*)arg)->sleep;

	printf("\nreader thread %s started",thread_name);
	while(!game_over) {; 
//		printf("\nreader thread %s reading",thread_name);
//		line = (char*) read_buffer(rb,&sleep);
		line  = Buffer_remove(rb);
		if(line != NULL) {
			printf("\n\tread line %s",line);
			int len = strlen(line);
			if(len==0)
				continue;
			tokens = str_split(line,len,';',&tok_count,len);
			if(tok_count < 2) {
				printf("\nseperator not found, unable to split string, %s",line);
				//goto sleep;
				continue;  
			}
			name = tokens[0];
			reading = tokens[1];
			if(!name || !reading) {
				fprintf(stderr,"\nInvalid reading from line %s",line);
				//goto sleep;
				continue;
			}
			//printf("\n\tsplit line to fields %s,%s",name,reading);
			value = strtof(reading,NULL);    
			printf("\n\tinserting %s : %f", name,value);
			//if(get(ht,name) == NULL)	{
		    //	insert(ht,name,value);	 
			//}
			//else {
			//	printf("\n\tFound existing record %s",name);
			//}
			free(line);
			line = NULL;
		}
		else{
			sleep:
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
//	RingBuffer *rb = create_ring_buffer();
	Hashtable *ht = create_hashtable();
	Buffer_t *rb = Buffer_create(RB_SIZE);
	pthread_t tid[5]; 
	
	if(ht==NULL||rb==NULL||file==NULL){
		printf("\nError, unable to start");
		return NULL;
	}	
	
	printf("\nopened file and created ring buffer");
	
	Thread_arg *writer = malloc(sizeof(Thread_arg));
	strcpy(writer->name,"writer");
	writer->rb = rb;
	writer->file = file;
	writer->ht = NULL;
 	writer->sleep.tv_sec = 0;
    writer->sleep.tv_nsec = 10;	
	
	error = pthread_create(&(tid[0]), NULL, &rb_write, writer); 
    if (error != 0){
		fprintf(stderr,"failed to create writer thread");
 		return NULL;
	}

	
	for(int i=1;i<5;i++) {
		Thread_arg *reader = malloc(sizeof(Thread_arg));
		sprintf(reader->name,"reader_%d",i);
		reader->rb = rb;
		reader->file = NULL;
		reader->ht = ht;
		reader->sleep.tv_sec = 0;
		reader->sleep.tv_nsec = 10;		

		error = pthread_create(&(tid[i]), NULL, &rb_read, reader); 
		if (error != 0){
			fprintf(stderr,"failed to create writer thread_%d",i);
			return NULL;
		}
	}

	for(int i=0;i<5;i++) {
		pthread_join(tid[i],NULL);
	}
  
    fclose(file); 
	//print_ht(ht);
	
	return NULL;	 
}


void single_thread_test() {
	const char* filename = "data.txt";
    FILE* file = fopen(filename,"r"); 
    char line[256]; 
	char *name,**tokens,*reading;
	size_t tok_count;
	float value;
	Hashtable *ht = create_hashtable();
    while (fgets(line, sizeof(line), file)) {
		int len = strlen(line);
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
		printf("\n\tsplit line to fields %s,%s",name,reading);
		value = strtof(reading,NULL);    
		printf("\ninserting %s : %f", name,value);
		insert(ht,name,value);	 
	} 
    fclose(file); 
	print_ht(ht);
	calculate_mean_and_print_result(ht);	
	delete_hashtable(ht);	
}

int main() {
	multi_thread_test();
	//single_thread_test();
	return 0;
}
