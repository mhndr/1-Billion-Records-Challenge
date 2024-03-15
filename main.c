#include <stdio.h>
#include <strings.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include <time.h> 
#include <semaphore.h>
#include <wchar.h>
#include "ring_buffer.h"

#define HT_SIZE 999999
#define RB_SIZE 999999

#define W_THREAD_MAX (20)
#define R_THREAD_MAX (20)


pthread_t tid[R_THREAD_MAX+W_THREAD_MAX];
bool writers_exited = false;


extern wchar_t **split_wchar(const wchar_t *in, wchar_t delm, size_t *num_elm);
extern void str_split_free(wchar_t **in, size_t num_elm);

typedef struct {
	wchar_t *key; //TODO:use wide char
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

typedef struct {
	char name[25];
	Buffer_t *rb;
	Hashtable *ht;
	FILE *file;
	pthread_t *readers;
	size_t reader_cnt;
}Thread_arg;


long hash(wchar_t* key) {
	long hashVal = 0;
	while (*key != '\0') {
		hashVal = (hashVal << 4) + *(key++);
		long g = hashVal & 0xF0000000L;
		if (g != 0) hashVal ^= g >> 24;
		hashVal &= ~g;
	}
	return hashVal;
}


int insert(Hashtable *ht, wchar_t* key, float value) {
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
			if(!wcscmp(curr->record->key,key)){
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
	//record->key = (wchar_t*)malloc(wcslen(key)+1);
	record->key = wcsdup(key);
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

Record* get(Hashtable *ht, wchar_t* key) {
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
		if(!wcscmp(curr->record->key,key)) {
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
				printf(" %ls ",curr->record->key);
				curr = curr->next;
			}
		}
	}
	return 0;
}

wchar_t** get_keys(Hashtable *ht, size_t *count) {
	if(!ht)
		return NULL;

	wchar_t **keys = (wchar_t**) malloc((sizeof(wchar_t*))*(ht->nentries));
	size_t cnt = 0;	
	for(int i=0;i<HT_SIZE;i++) {
		if(ht->list[i]->head != NULL) {
			Node *curr = ht->list[i]->head;
			while(curr) {
				//keys[cnt] = (wchar_t*) malloc(sizeof(wcslen(curr->record->key)+1));
				keys[cnt] = wcsdup(curr->record->key);
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
	wchar_t **keys=get_keys(ht,&key_count);
	printf("\n\nKEYS-get(key)");
	for(int i=0;i<key_count;i++) {
		Record *record = get(ht,keys[i]);
		if(record)
			printf("\n%ls - %f",keys[i],(float)record->sum/(float)record->count);
		else
			printf("\n%ls - Not Found" ,keys[i]);
	}
	return 0;
}


Buffer_t* Buffer_create(int n) {
	Buffer_t *buffer = (Buffer_t*) malloc(sizeof(Buffer_t));
	buffer->records = calloc(n,sizeof(wchar_t*));
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


bool  Buffer_insert(Buffer_t *buffer,wchar_t *record) {
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

wchar_t * Buffer_remove(Buffer_t *buffer) {
	if(!buffer)
		return NULL;

	wchar_t *item;
	
	if(sem_trywait(buffer->items)==-1 ) {
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
	
	//printf("\nIs Buffer Empty : Number of items in buffer : %zu",buffer->count);
	if(count==0) {
		return true;
	}
	return false; 
}

void* rb_write(void* arg) {
	FILE *file = (FILE*) ((Thread_arg*)arg)->file;
	Buffer_t *rb  = (Buffer_t*) ((Thread_arg*)arg)->rb;
	char *thread_name = (char*)((Thread_arg*)arg)->name;
	wchar_t buf[256];// (char*) malloc(256);
    struct timespec sleep; 
	sleep.tv_sec = 1;
    sleep.tv_nsec = 500;	
	wchar_t *line;	

	//printf("\nwriter thread started");
    while (fgetws(buf, sizeof(buf), file)!=NULL) {
		line = wcsdup(buf);
	//	printf("\nwriting line %ls",line);
		Buffer_insert(rb,line);
	}
	//printf("\nReached EOF,writer thread exiting");
	while(!Buffer_empty(rb));
	writers_exited = true;
	#ifdef DEBUG
	printf("\nreader thread %s exited",thread_name);
	#endif
	return NULL;
}

void* rb_read(void* arg) {
	Buffer_t *rb  = (Buffer_t*) ((Thread_arg*)arg)->rb;
	Hashtable *ht = (Hashtable*)  ((Thread_arg*)arg)->ht;
	char *thread_name = (char*)((Thread_arg*)arg)->name;
	wchar_t *line = NULL;
	wchar_t *name,*brkb,**tokens,*reading;
	float value;
	size_t tok_count=0;
   	int len;
	struct timespec sleep; 
	sleep.tv_sec = 0;
    sleep.tv_nsec = 150;	

	//printf("\nreader thread %s started",thread_name);
	while(!writers_exited) {; 
		line  = Buffer_remove(rb);
		if(line != NULL) {
	//		printf("\n\tread line %ls",line);
			tokens = split_wchar(line,L';',&tok_count);
			if(tok_count < 2) {
				//printf("\nseperator not found, unable to split string, %ls",line);
				continue;  
			}
			name = tokens[0];
			reading = tokens[1];
			if(!name || !reading) {
				//fprintf(stderr,"\nInvalid reading from line %ls",line);
				continue;
			}
			value = wcstof(reading,NULL);    
			//wprintf(L"\n\tinserting %ls : %f", name,value);
		    insert(ht,name,value);	 
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
	const char* filename = "measurements.txt";
    FILE* file = fopen(filename,"r"); 
	int error;
	Buffer_t *rb = Buffer_create(RB_SIZE);
 	Hashtable *ht = create_hashtable();

	
	if(ht==NULL || rb==NULL || file==NULL){
		printf("\nError, unable to start");
		return NULL;
	}	
	
//	printf("\nopened file and created ring buffer");
	
	for(int i=0;i<R_THREAD_MAX;i++) {
		Thread_arg *writer = malloc(sizeof(Thread_arg));
		sprintf(writer->name,"writer_%d",i);
		writer->rb = rb;
		writer->file = file;
		writer->ht = NULL;
		
		error = pthread_create(&(tid[i]), NULL, &rb_write, writer); 
		if (error != 0){
			fprintf(stderr,"failed to create writer thread %d",i);
			return NULL;
		}
	}
	
	for(int i=W_THREAD_MAX;i<W_THREAD_MAX+R_THREAD_MAX;i++) {
		Thread_arg *reader = malloc(sizeof(Thread_arg));
		sprintf(reader->name,"reader_%d",i);
		reader->rb = rb;
		reader->file = NULL;
		reader->ht = ht;
		error = pthread_create(&(tid[i]), NULL, &rb_read, reader); 
		if (error != 0){
			fprintf(stderr,"failed to create writer thread %d",i);
			return NULL;
		}
	}

	for(int i=0;i<R_THREAD_MAX+W_THREAD_MAX;i++) {
		pthread_join(tid[i],NULL);
	}
  	Buffer_delete(rb);
    fclose(file); 
	//print_ht(ht);
	calculate_mean_and_print_result(ht);	
	delete_hashtable(ht);	
	return NULL;	 
}
/*
void single_thread_test() {
	const char* filename = "measurements.txt";
    FILE* file = fopen(filename,"r"); 
    wchar_t line[256]; 
	char *name,**tokens,*reading;
	size_t tok_count;
	float value;
	Hashtable *ht = create_hashtable();
    while (fgets(line, sizeof(line), file)) {
		int len = wcslen(line);
		tokens = split_wchar(line,';',&tok_count);
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
*/
int main() {
	multi_thread_test();
	//single_thread_test();
	return 0;
}
