#include<stdio.h>
#include<strings.h>
#include<stdlib.h>
#include<pthread.h>
#include<stdbool.h>
#include <time.h> 


#define HT_SIZE 5000
#define RB_SIZE 100

typedef struct {
	char *key;
	int count;
	int sum;
	float min;
	float max;
	float mean;
}Record;

typedef struct {
	size_t size;
	Record *array; 
	pthread_mutex_t lock; 
}Entry;

typedef struct {
	size_t nentries;
	Entry* entries;
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
	Entry* entry = &ht->entries[idx];
	#ifdef DEBUG 
		printf("Insert key - %s",key);
	#endif
	pthread_mutex_lock(&entry->lock); 
	#ifdef DEBUG 
		printf("\nlock acquired on entry %zu ",idx); 
	#endif
	if(entry->size == 0) {
		//new entry
		entry->size = 1;
		entry->array = (Record*)malloc(sizeof(Record));
		
		//new record
		entry->array[0].key = (char*)malloc(strlen(key)+1);
		strncpy(entry->array[0].key, key,strlen(key));
		entry->array[0].count = 1;
		entry->array[0].min = value;
		entry->array[0].max = value;
		entry->array[0].sum = value;
		
		ht->nentries++;
	}else {
		//entry exists at idx, check if record exists
		int i;
		Record *record=NULL;
		for(i=0;i<entry->size;i++) {
			if(!strcmp(entry->array[i].key,key)){
				#ifdef DEBUG 
					printf("\n\tfound existing record %s",key); 
				#endif
				record = &entry->array[i];
				break;
			}
		}
		if(record == NULL) {
			//create new record
			entry->size++;
			#ifdef DEBUG 
				printf("\nentry size = %zu",entry->size); 
			#endif
			entry->array = (Record*) realloc(entry->array,sizeof(Record)*(entry->size));
			entry->array[entry->size-1].key = (char*)malloc(strlen(key)+1);
			strncpy(entry->array[entry->size-1].key, key,strlen(key));
			entry->array[entry->size-1].count = 1;
			entry->array[entry->size-1].sum = value;
			entry->array[entry->size-1].max = value;
			entry->array[entry->size-1].min = value;
		}
		else {
			//update existing record
			record->count += 1;
			record->sum += value;
			if(record->max < value)
				record->max = value;
			if(record->min > value)
				record->min = value;
		}
	}	
	#ifdef DEBUG 
		printf("\nnumber of entries = %zu",ht->nentries); 
	#endif
	pthread_mutex_unlock(&entry->lock); 
	#ifdef DEBUG 
		printf("\nlock released on entry %zu ",idx); 
	#endif
	return 0;
}

Record* get(Hashtable *ht, char* key) {
	if(ht==NULL || key==NULL)
		return NULL;

	size_t key_hash = hash(key);
	size_t idx = key_hash % HT_SIZE;
	Record *record=NULL;	
	Entry* entry = &ht->entries[idx];
	
	if(!entry || entry->size==0){
		return NULL;
	}
	for(int i=0;i<entry->size;i++) {
		record = &entry->array[i];
		if(!strcmp(record->key,key)) {
			return record;
		}
	}	
	return NULL;	
}

Hashtable* create_hashtable() {
	Hashtable *ht = (Hashtable*)malloc(sizeof(Hashtable));
	ht->nentries = 0;
	ht->entries = (Entry*) malloc(sizeof(Entry)*HT_SIZE);
	
	for(int i=0;i<HT_SIZE;i++) {
		ht->entries[i].size = 0;
		ht->entries[i].array = NULL;
		if (pthread_mutex_init(&ht->entries[i].lock, NULL) != 0) { 
        	#ifdef DEBUG 
				printf("\n mutex init has failed for entry no: %d",i);  
			#endif
        	return NULL; 
    	} 
	}
	return ht;
}

//TODO:
void delete_hashtable(void);

int print_ht(Hashtable* ht) {
	if(ht == NULL)
		return -1;
	
	int size;
	for(int i=0;i<HT_SIZE;i++) {
		if(ht->entries[i].size == 0) {
			printf("\nNULL");
		}
		else{
			size = ht->entries[i].size;
			printf("\n");
			for(int j=0;j<size;j++) {
				printf("%s ",ht->entries[i].array[j].key);
			}
		}
	}
	return 0;	
}


//int calculate_mean_and_print_result(Hashtable *ht) {
//	
//}

char** get_keys(Hashtable *ht, int *count) {
	char **keys ;
	keys = (char**)malloc(sizeof(char*)*HT_SIZE);
	if(ht == NULL)
		return NULL;
	int cnt = 0;	
	int size;
	for(int i=0;i<HT_SIZE;i++) {
		if(ht->entries[i].size != 0) {
			size = ht->entries[i].size;
			for(int j=0;j<size;j++) {
				keys[cnt] = (char*)malloc(strlen(ht->entries[i].array[j].key)+1);
				strcpy(keys[cnt],ht->entries[i].array[j].key);
				cnt++;
			}
		}
	}
	*count = cnt;
	return keys;
}

typedef struct {
	char **buffer;
	size_t buf_sz;
	size_t n_elems;
	size_t head;
	size_t tail;
	pthread_mutex_t rlock;
	pthread_mutex_t wlock;	
	pthread_mutex_t nlock;	

} RingBuffer;

RingBuffer* create_ring_buffer() {
	RingBuffer *rb = (RingBuffer*) malloc(sizeof(RingBuffer));
	rb->buf_sz = RB_SIZE;
	rb->buffer = (char**)malloc(sizeof(char*));
	rb->n_elems = 0;
	rb->head = 0;
	rb->tail = 0;
	pthread_mutex_init(&rb->wlock,NULL);
	pthread_mutex_init(&rb->rlock,NULL);
	pthread_mutex_init(&rb->nlock,NULL);
	return rb;
}

//TODO:
void delete_ring_buffer(void);

bool is_buffer_empty(RingBuffer *rb) {
	if(!rb)
		return false;
//	if((rb->head+1)% RB_SIZE >= rb->tail) 
	if(rb->n_elems == 0)
		return true;
	return false;
}

bool is_buffer_full(RingBuffer *rb) {
	if(!rb)
		return false;
//	if((rb->tail+1)% RB_SIZE >= rb->head) 
	if(rb->n_elems == RB_SIZE)
		return true;
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
		pthread_mutex_lock(&rb->nlock);
		rb->n_elems--;
		pthread_mutex_unlock(&rb->nlock);
	
		return data;
	}	
	printf("\nBuffer is Empty!!");
	nanosleep(sleep,sleep);
	return NULL;
}

bool write_buffer(RingBuffer *rb, char** data,struct timespec *sleep) {
	if(!rb)
		return false;

	if(!is_buffer_full(rb)) {
		pthread_mutex_lock(&rb->wlock);
		rb->buffer[rb->tail] = *data;
		rb->tail = (rb->tail+1)%RB_SIZE;
		pthread_mutex_unlock(&rb->wlock);
		pthread_mutex_lock(&rb->nlock);
		rb->n_elems++;
		pthread_mutex_unlock(&rb->nlock);
	
		return true;
	}	
	printf("\nBuffer is Full!!");
	nanosleep(sleep,sleep);
	return false;
}

typedef struct {
	char name[25];
	RingBuffer *rb;
	Hashtable *ht;
	FILE *file;
    struct timespec sleep; 

}Thread_arg;

bool game_over = false;

void* rb_write(void* arg) {
	FILE *file = (FILE*) ((Thread_arg*)arg)->file;
	RingBuffer *rb = (RingBuffer*)((Thread_arg*)arg)->rb;
	char *line =(char*) malloc(256);;
	struct timespec sleep = ((Thread_arg*)arg)->sleep;
	bool written;

	printf("\nwriter thread started");
	while(fgets(line, 256, file)) { 
		printf("\nwriting line %s",line);
		written = false;
		while(!written){
			written = write_buffer(rb,&line,&sleep);
			//if(!written)
			//	nanosleep(&sleep,&sleep);
		}
		line = (char*)malloc(256);
	}
	printf("\nReached EOF,writer thread exiting");
	while(!is_buffer_empty(rb));//wait for readers to finish
	game_over = true;
	return NULL;
}


void* rb_read(void* arg) {
	RingBuffer *rb = (RingBuffer*)((Thread_arg*)arg)->rb;
	char *thread_name = (char*)((Thread_arg*)arg)->name;
	char *line = NULL;
	char *name,*brkb,*reading;
	float value;
	Hashtable *ht = (Hashtable *)((Thread_arg*)arg)->ht;
	struct timespec sleep = ((Thread_arg*)arg)->sleep;

	printf("\nreader thread %s started",thread_name);
	while(!game_over) {
		printf("\nreader thread %s reading",thread_name);
		line = (char*) read_buffer(rb,&sleep);
		if(line != NULL) {
			name = strtok_r(line,";",&brkb);
			reading= strtok_r(0,"\n",&brkb);
			if(reading==NULL) {
				fprintf(stderr,"\nInvalid reading from line %s",line);
				//ignore this line
				//goto sleep;
				continue;
			}
			value = strtof(reading,NULL);    
			printf("\ninserting %s : %f", name,value);
			//insert(ht,name,value);	 
			free(line);
			line = NULL;
		}
		/*else{
			sleep:
			nanosleep(&sleep,&sleep);
		}*/
	}
	#ifdef DEBUG
	printf("\nreader thread %s exited",thread_name);
	#endif
	return NULL;
}


void* rb_test(void) {
	const char* filename = "data.txt";
    FILE* file = fopen(filename,"r"); 
	int error;
	RingBuffer *rb = create_ring_buffer();
	Hashtable *ht = create_hashtable();
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
		reader->sleep.tv_nsec = 100;		

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


void ht_test() {
	const char* filename = "data.txt";
    FILE* file = fopen(filename,"r"); 
    char line[256]; 
	char *name,*brkb,*reading;
	float value;
	Hashtable *ht = create_hashtable();
    while (fgets(line, sizeof(line), file)) { 
		name = strtok_r(line,";",&brkb);
		reading= strtok_r(0,"\n",&brkb);
		value = strtof(reading,NULL);    
		printf("\ninserting %s-%f", name,value);
		insert(ht,name,value);	
    } 
    fclose(file); 
	print_ht(ht);
	
	int key_count;
	char **keys=get_keys(ht,&key_count);
	printf("\n\nKEYS-get(key)");
	for(int i=0;i<key_count;i++) {
		Record *record = get(ht,keys[i]);
		if(record)
			printf("\n%s - %f",keys[i],(float)record->sum/(float)record->count);
		else
			printf("\n%s - Not Found" ,keys[i]);
	}
}

int main() {
	rb_test();
	//ht_test();
	return 0;
}
