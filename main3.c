#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <wchar.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <glib.h>

#define THREAD_MAX (10)

typedef struct {
	int count;
	int sum;
	float min;
	float max;
	float mean;
}Record;

typedef struct {
	char name[25];
	GHashTable *ht;
#ifdef DEBUG
	FILE *out_file;
#endif
	char *chunk_start;
	char *chunk_end;
	sem_t *start;
}Thread_arg;

pthread_t tid[THREAD_MAX+1]; //+1 for remainder records
Thread_arg reader[THREAD_MAX+1];

void * do_read(void* arg) {
#ifdef DEBUG
	FILE *out_file = ((Thread_arg*)arg)->out_file;
#endif
	char *thread_name = ((Thread_arg*)arg)->name;
	char *delim;
	wchar_t *name;
	GHashTable *ht = ((Thread_arg*)arg)->ht;
	char *iter = ((Thread_arg*)arg)->chunk_start;
	char *last = ((Thread_arg*)arg)->chunk_end;
	float value;
	Record *record = NULL;

	sem_wait(((Thread_arg*)arg)->start);
	printf("\nThread %s started",thread_name);
	while(iter!=last) {
		delim = strchr(iter, ';');
		if(delim==0)
			break;
		*delim = '\0';
		name = (wchar_t*)malloc((delim-iter+1)*sizeof(wchar_t));
		mbstowcs(name,iter,delim-iter);
		*delim = ';';
		iter = delim+1;

		delim = strchr(iter, '\n');
		if(delim==0)
			break;
		*delim = '\0';
#ifdef DEBUG
		fprintf(out_file,"%ls : %f\n",name,strtof(iter,NULL));
#endif
		value = strtof(iter,NULL);
		record = g_hash_table_lookup(ht,name);
		if(!record) {
			record = (Record*)malloc(sizeof(Record));
			record->count = 0;
			record->min = value;
			record->max = value;
			record->sum = 0;
		}
		record->count++;
		record->sum += value;
		if(record->max < value)
			record->max = value;
		if(record->min > value)
			record->min = value;
		g_hash_table_insert(ht,name,record);

		*delim = '\n';
		iter = delim+1;
	}
#ifdef DEBUG
	fclose(out_file);
#endif
	printf("\nThread %s done",thread_name);
	return NULL;
}


void  calculate_mean_and_print_result(gpointer key, gpointer value, gpointer userData) {
	char* realKey =  (char*)key;
   	Record* record = (Record*)value;

	printf("\n\nRESULT ");
	if(record)
		printf("\n%ls - %f",realKey,(float)record->sum/(float)record->count);
	else
		printf("\n%ls - Not Found" ,realKey);

	return ;
}


int main(int argc, char *argv[]) {
	struct stat finfo;
	int fh, chunk,error;
	char *chunk_start=NULL,*chunk_end=NULL,*mem_start;
	int thread_count = THREAD_MAX,i;
	GHashTable *ht = g_hash_table_new_full(g_direct_hash, g_direct_equal,NULL,NULL); //TODO
	sem_t *start;

	if(stat(argv[1], &finfo) == -1) return 0;
	if((fh = open(argv[1], O_RDWR)) == -1) return 0;
	printf("\nopened file , file size is %lld",finfo.st_size);

	mem_start = (char*)mmap(NULL, finfo.st_size, PROT_READ|PROT_WRITE,MAP_SHARED , fh, 0);
	if(mem_start == (char*)-1) return 0;
	madvise(mem_start, finfo.st_size, MADV_RANDOM); //POSIX_MADV_SEQUENTIAL);
	chunk_start = mem_start;
	chunk = finfo.st_size/THREAD_MAX;


	sem_unlink("./sem_start");
	start = sem_open("./sem_mutex",O_CREAT,0644,THREAD_MAX+1);

	//initialize thread args
	for(i=0;i<THREAD_MAX;i++) {
		sprintf(reader[i].name,"reader_%d",i);
#ifdef DEBUG
		reader[i].out_file = fopen(reader[i].name,"wb");
		if(reader[i].out_file == NULL) //if file does not exist, create it
   		{
			fprintf(stderr,"\nError while file to write");
			return -1;
    	}
#endif
		reader[i].ht = ht;
		reader[i].start=start;
		if(chunk_start+chunk > mem_start+finfo.st_size){
			printf("\nchunks exceeded bounds, exiting");
			return -1;
		}
		char tmp =  *(chunk_start+chunk);
		*(chunk_start+chunk) = '\0';
		chunk_end = strrchr(chunk_start, '\n');//find the nearest end of line
		*(chunk_start+chunk) = tmp;
		if(i!=THREAD_MAX-1 && chunk_end==0){
			fprintf(stderr,"\nError while partitioning the file , thread_no %d chunk %d",i,chunk);
			return -1;
		}

		char *first_line_end = strchr(chunk_start,'\n');
		*first_line_end = '\0';
		printf("\nchunk size = %d",chunk);
		printf("\nfirst - %s",chunk_start);
		*first_line_end = '\n';

		//find the start of last line to print
		char  *iter = chunk_end;
		char *last_line_start = NULL;
		while(iter>mem_start ){
			iter--;
			if(*iter == '\n') {
				last_line_start = iter+1;
				*chunk_end = '\0';
				printf("\nlast - %s\n",last_line_start);
				*chunk_end = '\n';
				break;
			}
		}

		reader[i].chunk_start = chunk_start;
		reader[i].chunk_end = chunk_end;

		chunk_start = chunk_end+1;//start for next thread
		if((finfo.st_size-(chunk_start-mem_start))<chunk) {
			chunk = finfo.st_size-(chunk_start-mem_start);
		}
		printf("next chunk size = %d\n",chunk);

	}
	if(chunk !=0) {
		printf("\nRemainder\n%s",chunk_start);
		i=THREAD_MAX;
		sprintf(reader[i].name,"reader_extra");
#ifdef DEBUG
		reader[i].out_file = fopen(reader[i].name,"wb");
		if(reader[i].out_file == NULL) //if file does not exist, create it
   		{
			fprintf(stderr,"\nError while file to write");
			return -1;
    	}
#endif
		reader[i].ht = ht;
		reader[i].start = start;
		reader[i].chunk_start = chunk_start;
		reader[i].chunk_end = chunk_end;
		thread_count = THREAD_MAX+1;
	}

	//create threads
	for(int i=0;i<thread_count;i++) {
		error = pthread_create(&(tid[i]), NULL, &do_read, (void*)&reader[i]);
		if (error != 0){
			fprintf(stderr,"\nfailed to create writer thread %d",i);
			return -1;
		}
	}

	for(int i=0;i<thread_count;i++) {
		sem_post(start);
	}

	//wait till threads finish
	for(int i=0;i<thread_count;i++) {
		pthread_join(tid[i],NULL);
	}

	//print_ht(ht);
	g_hash_table_foreach(ht,calculate_mean_and_print_result,NULL);
	munmap(mem_start, finfo.st_size);
	g_hash_table_destroy(ht);
	close(fh);
	return 0;
}
