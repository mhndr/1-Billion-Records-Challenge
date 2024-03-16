#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <wchar.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/semaphore.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include "hashtable.h"

#define THREAD_MAX (10)


typedef struct {
	char name[25];
	Hashtable *ht;
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
	char *delim,*value;
	wchar_t *name;
	Hashtable *ht = ((Thread_arg*)arg)->ht;
	char *iter = ((Thread_arg*)arg)->chunk_start;
	char *last = ((Thread_arg*)arg)->chunk_end;

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
		insert(ht,name,strtof(iter,NULL));
		*delim = '\n';
		iter = delim+1; 
	}
#ifdef DEBUG
	fclose(out_file);
#endif
	printf("\nThread %s done",thread_name);
	return NULL;
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


int main(int argc, char *argv[]) {
	struct stat finfo;
	int fh, chunk,error;
	char *chunk_start=NULL,*chunk_end=NULL,*mem_start;
	int thread_count = THREAD_MAX,i;
	Hashtable *ht = create_hashtable();
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
	calculate_mean_and_print_result(ht);
	munmap(mem_start, finfo.st_size);
	close(fh);
	return 0;
}

