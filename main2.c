#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<wchar.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>


#define THREAD_MAX (10)


pthread_t tid[THREAD_MAX];


typedef struct {
	char name[25];
	//Buffer_t *rb;
	//Hashtable *ht;
	FILE *out_file;
	char *chunk_start;
	char *chunk_end;
}Thread_arg;


void * do_read(void* arg) {
	FILE *out_file = ((Thread_arg*)arg)->out_file;
	char *thread_name = ((Thread_arg*)arg)->name;
	char *end,*name,*value;
	
	char *row_start = ((Thread_arg*)arg)->chunk_start;
	char *last = ((Thread_arg*)arg)->chunk_end;
	
	printf("\nThread %s started",thread_name);
	while(row_start!=last) {
		end = strchr(row_start, ';');
		if(end==0)
			break;
		*end = '\0';
		name = strdup(row_start);
		*end = ';'; 
		row_start = end+1;

		end = strchr(row_start, '\n');
		if(end==0)
			break;
		*end = '\0';
		value = strdup(row_start);
		fprintf(out_file,"%s : %f\n",name,strtof(value,NULL));
		*end = '\n';
		row_start = end+1; 
	}
	fclose(out_file);
	printf("\nThread %s done",thread_name);
	return NULL;
}


int main(int argc, char *argv[]) {
	struct stat finfo;
	int fh, chunk,error;
	char *chunk_start=NULL,*chunk_end=NULL,*mem_start;
	Thread_arg reader[THREAD_MAX];

	if(stat(argv[1], &finfo) == -1) return 0;
	if((fh = open(argv[1], O_RDWR)) == -1) return 0;
	printf("\nopened file , file size is %lld",finfo.st_size);

	mem_start = (char*)mmap(NULL, finfo.st_size, PROT_READ|PROT_WRITE,MAP_SHARED , fh, 0);
	if(mem_start == (char*)-1) return 0;
	madvise(mem_start, finfo.st_size, POSIX_MADV_SEQUENTIAL);
	chunk_start = mem_start;
	chunk = finfo.st_size/THREAD_MAX;
	
	//initialize thread args
	for(int i=0;i<THREAD_MAX;i++) {
		sprintf(reader[i].name,"reader_%d",i);
		reader[i].out_file = fopen(reader->name,"wb");
		if(reader[i].out_file == NULL) //if file does not exist, create it
   		{
        	reader[i].out_file = fopen(reader->name, "wb");
        	if (reader[i].out_file == NULL) {
				fprintf(stderr,"\nError while file to write");
				return -1;
			}
    	}
		//TODO: partition the file better
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
	}
	
	//create threads
	for(int i=0;i<THREAD_MAX;i++) {
		error = pthread_create(&(tid[i]), NULL, &do_read, (void*)&reader[i]); 
		if (error != 0){
			fprintf(stderr,"\nfailed to create writer thread %d",i);
			return -1;
		}
	}
	
	//wait till threads finish
	for(int i=0;i<THREAD_MAX;i++) {
		pthread_join(tid[i],NULL);
	}

	//calculate_mean();
	munmap(mem_start, finfo.st_size);
	close(fh);
	return 0;
}

