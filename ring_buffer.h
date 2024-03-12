#include<sys/semaphore.h>

typedef struct {
	wchar_t **records;
	int n;
	size_t count;
	int front;
	int rear;
	sem_t *mutex;
	sem_t *slots;
	sem_t *items;
//	pthread_mutex_t mutex;
//	pthread_mutex_t hlock;
//	pthread_mutex_t tlock;	

}Buffer_t;


Buffer_t* Buffer_create(int n);
void  	  Buffer_delete(Buffer_t *buffer);
bool  	  Buffer_insert(Buffer_t *buffer,wchar_t *record);
wchar_t*  Buffer_remove(Buffer_t *buffer);
bool  	  Buffer_empty(Buffer_t *buffer);

