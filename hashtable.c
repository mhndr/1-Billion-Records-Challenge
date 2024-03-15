#include <stdio.h>
#include <strings.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdbool.h>
#include <wchar.h>
#include "hashtable.h"

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



