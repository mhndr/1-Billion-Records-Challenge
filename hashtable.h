
#define HT_SIZE 99999

typedef struct {
	wchar_t *key;
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


int insert(Hashtable *ht, wchar_t* key, float value);
Record* get(Hashtable *ht, wchar_t* key);
Hashtable* create_hashtable();
void delete_hashtable(Hashtable *ht);
int print_ht(Hashtable* ht);
wchar_t** get_keys(Hashtable *ht, size_t *count);



