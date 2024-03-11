#include<stdio.h>
#include<string.h>
#include<stdlib.h>



char **str_split(const char *in, size_t in_len, char delm, size_t *num_elm, size_t max)
{
    char   *parsestr;
    char   **out;
    size_t  cnt = 1;
    size_t  i;

    if (in == NULL || in_len == 0 || num_elm == NULL)
        return NULL;

    parsestr = malloc(in_len+1);
    memcpy(parsestr, in, in_len+1);
    parsestr[in_len] = '\0';

    *num_elm = 1;
    for (i=0; i<in_len; i++) {
        if (parsestr[i] == delm)
            (*num_elm)++;
        if (max > 0 && *num_elm == max)
            break;
    }

    out    = malloc(*num_elm * sizeof(*out));
    out[0] = parsestr;
    for (i=0; i<in_len && cnt<*num_elm; i++) {
        if (parsestr[i] != delm)
            continue;

      /* Add the pointer to the array of elements */
        parsestr[i] = '\0';

       out[cnt] = parsestr+i+1;
        cnt++;
    }

    return out;
}

void str_split_free(char **in, size_t num_elm)
{
    if (in == NULL)
        return;
    if (num_elm != 0)
        free(in[0]);
    free(in);
}
/*
int main(int argc,char**argv){
	if(argc!=3) {
		printf("\nusage strsep <string> <seperator>");
		return -1;
	}
	char *test = argv[1];
	char *name,*value;
	size_t tok_count=0;
	char **tokens = str_split(test,strlen(test),*argv[2],&tok_count,strlen(test));
	if(tok_count < 2) {
		printf("\nseperator not found, unable to split string");
		return -1;  
	}
	name = tokens[0];
	value = tokens[1];
	printf("%s %s",name,value); 
	str_split_free(tokens,tok_count);
	return 0;
}*/
