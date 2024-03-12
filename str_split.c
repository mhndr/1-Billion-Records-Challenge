#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<wchar.h>


wchar_t** split_wchar(const wchar_t* str, wchar_t delimiter, size_t* token_count) {
    int count = 1; 
    for (const wchar_t* p = str; *p != L'\0'; ++p) {
        if (*p == delimiter) {
            ++count;
        }
    }

    // Allocate memory for the array of wchar_t pointers
    wchar_t** tokens = malloc(count * sizeof(wchar_t*));
    if (!tokens) {
        fprintf(stderr, "Memory allocation error\n");
        exit(1);
    }

    // Copy tokens into the array
    int index = 0;
    const wchar_t* start = str;
    for (const wchar_t* p = str; ; ++p) {
        if (*p == delimiter || *p == L'\0') {
            int len = p - start;
            tokens[index] = malloc((len + 1) * sizeof(wchar_t));
            if (!tokens[index]) {
                fprintf(stderr, "Memory allocation error\n");
                exit(1);
            }
            wcsncpy(tokens[index], start, len);
            tokens[index][len] = L'\0';
            ++index;
            start = p + 1;
        }
        if (*p == L'\0') {
            break;
        }
    }

    *token_count = count;
    return tokens;
}

void str_split_free(wchar_t **in, size_t num_elm)
{
    if (in == NULL)
        return;
	for (int i = 0; i < num_elm; ++i) {
        free(in[i]); 
    }

    free(in); 
}
/*

//TESTS

int main(int argc,wchar_t**argv){
	if(argc!=3) {
		printf("\nusage strsep <string> <seperator>");
		return -1;
	}
	wchar_t *test = argv[1];
	wchar_t *name,*value;
	size_t tok_count=0;
	wchar_t **tokens = str_split(test,strlen(test),*argv[2],&tok_count,strlen(test));
	if(tok_count < 2) {
		printf("\nseperator not found, unable to split string");
		return -1;  
	}
	name = tokens[0];
	value = tokens[1];
	printf("%s %s",name,value); 
	str_split_free(tokens,tok_count);
	return 0;
}

int main() {
    FILE *file;
    wchar_t line[256]; // Assuming maximum line length of 255 characters
    wchar_t *name,*value;
	size_t tok_count=0;
	wchar_t **tokens;
	
    // Open the file in binary mode to handle Unicode characters
    file = fopen("data.txt", "rb");
    if (file == NULL) {
        perror("Error opening file");
        return 1;
    }

    // Read each line from the file
    while (fgetws(line, sizeof(line), file) != NULL) {
        // Print the Unicode string to the terminal
		
		// Duplicate the wide character string using wcsdup
		wchar_t *duplicate = wcsdup(line);
		if (duplicate == NULL) {
			perror("Error duplicating string");
			return 1;
		}

		// Print the original and duplicated strings
		wprintf(L"\nOriginal: %ls", line);
		wprintf(L"\tDuplicated: %ls", duplicate);
		
		tokens = split_wchar(duplicate ,L';',&tok_count);
		if(tok_count < 2) {
			printf("\nseperator not found, unable to split string");
			continue;
		}
		name = tokens[0];
		value = tokens[1];
		wprintf(L"\t%ls - %ls",name,value); 
		str_split_free(tokens,tok_count);

		// Free the memory allocated for the duplicated string
		free(duplicate);
	}

    // Close the file
    fclose(file);

    return 0;
}
*/
