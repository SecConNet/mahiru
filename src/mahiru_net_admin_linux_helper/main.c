/** Functions for starting a subprocess and communicating with it. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "subprocess.h"


int main(int argc, char * argv[]) {
    char * program = "/bin/cat";
    char * const args[] = {program, NULL};
    char * const env[] = {NULL};

    const char * input = "Testing";

    const char * out = NULL;
    ssize_t out_size = 0;

    char * strbuf;

    if (run(program, args, env, input, strlen(input), &out, &out_size) != 0)
        return EXIT_FAILURE;

    strbuf = (char*)malloc(out_size + 1);
    memcpy(strbuf, out, out_size);
    strbuf[out_size] = '\0';
    free((void*)out);

    printf("Output: %s", strbuf);
    free(strbuf);

    return EXIT_SUCCESS;
}

