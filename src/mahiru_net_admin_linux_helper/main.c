#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

#include "subprocess.h"


int main(int argc, char * argv[]) {
    char * program = "/sbin/ip";
    char * const args[] = {
        program, "link", "add", "veth-test", "type", "veth",
        NULL};
    char * const env[] = {NULL};

    const char * input = NULL;
    ssize_t in_size = 0l;

    const char * out = NULL;
    ssize_t out_size = 0;

    char * strbuf;

    if (run(program, args, env, input, in_size, &out, &out_size) != 0)
        return EXIT_FAILURE;

    strbuf = (char*)malloc(out_size + 1);
    memcpy(strbuf, out, out_size);
    strbuf[out_size] = '\0';
    free((void*)out);

    if (out_size != 0)
        printf("Command output:\n%s", strbuf);
    free(strbuf);

    return EXIT_SUCCESS;
}

