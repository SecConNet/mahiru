/** Functions for starting a subprocess and communicating with it. */
#pragma once

#include <sys/types.h>


/** Run a command and optionally communicate with it.
 *
 * @param filename The file to execute.
 * @param argv Arguments to pass (may be NULL).
 * @param env Environment variables to set (may be NULL).
 * @param in_buf Data to send to stdin (may be NULL).
 * @param in_size Length of input data.
 * @param out_buf (out) Pointer to a buffer with output received from the
 *              command. This buffer will be allocated by this function, and
 *              must be  freed using free() by the caller after use.
 * @param out_size (out) Pointer to a variable to store the number of chars in
 *              the output buffer in.
 * @return 0 on success, -1 on error.
 */
int run(
        const char * filename, char * const argv[], char * const env[],
        const char * in_buf, ssize_t in_size,
        const char ** out_buf, ssize_t * out_size);

