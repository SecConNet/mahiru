/** Functions for starting a subprocess and communicating with it. */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include "subprocess.h"


typedef struct {
    int parent_out, child_in;
    int child_out, parent_in;
} io_pipes_t;


/** Create pipes for communicating with an external subprocess.
 *
 * @param pipes An io_pipes_t object that will be initialised to contain two
 *      pipes. Must be allocated, will be overwritten.
 * @return 0 on success, -1 on error.
 */
static int create_pipes(io_pipes_t * pipes) {
    int fds[2];

    if (pipe(fds) != 0) {
        perror("Allocating first pipe");
        goto exit_0;
    }
    pipes->parent_out = fds[1];
    pipes->child_in = fds[0];

    if (pipe(fds) != 0) {
        perror("Allocating second pipe");
        goto exit_1;
    }
    pipes->child_out = fds[1];
    pipes->parent_in = fds[0];

    return 0;

exit_1:
    close(pipes->parent_out);
    close(pipes->child_in);

exit_0:
    return -1;
}


/** Set up pipes on the child process side.
 *
 * Reconfigures standard in, out and error to use the given pipes to
 * communicate, and closes the unused fds as appropriate.
 *
 * @param pipes Pipes to use to communicate with the parent.
 * @return 0 on success, -1 on error.
 */
static int setup_pipes_for_child(io_pipes_t const * pipes) {
    if (dup2(pipes->child_in, STDIN_FILENO) < 0) {
        perror("Redirecting stdin");
        goto exit_0;
    }

    if (dup2(pipes->child_out, STDOUT_FILENO) < 0) {
        perror("Redirecting stdout");
        goto exit_0;
    }

    if (dup2(pipes->child_out, STDERR_FILENO) < 0) {
        perror("Redirecting stderr");
        goto exit_0;
    }

    if (close(pipes->parent_out) != 0)
        perror("Closing pipe in child");

    if (close(pipes->child_in) != 0)
        perror("Closing pipe in child");

    if (close(pipes->child_out) != 0)
        perror("Closing pipe in child");

    if (close(pipes->parent_in) != 0)
        perror("Closing pipe in child");

    return 0;

exit_0:
    return -1;
}


/** Set up pipes on the parent process side.
 *
 * This closes unused fds as appropriate.
 *
 * @param pipes Pipes to use to communicate with the child.
 * @return 0 on success, -1 on error.
 */
static int setup_pipes_for_parent(io_pipes_t const * pipes) {
    if (close(pipes->child_in) != 0)
        perror("Closing pipe in parent");
    if (close(pipes->child_out) != 0)
        perror("Closing pipe in parent");
    return 0;
}


/** Write a data buffer to a file descriptor.
 *
 * @param fd The file descriptor to write to.
 * @param data The data to write.
 * @param len Length of the data to write.
 * @return 0 on success, -1 on error, in which case errno will be set.
 */
static int write_all(int fd, char const * data, ssize_t len) {
    ssize_t num_written;

    while (len > 0) {
        num_written = write(fd, data, len);
        if (num_written < 0)
            return -1;
        data += num_written;
        len -= num_written;
    }
    return 0;
}


/** Read data from a file descriptor until it closes.
 *
 * @param fd The file descriptor to read from
 * @param buffer (out) The buffer the data has been written to. This must be
 *          freed by the caller using free(). Will be unchanged in case of
 *          error.
 * @param size The number of chars of data in the buffer. Will be unchanged in
 *          case of error
 * @return 0 on success, -1 on failure.
 */
static int read_all(int fd, const char ** buffer, ssize_t * size) {
    const ssize_t chunk_size = 1024;
    ssize_t num_read, total_read = 0;
    char *buf = NULL;
    ssize_t buf_size = 0;

    do {
        if (buf_size == total_read) {
            buf = (char*)realloc(buf, buf_size + chunk_size);
            if (buf == NULL) {
                perror("When reading external program stdout/err");
                goto exit_0;
            }
            buf_size += chunk_size;
        }

        num_read = read(fd, buf + total_read, buf_size - total_read);
        if (num_read < 0) {
            perror("Reading from external program stdout/err");
            goto exit_0;
        }
        total_read += num_read;
    } while (num_read > 0);

    *buffer = buf;
    *size = total_read;
    return 0;

exit_0:
    free(buf);
    return -1;
}


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
        const char ** out_buf, ssize_t * out_size)
{
    io_pipes_t pipes;
    int child_pid;

    char *none = NULL;

    const char * out_buf_ = NULL;
    ssize_t out_size_ = 0;

    int status, exit_status;

    if (create_pipes(&pipes) != 0)
        goto exit_0;

    child_pid = fork();

    if (child_pid == 0) {
        // child
        if (setup_pipes_for_child(&pipes) != 0)
            exit(1);

        if (argv == NULL)
            argv = &none;
        if (env == NULL)
            env = &none;

        execve(filename, argv, env);
        perror("Executing command");
        exit(255);
    }
    else if (child_pid > 0) {
        // parent
        if (setup_pipes_for_parent(&pipes) != 0)
            goto exit_0;

        if (in_buf != NULL) {
            if (write_all(pipes.parent_out, in_buf, in_size) != 0) {
                perror("Writing to stdin in parent");
                goto exit_2;
            }
        }

        if (close(pipes.parent_out) != 0) {
            perror("Closing stdin pipe in parent");
            goto exit_1;
        };

        if (out_buf != NULL) {
            if (read_all(pipes.parent_in, &out_buf_, &out_size_) != 0) {
                goto exit_1;
            }
        }

        if (close(pipes.parent_in) != 0) {
            perror("Closing stdout/err pipe in parent");
            goto exit_0;
        }

        waitpid(child_pid, &status, 0);

        if (WIFEXITED(status)) {
            exit_status = WEXITSTATUS(status);
            if (exit_status != 0) {
                fprintf(stderr, "Child exited with status %d\n", exit_status);
            }
        }
        else if (WIFSIGNALED(status)) {
            exit_status = WTERMSIG(status);
            if (exit_status != 0) {
                fprintf(stderr, "Child terminated by signal %d\n", exit_status);
            }
        }

        *out_buf = out_buf_;
        *out_size = out_size_;
        return 0;
    }
    else {
        // fork error
        close(pipes.parent_out);
        close(pipes.child_in);
        close(pipes.child_out);
        close(pipes.parent_in);
    }

exit_2:
    close(pipes.parent_out);

exit_1:
    close(pipes.parent_in);

exit_0:
    return -1;
}

