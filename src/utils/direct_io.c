#define _GNU_SOURCE
#include "utils/direct_io.h"
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#ifdef __APPLE__
#define O_DIRECT 0
#endif

void
dio_buffer_init(DirectIOBuffer *dio, size_t size)
{
    if (dio->initialized)
        return;

    dio->buffer_size = size;
    dio->cached_start = 0;
    dio->cached_end = 0;
    dio->fd = -1;

    if (posix_memalign((void **)&dio->buffer, 4096, size) != 0)
    {
        elog(ERROR, "posix_memalign failed: %s", strerror(errno));
    }

    dio->initialized = true;
}

int
dio_open_file(const char *path, int flags, mode_t mode)
{
    int fd;
    int open_flags = flags;

#ifndef __APPLE__
    open_flags |= O_DIRECT;
#endif

    fd = open(path, open_flags, mode);

#ifdef __APPLE__
    if (fd != -1)
    {
        if (fcntl(fd, F_NOCACHE, 1) == -1)
        {
             /* Not fatal, but good to know */
             elog(WARNING, "fcntl(F_NOCACHE) failed for %s: %s", path, strerror(errno));
        }
    }
#endif

    return fd;
}

ssize_t
dio_buffer_read_block(DirectIOBuffer *dio, char *target, off_t offset, size_t size)
{
    off_t chunk_start;
    ssize_t read_bytes;

    if (!dio->initialized)
        elog(ERROR, "DirectIOBuffer not initialized");

    /* Check if the requested block is already in the cache */
    if (dio->fd != -1 && offset >= dio->cached_start && offset + size <= dio->cached_end)
    {
        memcpy(target, dio->buffer + (offset - dio->cached_start), size);
        return size;
    }

    /* Block not in cache, read new chunk */
    dio->cached_start = (offset / 1024 / 1024) * 1024 * 1024; /* Align to 1MB */
    chunk_start = dio->cached_start;

    read_bytes = pread(dio->fd, dio->buffer, dio->buffer_size, chunk_start);
    if (read_bytes < 0)
        return -1;

    dio->cached_end = chunk_start + read_bytes;

    /* If we read enough to cover the requested block */
    if (offset >= dio->cached_start && offset + size <= dio->cached_end)
    {
        memcpy(target, dio->buffer + (offset - dio->cached_start), size);
        return size;
    }

    /* We might have reached EOF and read less than requested */
    if (offset < dio->cached_end)
    {
        size_t available = dio->cached_end - offset;
        size_t to_copy = (available < size) ? available : size;
        memcpy(target, dio->buffer + (offset - dio->cached_start), to_copy);
        return to_copy;
    }

    return 0; /* EOF */
}

void
dio_buffer_cleanup(DirectIOBuffer *dio)
{
    if (!dio->initialized)
        return;

    if (dio->buffer)
        free(dio->buffer);

    dio->buffer = NULL;
    dio->initialized = false;
}
