#ifndef __QPROCESSOR_H__
#define __QPROCESSOR_H__

int process_rest_request (char const json[], char const folder[], char message[], uint64_t *const io_blocks_counter, double *const io_mb_counter, request_t const type);
char* qprocessor (char command[], char const folder[], char message[], uint64_t *const io_blocks_counter, double *const io_mb_counter, int fd);

#endif

