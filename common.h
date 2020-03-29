#ifndef TOGIPV2_COMMON_H
#define TOGIPV2_COMMON_H

#define N_BUFFER 10000000
#define N_COMMAND 1000
#define N_TIMESTAMP 100
#define N_CHANNEL 1000
#define N_PAIR 100
#define CALC_MIN(nanosec) (nanosec/1000000000/60)

struct charstringcmparator {
     bool operator()(char const *a, char const *b) const {
        return strcmp(a, b) < 0;
    }
};

bool startswith(const char *str, const char *prefix);

#endif