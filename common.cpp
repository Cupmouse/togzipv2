#include <string.h>

#include "common.h"

bool startswith(const char *str, const char *prefix) {
    return strlen(str) >= strlen(prefix) && strncmp(str, prefix, strlen(prefix)) == 0;
}
