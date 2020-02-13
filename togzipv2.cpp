#include <time.h>
#include <iostream>
#include <limits>
#include <string.h>
#define N_L 10000000
#define N_FILENAME 1000
#define N_TYPE 10
#define CALC_HOURS(nanosec) ((nanosec/1000000000/3600)%24)

inline unsigned long long timestamp_nanosec(char *str) {
    struct tm time;
    unsigned long long nanosec;

    memset(&time, 0, sizeof(struct tm));

    time.tm_isdst = -1;
    strptime(str, "%Y-%m-%d %H:%M:%S", &time);

    nanosec = ((unsigned long long) timegm(&time)) * 1000000000;
    nanosec += atol(str+strlen("2020-01-01 19:12:03.")) * 1000;

    return nanosec;
}

int main(int argc, const char *argv[]) {
    const char* exchange = argv[1];

    /* start reading */
    // buffer for storing type
    char* type = (char*) malloc(sizeof(char)*N_TYPE);
    memset(type, 0, N_TYPE);
    // buffer for storing an line
    char* buf = (char*) malloc(sizeof(char)*N_L);
    // initialize buffer
    memset(buf, 0, N_L);
    // char array to store command to write to
    char* comm = (char*) malloc(sizeof(char)*N_FILENAME);
    memset(comm, 0, N_FILENAME);

    unsigned long long line_timestamp;
    unsigned int last_hours;
    
    // read head
    // constant "head"
    std::cin.getline(buf, N_L, ',');
    // constant "0"
    std::cin.getline(buf, N_L, ',');
    // datetime
    std::cin.getline(buf, N_L, ',');
    line_timestamp = timestamp_nanosec(buf);
    last_hours = CALC_HOURS(line_timestamp);
    // constant websocket
    std::cin.getline(buf, N_L, ',');
    // constant "0"
    std::cin.getline(buf, N_L, ',');
    // url
    std::cin.getline(buf, N_L);

    // make out filename
    snprintf(comm, N_FILENAME, "pigz -9 > converted/%s_%llu.gzip", exchange, line_timestamp);
    // open pipe to output to
    FILE* out = popen(comm, "w");

    fprintf(out, "start\t%llu\t", line_timestamp);
    fputs(buf, out);
    fputc('\n', out);

    while (std::cin.getline(type, N_TYPE, ',')) {
        // read timestamp
        std::cin.getline(buf, N_L, ',');
        line_timestamp = timestamp_nanosec(buf);

        // if hours is different, then new file
        if (CALC_HOURS(line_timestamp) != last_hours) {
            pclose(out);
            
            snprintf(comm, N_FILENAME, "pigz -9 > converted/%s_%llu.gzip", exchange, line_timestamp);
            out = popen(comm, "w");

            last_hours = CALC_HOURS(line_timestamp);
        }

        if (type[0] == 'm' && type[1] == 's' && type[2] == 'g') {
            // rest of the line is a msg
            std::cin.getline(buf, N_L);

            // v2
            fprintf(out, "msg\t%llu\t", line_timestamp);
            fputs(buf, out);
            fputc('\n', out);

        } else if (type[0] == 'e' && type[1] == 'm' && type[2] == 'i' && type[3] == 't') {
            std::cin.getline(buf, N_L);
            
            // v2
            fprintf(out, "send\t%llu\t", line_timestamp);
            fputs(buf, out);
            fputc('\n', out);

        } else if (type[0] == 'e' && type[1] == 'o' && type[2] == 's') {
            fprintf(out, "end\t%llu\n", line_timestamp);

            // end of stream, ignore lest
            std::cin.ignore(std::numeric_limits<std::streamsize>::max());

        } else {
            std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        }
    }

    pclose(out);

    return 0;
}