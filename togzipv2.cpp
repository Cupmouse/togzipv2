#include <time.h>
#include <iostream>
#include <limits>
#include <string.h>
#include <string>
#define N_BUFFER 10000000
#define N_COMMAND 1000
#define N_TIMESTAMP 100
#define CALC_MIN(nanosec) (nanosec/1000000000/60)

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
    // buffer for storing an datetime
    char* timestamp = (char*) malloc(sizeof(char)*N_TIMESTAMP);
    // buffer for storing an line
    char* buf = (char*) malloc(sizeof(char)*N_BUFFER);
    // char array to store command to write to
    char* command = (char*) malloc(sizeof(char)*N_COMMAND);

    // timestamp
    unsigned long long ts;
    // hours in unixtime
    unsigned long long mins;
    
    // read head
    // constant "head"
    std::cin.getline(buf, N_BUFFER, ',');
    // constant "0"
    std::cin.getline(buf, N_BUFFER, ',');
    // datetime
    std::cin.getline(buf, N_BUFFER, ',');
    ts = timestamp_nanosec(buf);
    mins = CALC_MIN(ts);

    // make out filename
    snprintf(command, N_COMMAND, "pigz -9 > converted/%s_%llu.gzip", exchange, ts);
    // open pipe to output to
    FILE* out = popen(command, "w");

    // constant websocket
    std::cin.getline(buf, N_BUFFER, ',');
    // constant "0"
    std::cin.getline(buf, N_BUFFER, ',');
    // url
    std::cin.getline(buf, N_BUFFER);

    fprintf(out, "start\t%llu\t%s\n", ts, buf);

    char c;
    
    while (std::cin.getline(buf, N_BUFFER, ',')) {
        // read timestamp
        fread(timestamp, sizeof(char), strlen("2020-01-01 19:12:03.000000,"), stdin);
        *(timestamp+strlen("2020-01-01 19:12:03.000000")) = '\0';

        ts = timestamp_nanosec(timestamp);

        // if hours is different, then new file
        if (CALC_MIN(ts) != mins) {
            pclose(out);
            
            snprintf(command, N_COMMAND, "pigz -9 > converted/%s_%llu.gzip", exchange, ts);
            out = popen(command, "w");

            mins = CALC_MIN(ts);
        }

        if (buf[0] == 'm' && buf[1] == 's' && buf[2] == 'g') {
            // rest of the line is a msg
            // v2
            fprintf(out, "msg\t%llu\t", ts);
            do {
                c = fgetc(stdin);
                fputc(c, out);
            } while (c != '\n');

        } else if (buf[0] == 'e' && buf[1] == 'm' && buf[2] == 'i' && buf[3] == 't') {
            // v2
            fprintf(out, "send\t%llu\t", ts);
            do {
                c = fgetc(stdin);
                fputc(c, out);
            } while (c != '\n');

        } else if (buf[0] == 'e' && buf[2] == 'r' && buf[2] == 'r') {
            // v2
            fprintf(out, "err\t%llu\t", ts);
            do {
                c = fgetc(stdin);
                fputc(c, out);
            } while (c != '\n');

        } else if (buf[0] == 'e' && buf[1] == 'o' && buf[2] == 's') {
            fprintf(out, "end\t%llu\n", ts);

            // end of stream, ignore lest
            std::cin.ignore(std::numeric_limits<std::streamsize>::max());

        } else {
            std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        }
    }

    pclose(out);
    free(buf);
    free(command);

    return 0;
}