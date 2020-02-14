#include <time.h>
#include <iostream>
#include <limits>
#include <string.h>
#include <string>
#define N_BUFFER 10000000
#define N_COMMAND 1000
#define N_TIMESTAMP 100

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

inline void standardize_time(char *str) {
    unsigned long long nanosec;

    *(str+strlen("2020-01-01")) = 'T';
    nanosec = atol(str+strlen("2020-01-01 19:12:03.")) * 1000;
    snprintf(str+strlen("2020-01-01 19:12:03."), 20, "%09llu", nanosec);
    *(str+strlen("2020-01-01 19:12:03.000000000")) = 'Z';
    *(str+strlen("2020-01-01 19:12:03.000000000Z")) = '\0';
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

    // last digit of hour
    char last_hours;
    
    // read head
    // constant "head"
    std::cin.getline(buf, N_BUFFER, ',');
    // constant "0"
    std::cin.getline(buf, N_BUFFER, ',');
    // datetime
    std::cin.getline(timestamp, N_TIMESTAMP, ',');
    last_hours = *(timestamp+strlen("2020-01-01 1"));

    // make out filename
    snprintf(command, N_COMMAND, "pigz -9 > converted/%s_%llu.gzip", exchange, timestamp_nanosec(timestamp));
    // open pipe to output to
    FILE* out = popen(command, "w");

    // standardize time
    standardize_time(timestamp);
    // constant websocket
    std::cin.getline(buf, N_BUFFER, ',');
    // constant "0"
    std::cin.getline(buf, N_BUFFER, ',');
    // url
    std::cin.getline(buf, N_BUFFER);

    fprintf(out, "{\"type\":\"start\",\"timestamp\":\"%s\"\"data\":\"%s\"}\n", timestamp, buf);

    while (std::cin.getline(buf, N_BUFFER, ',')) {
        // read timestamp
        std::cin.getline(timestamp, N_TIMESTAMP, ',');

        // if hours is different, then new file
        if (*(timestamp+strlen("2020-01-01T1")) != last_hours) {
            pclose(out);
            
            snprintf(command, N_COMMAND, "pigz -9 > converted/%s_%llu.gzip", exchange, timestamp_nanosec(timestamp));
            out = popen(command, "w");

            last_hours = *(timestamp+strlen("2020-01-01T1"));
        }

        // standardize
        standardize_time(timestamp);

        if (buf[0] == 'm' && buf[1] == 's' && buf[2] == 'g') {
            // rest of the line is a msg
            std::cin.getline(buf, N_BUFFER);

            // v2
            fputs("{\"type\":\"msg\",\"timestamp\":\"", out);
            fputs(timestamp, out);
            fputs("\",\"data\":", out);
            fputs(buf, out);
            fputs("}\n", out);

        } else if (buf[0] == 'e' && buf[1] == 'm' && buf[2] == 'i' && buf[3] == 't') {
            std::cin.getline(buf, N_BUFFER);
            
            // v2
            fputs("{\"type\":\"send\",\"timestamp\":\"", out);
            fputs(timestamp, out);
            fputs("\",\"data\":", out);
            fputs(buf, out);
            fputs("}\n", out);

        } else if (buf[0] == 'e' && buf[1] == 'o' && buf[2] == 's') {
            fputs("{\"type\":\"end\",\"timestamp\":\"", out);
            fputs(timestamp, out);
            fputs("\"}\n", out);

            // end of stream, ignore lest
            std::cin.ignore(std::numeric_limits<std::streamsize>::max());

        } else {
            std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        }
    }

    pclose(out);
    free(buf);
    free(command);
    free(timestamp);

    return 0;
}