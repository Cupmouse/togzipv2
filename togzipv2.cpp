#include <time.h>
#include <iostream>
#include <limits>
#include <string.h>
#include <string>

#include "common.h"
#include "bitfinex.h"
#include "bitflyer.h"
#include "bitmex.h"

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

    void (*get_channel_send)(char*, char*);
    void (*get_channel_msg)(char*, char*);
    void (*get_channel_status)(unsigned long long, FILE*);

    if (strcmp(exchange, "bitfinex") == 0) {
        exchange = "bitfinex-private";

        get_channel_send = send_bitfinex;
        get_channel_msg = msg_bitfinex;
        // get_channel_status = status_bitfinex;
    } else if (strcmp(exchange, "bitflyer") == 0) {
        get_channel_send = send_bitflyer;
        get_channel_msg = msg_bitflyer;
        // get_channel_status = status_bitflyer;
    } else if (strcmp(exchange, "bitmex") == 0) {
        get_channel_send = send_bitmex;
        get_channel_msg = msg_bitmex;
        get_channel_status = status_bitmex;
    } else {
        std::cerr << "unknown exchange" << std::endl;
        exit(1);
    }

    // buffer for storing an datetime
    char* timestamp = new char[N_TIMESTAMP];
    // buffer for storing an line
    char* buf = new char[N_BUFFER];
    // buffer for storing an status
    char* status = new char[N_BUFFER];
    // char array to store command to write to
    char* command = new char[N_COMMAND];

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
    snprintf(command, N_COMMAND, "gzip -9 > converted/%s_%llu.gz", exchange, ts);
    // open pipe to output to
    FILE* out = popen(command, "w");

    // constant websocket
    std::cin.getline(buf, N_BUFFER, ',');
    // constant "0"
    std::cin.getline(buf, N_BUFFER, ',');
    // url
    std::cin.getline(buf, N_BUFFER);

    fprintf(out, "start\t%llu\t%s\n", ts, buf);

    char *channel = new char[N_CHANNEL];
    
    while (std::cin.getline(buf, N_BUFFER, ',')) {
        // read timestamp
        fgets(timestamp, strlen("2020-01-01 19:12:03.000000,")+1, stdin);
        *(timestamp+strlen("2020-01-01 19:12:03.000000")) = '\0';

        ts = timestamp_nanosec(timestamp);

        // if hours is different, then new file
        if (CALC_MIN(ts) != mins) {
            pclose(out);
            
            snprintf(command, N_COMMAND, "gzip -9 > converted/%s_%llu.gz", exchange, ts);
            out = popen(command, "w");

            mins = CALC_MIN(ts);

            // for each 10 minutes, prints out board snapshot
            if (mins % 10 == 0) {
                // output status line
                get_channel_status(ts, out);
            }
        }

        if (buf[0] == 'm' && buf[1] == 's' && buf[2] == 'g') {
            // rest of the line is a msg
            std::cin.getline(buf, N_BUFFER);
            get_channel_msg(buf, channel);
            // v2
            fprintf(out, "msg\t%llu\t%s\t", ts, channel);
            fputs(buf, out);
            fputc('\n', out);

        } else if (buf[0] == 'e' && buf[1] == 'm' && buf[2] == 'i' && buf[3] == 't') {
            std::cin.getline(buf, N_BUFFER);
            get_channel_send(buf, channel);
            // v2
            fprintf(out, "send\t%llu\t%s\t", ts, channel);
            fputs(buf, out);
            fputc('\n', out);

        } else if (buf[0] == 'e' && buf[2] == 'r' && buf[2] == 'r') {
            std::cin.getline(buf, N_BUFFER);
            // v2
            fprintf(out, "err\t%llu\t", ts);
            fputs(buf, out);
            fputc('\n', out);

        } else if (buf[0] == 'e' && buf[1] == 'o' && buf[2] == 's') {
            fprintf(out, "end\t%llu\n", ts);

            // end of stream, ignore lest
            std::cin.ignore(std::numeric_limits<std::streamsize>::max());

        } else {
            std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        }
    }

    pclose(out);
    delete buf;
    delete command;
    delete status;
    delete timestamp;
    delete channel;

    return 0;
}