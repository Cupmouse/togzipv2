#include <iostream>
#include <limits>
#include <string.h>
#define N_L 10000000

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

int main(int argc, char *argv[]) {
    /* start reading */
    // buffer for storing an line
    char* buf = (char*) std::malloc(sizeof(char)*N_L);
    // initialize buffer
    memset(buf, 0, N_L);

    unsigned long long line_timestamp;
    
    // read head
    // constant "head"
    std::cin.getline(buf, N_L, ',');
    // constant "0"
    std::cin.getline(buf, N_L, ',');
    // datetime
    std::cin.getline(buf, N_L, ',');
    line_timestamp = timestamp_nanosec(buf);
    // constant websocket
    std::cin.getline(buf, N_L, ',');
    // constant "0"
    std::cin.getline(buf, N_L, ',');
    // url
    std::cin.getline(buf, N_L);

    std::cout << "start\t" << line_timestamp << '\t' << buf << std::endl;


    while (std::cin.getline(buf, N_L, ',')) {
        if (buf[0] == 'm' && buf[1] == 's' && buf[2] == 'g') {
            // read timestamp
            std::cin.getline(buf, N_L, ',');
            line_timestamp = timestamp_nanosec(buf);

            // rest of the line is a msg
            std::cin.getline(buf, N_L);

            // v2
            std::cout << "msg\t" << line_timestamp << '\t' << buf << std::endl;

        } else if (buf[0] == 'e' && buf[1] == 'm' && buf[2] == 'i' && buf[3] == 't') {
            std::cin.getline(buf, N_L, ',');
            line_timestamp = timestamp_nanosec(buf);

            std::cin.getline(buf, N_L);
            
            // v2
            std::cout << "send\t" << line_timestamp << '\t' << buf << std::endl;

        } else if (buf[0] == 'e' && buf[1] == 'o' && buf[2] == 's') {
            std::cin.getline(buf, N_L, ',');
            line_timestamp = timestamp_nanosec(buf);

            std::cout << "end\t" << line_timestamp << std::endl;

            // end of stream, ignore lest
            std::cin.ignore(std::numeric_limits<std::streamsize>::max());

        } else {
            std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        }
    }

    return 0;
}