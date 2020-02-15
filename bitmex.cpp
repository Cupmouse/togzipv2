#include <iostream>
#include <rapidjson/document.h>

#include "common.h"
#include "bitmex.h"

using namespace rapidjson;

void send_bitmex(char *message, char *channel) {
    // do nothing
    std::cerr << "this should not be called" << std::endl;
    exit(1);
}

void msg_bitmex(char *message, char *channel) {
    Document doc;

    doc.Parse(message);

    if (doc.HasMember("table")) {
        strncpy(channel, doc["table"].GetString(), N_CHANNEL);

    } else if (doc.HasMember("info")) {
        strcpy(channel, "info");

    } else if (doc.HasMember("subscribe")) {
        strncpy(channel, doc["subscribe"].GetString(), N_CHANNEL);

    } else if (doc.HasMember("error")) {
        strcpy(channel, "error");

    } else {
        std::cerr << "unknown channel" << std::endl;
        exit(1);
    }
}
