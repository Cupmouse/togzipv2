#include <iostream>
#include <map>
#include <string>
#include <rapidjson/document.h>

#include "common.h"
#include "bitfinex.h"

using namespace rapidjson;

std::map<unsigned int, char*> bitfinex_idvch;

void send_bitfinex(char *message, char *channel) {
    Document doc;

    doc.Parse(message);

    snprintf(channel, N_CHANNEL, "%s_%s", doc["channel"].GetString(), doc["symbol"].GetString());
}

void msg_bitfinex(char *message, char *channel) {
    Document doc;

    doc.Parse(message);

    if (doc.IsObject()) {
        if (strcmp(doc["event"].GetString(), "subscribed") == 0) {
            // response to subscribe
            const char *event_channel = doc["channel"].GetString();
            const char *symbol = doc["symbol"].GetString();
            unsigned int chanId = doc["chanId"].GetUint();

            char *val = (char *) malloc(sizeof(char)*N_CHANNEL);

            snprintf(val, N_CHANNEL, "%s_%s", event_channel, symbol);

            // set channel name for id
            bitfinex_idvch[chanId] = val;
            
            strncpy(channel, val, N_CHANNEL);
        } else if (strcmp(doc["event"].GetString(), "info") == 0) {
            strcpy(channel, "info");
        } else {
            std::cerr << "unknown channel" << std::endl;
            exit(1);
        }
    } else {
        // must be an array
        // normal channel message
        strncpy(channel, bitfinex_idvch[doc[0].GetUint()], N_CHANNEL);
    }
}
