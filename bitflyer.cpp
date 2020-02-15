#include <iostream>
#include <map>
#include <string>
#include <rapidjson/document.h>

#include "common.h"
#include "bitflyer.h"

using namespace rapidjson;

std::map<unsigned int, char*> bitflyer_idvch;

void send_bitflyer(char *message, char *channel) {
    Document doc;

    doc.Parse(message);

    const char *params_channel = doc["params"]["channel"].GetString();
    unsigned int chanId = doc["id"].GetUint();

    // set channel name for id
    char *val = (char *) malloc(sizeof(char)*N_CHANNEL);
    strncpy(val, params_channel, N_CHANNEL);
    bitflyer_idvch[chanId] = val;
    
    strncpy(channel, params_channel, N_CHANNEL);
}

void msg_bitflyer(char *message, char *channel) {
    Document doc;

    doc.Parse(message);

    if (doc.HasMember("method")) {
        if (strcmp(doc["method"].GetString(), "channelMessage") == 0) {
            // normal channel message
            strncpy(channel, doc["params"]["channel"].GetString(), N_CHANNEL);
        } else {
            std::cerr << "unknown channel" << std::endl;
            exit(1);
        }
    } else {
        // response to subscribe
        strncpy(channel, bitflyer_idvch[doc["id"].GetUint()], N_CHANNEL);
    }
}
