#include <iostream>
#include <map>
#include <string>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>

#include "common.h"
#include "bitfinex.h"

using namespace rapidjson;

std::map<unsigned int, char*> bitfinex_idvch;

struct BitfinexBookElement {
    uint64_t count;
    double amount;
};

std::map<unsigned int, std::map<double, BitfinexBookElement>*> bitfinex_orderbooks;

inline void bitfinex_record_orderbook_single(unsigned int chanId, GenericArray<false, rapidjson::Value> order) {
    double price = order[0].GetDouble();
    uint64_t count = order[1].GetUint64();
    // negative if ask
    double amount = order[2].GetDouble();

    auto& orderbook = *bitfinex_orderbooks[chanId];
    if (count == 0) {
        // remove
        if (orderbook.erase(price) != 1) {
            std::cerr << "erase failed" << std::endl;
            exit(1);
        }
    } else {
        orderbook[price] = { count: count, amount: amount };
    }
}

inline void bitfinex_record_orderbook(unsigned int chanId, GenericArray<false, rapidjson::Value> orders) {
    if (bitfinex_orderbooks.find(chanId) == bitfinex_orderbooks.end()) {
        bitfinex_orderbooks[chanId] = new std::map<double, BitfinexBookElement>;
    }

    if (!orders[0].IsArray()) {
        // just one order will be flattened
        bitfinex_record_orderbook_single(chanId, orders);
    } else {
        for (auto& order : orders) {
            bitfinex_record_orderbook_single(chanId, order.GetArray());
        }
    }
}

void status_bitfinex(unsigned long long ts, FILE *out) {
    for (auto chanIdOrderbook : bitfinex_orderbooks) {
        unsigned int chanId = chanIdOrderbook.first;
        auto& orderbook = *chanIdOrderbook.second;
        char *channel = bitfinex_idvch[chanId];

        Document doc(kArrayType);
        auto& allocator = doc.GetAllocator();

        for (auto order : orderbook) {
            Value valOrder(kArrayType);

            Value valPrice(kNumberType);
            Value valCount(kNumberType);
            Value valAmount(kNumberType);

            double price = order.first;
            valPrice.SetDouble(price);
            valOrder.PushBack(valPrice, allocator);
            uint64_t count = order.second.count;
            valCount.SetUint64(count);
            valOrder.PushBack(valCount, allocator);
            double amount = order.second.amount;
            valAmount.SetDouble(amount);
            valOrder.PushBack(valAmount, allocator);
            
            doc.PushBack(valOrder, allocator);
        }

        StringBuffer sb;
        Writer<StringBuffer> writer(sb);
        doc.Accept(writer);

        fprintf(out, "status\t%llu\t%s\t", ts, channel);
        fputs(sb.GetString(), out);
        fputc('\n', out);
    }
}

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
        unsigned int chanId = doc[0].GetUint();
        strncpy(channel, bitfinex_idvch[chanId], N_CHANNEL);

        if (startswith(channel, "book")) {
            if (doc[1].IsArray()) {
                // there could be hb which is heartbeat
                char *symbol = new char[N_PAIR];
                strncpy(symbol, channel+strlen("book_"), N_PAIR);
                bitfinex_record_orderbook(chanId, doc[1].GetArray());
                delete [] symbol;
            }
        }
    }
}
