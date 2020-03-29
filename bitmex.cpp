#include <iostream>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <map>

#include "common.h"
#include "bitmex.h"

#define BITMEX_SIDE_INDEX(side) (strcmp(side, "Buy") == 0)

using namespace rapidjson;

void send_bitmex(char *message, char *channel) {
    // do nothing
    std::cerr << "this should not be called" << std::endl;
    exit(1);
}

struct BitmexBookKey {
    char *symbol;
    int side;
    uint64_t id;

    bool operator < (const BitmexBookKey& another) const {
        int str = strcmp(symbol, another.symbol);
        if (str == 0) {
            if (side == another.side) {
                return id < another.id;
            } else {
                return side < another.side;
            }
        } else {
            return str < 0;
        }
    }
};

struct BitmexBookElement {
    double price;
    uint64_t size;
};

// map<channel, map<bookelement_id, bookelement>>
std::map<BitmexBookKey, BitmexBookElement> orderbooks;

void bitmex_record_orderbook(Document &doc) {
    const char *action = doc["action"].GetString();
    const auto data = doc["data"].GetArray();

    if (strcmp(action, "partial") == 0 || strcmp(action, "insert") == 0) {
        for (auto& elem : data) {
            char *symbol = new char[N_PAIR];
            strncpy(symbol, elem["symbol"].GetString(), N_PAIR);
            const uint64_t id = elem["id"].GetUint64();
            const char* side = elem["side"].GetString();
            BitmexBookKey key = {symbol, BITMEX_SIDE_INDEX(side), id};

            const double price = elem["price"].GetDouble();
            const uint64_t size = elem["size"].GetUint64();
            BitmexBookElement bookelem = { price, size };

            if (orderbooks.find(key) == orderbooks.end()) {
                // this symbol is not included in the map
                orderbooks[key] = bookelem;
            } else {
                orderbooks[key] = bookelem;
                delete [] symbol;
            }
        }
    } else if (strcmp(action, "update") == 0) {
        for (auto& elem : data) {
            // update the size
            char *symbol = new char[N_PAIR];
            strncpy(symbol, elem["symbol"].GetString(), N_PAIR);
            const char *side = elem["side"].GetString();
            const uint64_t id = elem["id"].GetUint64();
            const uint64_t size = elem["size"].GetUint64();
            BitmexBookKey key = { symbol, BITMEX_SIDE_INDEX(side), id };
            BitmexBookElement bookelem = orderbooks[key];
            bookelem.size = size;
            orderbooks[key] = bookelem;
            delete [] symbol;
        }
    } else if (strcmp(action, "delete") == 0) {
        for (auto& elem : data) {
            // delete it from the record
            char *symbol = new char[N_PAIR];
            strncpy(symbol, elem["symbol"].GetString(), N_PAIR);
            const uint64_t id = elem["id"].GetUint64();
            const char *side = elem["side"].GetString();
            BitmexBookKey key = { symbol, BITMEX_SIDE_INDEX(side), id };
            // erase it from map
            if (orderbooks.erase(key) != 1) {
                std::cerr << "erase failed" << std::endl;
                exit(1);
            }
            delete [] key.symbol;
        }
    } else {
        std::cerr << "unknown action type: " << action << std::endl;
        exit(1);
    }
}

void msg_bitmex(char *message, char *channel) {
    Document doc;

    // kParseFullPrecisionFlag is needed because of the orderbook recording
    doc.Parse<kParseFullPrecisionFlag>(message);

    if (doc.HasMember("table")) {
        strncpy(channel, doc["table"].GetString(), N_CHANNEL);

        if (strncmp(channel, "orderBookL2", N_CHANNEL) == 0) {
            // if this is orderBookL2 topic, then we need to preserve orderbooks
            bitmex_record_orderbook(doc);
        }

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

// parse orderbook status into a line
void status_bitmex(unsigned long long ts, FILE *out) {
    Document doc(kArrayType);
    auto& alloc = doc.GetAllocator();

    // for each symbol
    for (auto& keyValue : orderbooks) {
        Value bookelem(kObjectType);
        auto key = keyValue.first;
        auto value = keyValue.second;
        Value symbolVal(key.symbol, alloc);
        bookelem.AddMember("symbol", symbolVal, alloc);
        bookelem.AddMember("id", key.id, alloc);
        Value side(key.side ? "Buy" : "Sell", alloc);
        bookelem.AddMember("side", side, alloc);
        bookelem.AddMember("price", value.price, alloc);
        bookelem.AddMember("size", value.size, alloc);
        // add it on the end of the side array
        doc.PushBack(bookelem, alloc);
    }

    // write it to char array (string)
    StringBuffer sb;
    Writer<StringBuffer> writer(sb);
    doc.Accept(writer);

    fprintf(out, "status\t%llu\torderBookL2\t", ts);
    fputs(sb.GetString(), out);
    fputc('\n', out);
}
