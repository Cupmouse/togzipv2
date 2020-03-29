#include <iostream>
#include <map>
#include <string>
#include <list>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>

#include "common.h"
#include "bitflyer.h"

#define BITFLYER_SIDE_INDEX(side) (strcmp(side, "bids") == 0)

using namespace rapidjson;

std::map<unsigned int, char*> bitflyer_idvch;

struct BitflyerBookKey {
    int side;
    double price;

    bool operator < (const BitflyerBookKey& a) const {
        if (side != a.side) {
            return side < a.side;
        } else {
            // in bid, higher is the better
            // in ask, lower is the better
            if (side == BITFLYER_SIDE_INDEX("bids")) {
                return price > a.price;
            } else {
                return price < a.price;
            }
        }
    }
};

std::map<char*, std::map<BitflyerBookKey, double>*, charstringcmparator> bitflyer_orderbooks;

void bitflyer_record_orderbook(char *symbol, GenericObject<false, rapidjson::Value> message) {
    auto& memOrderbook = *bitflyer_orderbooks[symbol];
    for (auto& ask : message["asks"].GetArray()) {
        double price = ask["price"].GetDouble();
        if (price == 0) {
            // this is itayose order execution, ignore
            continue;
        }
        double size = ask["size"].GetDouble();
        const BitflyerBookKey key = { side: BITFLYER_SIDE_INDEX("asks"), price };
        if (size == 0) {
            // delete
            // for some reason, snapshots are not complete, sometimes causing this erase to fail
            // ignore that
            memOrderbook.erase(key);
        } else {
            memOrderbook[key] = size;
        }
    }
    for (auto& bid : message["bids"].GetArray()) {
        double price = bid["price"].GetDouble();
        if (price == 0) continue;
        double size = bid["size"].GetDouble();
        const BitflyerBookKey key = { side: BITFLYER_SIDE_INDEX("bids"), price };
        if (size == 0) {
            memOrderbook.erase(key);
        } else memOrderbook[key] = size;
    }
}

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

    doc.Parse<kParseFullPrecisionFlag>(message);

    if (doc.HasMember("method")) {
        if (strcmp(doc["method"].GetString(), "channelMessage") == 0) {
            // normal channel message
            strncpy(channel, doc["params"]["channel"].GetString(), N_CHANNEL);
            
            // orderbook things
            if (startswith(channel, "lightning_board_snapshot_")) {
                char *symbol = new char[N_PAIR];
                strncpy(symbol, channel+strlen("lightning_board_snapshot_"), N_PAIR);
                if (bitflyer_orderbooks.find(symbol) == bitflyer_orderbooks.end()) {
                    bitflyer_orderbooks[symbol] = new std::map<BitflyerBookKey, double>;
                    bitflyer_record_orderbook(symbol, doc["params"]["message"].GetObject());
                    // this is the first time this symbol gets a snapshot
                    // don't delete symbol if this is the first time
                } else {
                    delete bitflyer_orderbooks[symbol];
                    bitflyer_orderbooks[symbol] = new std::map<BitflyerBookKey, double>;
                    bitflyer_record_orderbook(symbol, doc["params"]["message"].GetObject());
                    delete [] symbol;
                }
            } else if (startswith(channel, "lightning_board_")) {
                char *symbol = new char[N_PAIR];
                strncpy(symbol, channel+strlen("lightning_board_"), N_PAIR);
                // we somehow can get board change before getting a snapshot
                if (bitflyer_orderbooks.find(symbol) != bitflyer_orderbooks.end()) {
                    bitflyer_record_orderbook(symbol, doc["params"]["message"].GetObject());
                }
                delete [] symbol;
            }
        } else {
            std::cerr << "unknown channel" << std::endl;
            exit(1);
        }
    } else {
        // response to subscribe
        strncpy(channel, bitflyer_idvch[doc["id"].GetUint()], N_CHANNEL);
    }
}

void status_bitflyer(unsigned long long ts, FILE *out) {
    for (auto symbolOrderbook : bitflyer_orderbooks) {
        char *symbol = symbolOrderbook.first;
        auto& orderbook = *symbolOrderbook.second;

        Document doc(kObjectType);
        auto& allocator = doc.GetAllocator();
        Value asks(kArrayType);
        Value bids(kArrayType);
        
        // for all bid order
        for (auto keyOrder : orderbook) {
            auto key = keyOrder.first;
            auto size = keyOrder.second;
            
            Value order(kObjectType), valPrice(kNumberType), valSize(kNumberType);

            valPrice.SetDouble(key.price);
            order.AddMember("price", valPrice, allocator);
            valSize.SetDouble(size);
            order.AddMember("size", valSize, allocator);

            if (key.side == BITFLYER_SIDE_INDEX("bids")) {
                bids.PushBack(order, allocator);
            } else {
                asks.PushBack(order, allocator);
            }
        }
        doc.AddMember("bids", bids, allocator);
        doc.AddMember("asks", asks, allocator);

        StringBuffer sb;
        Writer<StringBuffer> writer(sb);
        doc.Accept(writer);

        fprintf(out, "status\t%llu\tlightning_board_snapshot_%s\t", ts, symbol);
        fputs(sb.GetString(), out);
        fputc('\n', out);
    }
}
