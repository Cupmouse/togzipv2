#include <iostream>
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <unordered_map>
#include <map>
#include <set>
#include <string>

#include "common.h"
#include "bitmex.h"

#define BITMEX_MAX_ORDERS 20000
#define BITMEX_SIDE_INDEX(side) (strcmp(side, "Buy") == 0)

using namespace rapidjson;

void send_bitmex(char *message, char *channel) {
    // do nothing
    std::cerr << "this should not be called" << std::endl;
    exit(1);
}

struct BitmexBookElement {
    double price;
    uint64_t size;
};

struct BitmexReconstructedBookElement {
    uint64_t id;
    uint64_t size;
};

// map<channel, map<bookelement_id, bookelement>>
std::unordered_map<std::string, std::unordered_map<uint64_t, BitmexBookElement>[2]> bitmexOrderbooks;
std::unordered_map<std::string, Document> instruments;
std::set<std::string> bitmex_subscribed;

inline void bitmex_record_orderbook(Document &doc) {
    const char *action = doc["action"].GetString();
    const auto data = doc["data"].GetArray();

    if (strcmp(action, "partial") == 0 || strcmp(action, "insert") == 0) {
        std::set<std::string> involved;
        
        for (auto&& elem : data) {
            std::string symbol(elem["symbol"].GetString());
            const uint64_t id = elem["id"].GetUint64();
            const char* side = elem["side"].GetString();
            const double price = elem["price"].GetDouble();
            // bitmex did whoopsie on ETHUSD, this could be negative
            // https://blog.bitmex.com/ja-jp-ethusd-orderbook-feed-issues-24-june-2019/
            if (!elem["size"].IsUint64()) {
                continue;
            }
            const uint64_t size = elem["size"].GetUint64();

            auto& orders = bitmexOrderbooks[symbol][BITMEX_SIDE_INDEX(side)];
            orders[id] = { price: price, size: size };

            std::set<double> dueToRemove;
            auto& oppositeOrders = bitmexOrderbooks[symbol][!BITMEX_SIDE_INDEX(side)];
            if (BITMEX_SIDE_INDEX(side) == BITMEX_SIDE_INDEX("Sell")) {
                for (auto i = oppositeOrders.begin(); i != oppositeOrders.end(); i++) {
                    if (i->second.price >= price) {
                        dueToRemove.insert(i->first);
                    }
                }
            } else {
                for (auto i = oppositeOrders.begin(); i != oppositeOrders.end(); i++) {
                    if (i->second.price <= price) {
                        dueToRemove.insert(i->first);
                    }
                }
            }
            for (auto i = dueToRemove.begin(); i != dueToRemove.end(); i++) {
                std::cerr << side << " " << price << " erase " << *i << std::endl;
                oppositeOrders.erase(*i);
            }
            involved.insert(symbol);
        }
    } else if (strcmp(action, "update") == 0) {
        for (auto&& elem : data) {
            // update the size
            std::string symbol(elem["symbol"].GetString());
            const char *side = elem["side"].GetString();
            const uint64_t id = elem["id"].GetUint64();
            if (!elem["size"].IsUint64()) {
                continue;
            }
            const uint64_t size = elem["size"].GetUint64();
            auto& orderbook = bitmexOrderbooks[symbol][BITMEX_SIDE_INDEX(side)];

            if (orderbook.find(id) == orderbook.end()) {
                continue;
            }
            BitmexBookElement order = orderbook[id];
            order.size = size;
            orderbook[id] = order;
        }
    } else if (strcmp(action, "delete") == 0) {
        for (auto&& elem : data) {
            // delete it from the record
            std::string symbol(elem["symbol"].GetString());
            const uint64_t id = elem["id"].GetUint64();
            const char *side = elem["side"].GetString();
            auto& orderbook = bitmexOrderbooks[symbol][BITMEX_SIDE_INDEX(side)];
            // erase it from map
            orderbook.erase(id);
        }
    } else {
        std::cerr << "unknown action type: " << action << std::endl;
        exit(1);
    }
}

inline void bitmex_instrument(Document &doc) {
    const char *action = doc["action"].GetString();
    const auto data = doc["data"].GetArray();

    if (strcmp(action, "partial") == 0 || strcmp(action, "insert") == 0) {
        for (auto&& elem : data) {
            std::string symbol(elem["symbol"].GetString());
            auto& copied = instruments[symbol];
            copied.CopyFrom(elem, copied.GetAllocator());
        }
    } else if (strcmp(action, "update") == 0) {
        for (auto&& elem : data) {
            std::string symbol(elem["symbol"].GetString());
            if (instruments.find(symbol) == instruments.end()) {
                std::cerr << "instrument for " << symbol << " is not prepared" << std::endl;
                continue;
            }
            auto& instrDoc = instruments[symbol];
            for (auto member = elem.MemberBegin(); member != elem.MemberEnd(); member++) {
                instrDoc[member->name.GetString()].CopyFrom(member->value, instrDoc.GetAllocator());
            }
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
        } else if (strncmp(channel, "instrument", N_CHANNEL) == 0) {
            bitmex_instrument(doc);
        }

    } else if (doc.HasMember("info")) {
        strcpy(channel, "info");

    } else if (doc.HasMember("subscribe")) {
        strncpy(channel, doc["subscribe"].GetString(), N_CHANNEL);
        bitmex_subscribed.insert(channel);

    } else if (doc.HasMember("error")) {
        strcpy(channel, "error");

    } else {
        std::cerr << "unknown channel" << std::endl;
        exit(1);
    }
}

inline void bitmex_snapshot_orderbook(unsigned long long ts, FILE *out) {
    Document doc(kArrayType);
    auto& alloc = doc.GetAllocator();

    // for each symbol
    for (auto&& symbolSides : bitmexOrderbooks) {
        std::string symbol = symbolSides.first;
        auto sideIds = symbolSides.second;

        // we need to reconstruct orderbook in order to get rid of the excessive orders
        // if we don't perform this, we sometimes get over 300 MB of orderbook data.
        std::map<double, BitmexReconstructedBookElement> recSells; 
        std::map<double, BitmexReconstructedBookElement> recBuys; 

        auto& memSellOrders = sideIds[BITMEX_SIDE_INDEX("Sell")];
        for (auto&& idOrder : memSellOrders) {
            recSells[idOrder.second.price] = BitmexReconstructedBookElement{
                id: idOrder.first,
                size: idOrder.second.size
            };
        }
        int i = 0;
        for (auto itr = recSells.begin(); i < BITMEX_MAX_ORDERS && itr != recSells.end(); itr++) {
            auto price = itr->first;
            auto order = itr->second;

            Value bookelem(kObjectType);
            Value symbolVal(symbol.c_str(), alloc);
            bookelem.AddMember("symbol", symbolVal, alloc);
            bookelem.AddMember("id", order.id, alloc);
            bookelem.AddMember("side", "Sell", alloc);
            bookelem.AddMember("price", price, alloc);
            bookelem.AddMember("size", order.size, alloc);
            doc.PushBack(bookelem, alloc);

            i++;
        }
        if (i == BITMEX_MAX_ORDERS) {
            std::cerr << "Sell cut down: " << symbol << " " << recSells.size() << std::endl;
        }
        auto& memBuyOrders = sideIds[BITMEX_SIDE_INDEX("Buy")];
        for (auto&& idOrder : memBuyOrders) {
            recBuys[idOrder.second.price] = BitmexReconstructedBookElement{
                id: idOrder.first,
                size: idOrder.second.size
            };
        }
        i = 0;
        for (auto itr = recBuys.rbegin(); i < BITMEX_MAX_ORDERS && itr != recBuys.rend(); itr++) {
            auto price = itr->first;
            auto order = itr->second;

            Value bookelem(kObjectType);
            Value symbolVal(symbol.c_str(), alloc);
            bookelem.AddMember("symbol", symbolVal, alloc);
            bookelem.AddMember("id", order.id, alloc);
            bookelem.AddMember("side", "Buy", alloc);
            bookelem.AddMember("price", price, alloc);
            bookelem.AddMember("size", order.size, alloc);
            doc.PushBack(bookelem, alloc);

            i++;
        }
        if (i == BITMEX_MAX_ORDERS) {
            std::cerr << "Buy cut down: " << symbol << " " << recBuys.size() << std::endl;
        }
    }

    // write it to char array (string)
    StringBuffer sb;
    Writer<StringBuffer> writer(sb);
    doc.Accept(writer);

    fprintf(out, "state\t%llu\torderBookL2\t", ts);
    fputs(sb.GetString(), out);
    fputc('\n', out);
}

inline void bitmex_snapshot_instrument(unsigned long long ts, FILE *out) {
    Document doc(kArrayType);
    auto& alloc = doc.GetAllocator();

    for (auto&& keyValue : instruments) {
        Value val(kObjectType);
        val.CopyFrom(keyValue.second, alloc);
        doc.PushBack(val, alloc);
    }

    // write it to char array (string)
    StringBuffer sb;
    Writer<StringBuffer> writer(sb);
    doc.Accept(writer);

    fprintf(out, "state\t%llu\tinstrument\t", ts);
    fputs(sb.GetString(), out);
    fputc('\n', out);
}

// parse orderbook status into a line
void status_bitmex(unsigned long long ts, FILE *out) {
    Document subscribed(kArrayType);
    for (auto&& channel : bitmex_subscribed) {
        Value valChannel(kStringType);
        valChannel.SetString(channel.c_str(), subscribed.GetAllocator());
        subscribed.PushBack(valChannel, subscribed.GetAllocator());
    }
    StringBuffer subSb;
    Writer<StringBuffer> subsWriter(subSb);
    subscribed.Accept(subsWriter);
    fprintf(out, "state\t%llu\t%s\t", ts, CHANNEL_SUBSCRIBED);
    fputs(subSb.GetString(), out);
    fputc('\n', out);

    bitmex_snapshot_orderbook(ts, out);
    bitmex_snapshot_instrument(ts, out);
}
