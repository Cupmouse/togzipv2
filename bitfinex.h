#ifndef BITFINEX_H
#define BITFINEX_H

void send_bitfinex(char *message, char *channel);

void msg_bitfinex(char *message, char *channel);

void status_bitfinex(unsigned long long ts, FILE *out);

#endif