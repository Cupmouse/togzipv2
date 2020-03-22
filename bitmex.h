#ifndef BITMEX_H
#define BITMEX_H

void send_bitmex(char *message, char *channel);

void msg_bitmex(char *message, char *channel);

void status_bitmex(unsigned long long ts, FILE *out);

#endif