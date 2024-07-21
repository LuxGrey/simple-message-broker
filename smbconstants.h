/**
 * smbconstants.h
 *
 * Defines shared constants used by smb programs
 */

#ifndef _SMBCONSTANTS_H_
#define _SMBCONSTANTS_H_

#define TOPIC_LENGTH 20

static const int broker_port = 8080;
/**
 * Delimiter character that is to be used to separate different components of
 * a request that is sent to a broker
 */
static const char msg_delim = '!';
static const char topic_wildcard = '#';
static const char *method_publish = "PUB!";
static const char *method_subscribe = "SUB!";
static const char *method_unsubscribe = "UNSUB!";

#endif