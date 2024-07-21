/**
 * smbbroker.c
 *
 * A message broker program that is compatible with the message publisher
 * program smbpublisher and the message subscriber program smbpublisher
 *
 * Does not require any arguments
 *
 * Runs in an infinite loop, accepting message publishes from any client
 * Published message will be immediately forwarded to any subscribers that are
 * currently subscribed to the topic of that message
 *
 * Allows for subscribers to subscribe to the '#' topic, which will result in
 * the broker forwarding messages of any topic to such subscribers
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>

#include "smbconstants.h"

#define SUB_ADDRESSES_LENGTH 10
#define TOPIC_SUBS_MAP_LENGTH 10
#define INDEX_WILDCARD_TOPIC 0
#define LOG_BUFFER_SIZE 1024

const char *empty_topic = "";
const in_addr_t empty_address = INADDR_NONE;
const char *log_file_name = "smbbroker.log";

char log_buffer[LOG_BUFFER_SIZE];
FILE *log_file;

typedef struct topic_subs_struct {
  char topic[TOPIC_LENGTH];
  struct sockaddr_in sub_addresses[SUB_ADDRESSES_LENGTH];
} topic_subs;

/**
 * A map where each entry maps a single topic to multiple subscriber addresses
 */
topic_subs topic_subs_map[TOPIC_SUBS_MAP_LENGTH];

/**
 * Writes the provided string to the log file, preceeded by the current date and
 * time
 */
void write_to_log(const char *log_str) {
  time_t current_time = time(NULL);
  struct tm *time_struct = localtime(&current_time);
  fprintf(log_file, "[%d-%02d-%02d %02d:%02d:%02d] %s\n",
          time_struct->tm_year + 1900, time_struct->tm_mon + 1,
          time_struct->tm_mday, time_struct->tm_hour, time_struct->tm_min,
          time_struct->tm_sec, log_str);
  fflush(log_file);
}

/**
 * Prints the provided string to the provided stream and also the log file.
 * Automatically appends a newline character when printing to the provided
 * stream.
 */
void fprintln_and_log(FILE *stream, const char *str) {
  fprintf(stream, "%s\n", str);
  write_to_log(str);
}

/**
 * Determines whether the two provided address structures are identical based
 * on their IP address and port.
 *
 * Returns true if the addresses are identical, otherwise returns false
 */
bool is_same_address(const struct sockaddr_in *addr1,
                     const struct sockaddr_in *addr2) {
  return addr1->sin_addr.s_addr == addr2->sin_addr.s_addr &&
         addr1->sin_port == addr2->sin_port;
}

/**
 * Sets the provided address structure to be recognizably empty
 */
void set_addr_empty(struct sockaddr_in *addr) {
  memset((void *)addr, 0, sizeof(*addr));
  // explicitly set s_addr to an "empty" value that can be compared against
  // later
  addr->sin_addr.s_addr = empty_address;
}

/**
 * If the provided topic structure has no subscribers, reset it so that the
 * entry is free to be used for a new topic.
 *
 * If the topic still has a subscribers, do nothing.
 */
void remove_unused_topic(topic_subs *topic_struct) {
  int i;

  // check if there is still an address subscribed to this topic
  for (i = 0; i < SUB_ADDRESSES_LENGTH; i++) {
    if (topic_struct->sub_addresses[i].sin_addr.s_addr != empty_address) {
      // subscriber found, exit without changing anything
      return;
    }
  }

  snprintf(log_buffer, LOG_BUFFER_SIZE,
           "Last subscriber was unsubscribed from topic '%s', removing topic",
           topic_struct->topic);
  fprintln_and_log(stderr, log_buffer);
  strcpy(topic_struct->topic, empty_topic);
}

/**
 * Attempts to find an appropriate topic subs instance for the provided topic in
 * the topic subs list
 *
 * Returns a pointer to the found instance or NULL if none could be found
 */
topic_subs *find_topic_sub(const char *topic) {
  int i;

  // attempt to find corresponding topic subs instance in list
  for (i = 0; i < TOPIC_SUBS_MAP_LENGTH; i++) {
    if (strncmp(topic_subs_map[i].topic, topic, sizeof(topic)) == 0) {
      // instance found, return pointer
      return &topic_subs_map[i];
    }
  }

  return NULL;
}

/**
 * Attempts to find the provided topic in the topic subs list
 *
 * If it is found, a pointer to the corresponding topic subs object is returned
 * If it is not found, a new corresponding topic subs object will be set up in
 * the list and a pointer for that object will be returned
 * If a new object cannot be set up because the list is full, NULL is returned
 */
topic_subs *find_or_insert_topic_sub(const char *topic) {
  topic_subs *found_topic_struct;
  int i;

  // attempt to find corresponding topic subs instance in list
  if ((found_topic_struct = find_topic_sub(topic)) != NULL) {
    return found_topic_struct;
  }

  // could not find suitable instance, so configure an unused one for the new
  // topic
  for (i = 0; i < TOPIC_SUBS_MAP_LENGTH; i++) {
    if (strlen(topic_subs_map[i].topic) == 0) {
      strcpy(topic_subs_map[i].topic, topic);
      return &topic_subs_map[i];
    }
  }

  // no unused instance remaining for the new topic
  snprintf(log_buffer, LOG_BUFFER_SIZE,
           "No more free slots to register new topic '%s'", topic);
  fprintln_and_log(stderr, log_buffer);
  return NULL;
}

/**
 * Sends the provided message to the provided address
 *
 * Returns 0 if message was sent without issues, otherwise returns 1 on error.
 */
int send_message(const char *message, struct sockaddr_in dest_addr,
                 int sock_fd) {
  socklen_t dest_size;
  int length, nbytes;

  dest_size = sizeof(dest_addr);

  length = strlen(message);
  nbytes = sendto(sock_fd, message, length, 0, (struct sockaddr *)&dest_addr,
                  dest_size);
  if (nbytes != length) {
    snprintf(log_buffer, LOG_BUFFER_SIZE,
             "Failed to send message '%s' to host %s:%d", message,
             inet_ntoa(dest_addr.sin_addr), ntohs(dest_addr.sin_port));
    fprintln_and_log(stderr, log_buffer);
    perror("sendto");
    return 1;
  }

  snprintf(log_buffer, LOG_BUFFER_SIZE, "Sent message '%s' to host %s:%d",
           message, inet_ntoa(dest_addr.sin_addr), ntohs(dest_addr.sin_port));
  fprintln_and_log(stderr, log_buffer);
  return 0;
}

/**
 * Validates the provided topic string
 *
 * Returns 0 if topic is valid, otherwise returns 1
 */
int validate_topic(const char *topic, bool wildcardAllowed) {
  // assert that topic is not an empty string, since that is reserved as an
  // identifier for empty topics
  if (strlen(topic) == 0) {
    fprintln_and_log(stderr, "Topic is not allowed to be an empty string");
    return 1;
  }

  // assert that topic is not too long to store
  if (strlen(topic) >= TOPIC_LENGTH) {
    snprintf(log_buffer, LOG_BUFFER_SIZE, "Topic '%s' exceeds max length of %u",
             topic, TOPIC_LENGTH);
    fprintln_and_log(stderr, log_buffer);
    return 1;
  }

  // assert that topic does not contain the message delimiter character
  if (strchr(topic, msg_delim) != NULL) {
    snprintf(
        log_buffer, LOG_BUFFER_SIZE,
        "Topic '%s' is not allowed to contain message delimiter character '%c'",
        topic, msg_delim);
    fprintln_and_log(stderr, log_buffer);
    return 1;
  }

  // assert that topic does not contain the wildcard character
  if (!wildcardAllowed && strchr(topic, topic_wildcard) != NULL) {
    snprintf(log_buffer, LOG_BUFFER_SIZE,
             "Topic '%s' is not allowed to contain wildcard character '%c'",
             topic, topic_wildcard);
    fprintln_and_log(stderr, log_buffer);
    return 1;
  }

  return 0;
}

/**
 * Handles a publish request
 *
 * Forwards received message to all subscribers of the specified topic and
 * all subscribers of the wildcard topic
 *
 * Returns 0 if published message could be forwarded without issues, otherwise
 * returns 1 on errors
 */
int handle_publish(char *request, int sock_fd) {
  char *topic, *message;
  topic_subs *found_topic;
  int i;

  // isolate request components
  // first jump over method, get the topic as the next token
  // and then use the remaining substring as message contents
  strtok(request, "!");
  topic = strtok(NULL, "!");
  message = strtok(NULL, "");

  // validate topic
  if (validate_topic(topic, false) != 0) {
    return 1;
  }

  // validate message
  // assert that message does not contain the message delimiter character
  if (strchr(message, msg_delim) != NULL) {
    snprintf(log_buffer, LOG_BUFFER_SIZE,
             "Message is not allowed to contain message delimiter "
             "character '%c'",
             msg_delim);
    fprintln_and_log(stderr, log_buffer);
    return 1;
  }

  // forward message to subscribers of wildcard topic
  found_topic = &topic_subs_map[INDEX_WILDCARD_TOPIC];
  for (i = 0; i < SUB_ADDRESSES_LENGTH; i++) {
    if (found_topic->sub_addresses[i].sin_addr.s_addr != empty_address) {
      send_message(message, found_topic->sub_addresses[i], sock_fd);
    }
  }

  // try to find list of subscribers for current topic
  found_topic = find_topic_sub(topic);
  if (found_topic == NULL) {
    snprintf(log_buffer, LOG_BUFFER_SIZE,
             "Topic '%s' has no subscribers, discarding message", topic);
    fprintln_and_log(stderr, log_buffer);
    return 0;
  }

  // forward message to subscribers of current topic
  for (i = 0; i < SUB_ADDRESSES_LENGTH; i++) {
    if (found_topic->sub_addresses[i].sin_addr.s_addr != empty_address) {
      send_message(message, found_topic->sub_addresses[i], sock_fd);
    }
  }

  return 0;
}

/**
 * Handles a subscribe request
 *
 * Registers subscriber address data as recipient for the specified topic
 *
 * Returns 0 if topic subscription could be stored without issues, otherwise
 * returns 1 on errors
 */
int handle_subscribe(char *request, const struct sockaddr_in *sub_address) {
  char *topic;
  topic_subs *topic_struct;
  int i;

  // isolate topic from subscriber message
  // first jump over method, then get the remaining substring after the first
  // delimiter
  strtok(request, "!");
  topic = strtok(NULL, "");

  // validate topic
  if (validate_topic(topic, true) != 0) {
    return 1;
  }

  // get instance that stores subscribers for requested topic
  topic_struct = find_or_insert_topic_sub(topic);
  if (topic_struct == NULL) {
    return 1;
  }

  // check via IP address and port if subscriber is already subscribed to
  // requested topic
  for (i = 0; i < SUB_ADDRESSES_LENGTH; i++) {
    if (is_same_address(&topic_struct->sub_addresses[i], sub_address)) {
      snprintf(log_buffer, LOG_BUFFER_SIZE,
               "Host %s:%d is already subscribed to topic '%s'",
               inet_ntoa(sub_address->sin_addr), ntohs(sub_address->sin_port),
               topic);
      fprintln_and_log(stderr, log_buffer);
      return 0;
    }
  }

  // attempt to add new subscriber to list for requested topic,
  // do so by finding an unused address entry
  for (i = 0; i < SUB_ADDRESSES_LENGTH; i++) {
    if (topic_struct->sub_addresses[i].sin_addr.s_addr == empty_address) {
      // copy address data of subscribing client to unused entry in map
      (*topic_struct).sub_addresses[i] = *sub_address;
      snprintf(log_buffer, LOG_BUFFER_SIZE,
               "Host %s:%d is now subscribed to topic '%s'",
               inet_ntoa(sub_address->sin_addr), ntohs(sub_address->sin_port),
               topic);
      fprintln_and_log(stderr, log_buffer);
      return 0;
    }
  }

  snprintf(log_buffer, LOG_BUFFER_SIZE,
           "No more free slots to subscribe host %s:%d to topic '%s'",
           inet_ntoa(sub_address->sin_addr), ntohs(sub_address->sin_port),
           topic);
  fprintln_and_log(stderr, log_buffer);
  return 1;
}

/**
 * Handles an unsubscribe request
 *
 * Searches for the subscriber in the list and removes its entry if found.
 *
 * Returns 0 if subscriber could be unsubscribed from topic without issues,
 * otherwise returns 1 on errors
 */
int handle_unsubscribe(char *request, const struct sockaddr_in *sub_address) {
  char *topic;
  topic_subs *topic_struct;
  int i;

  // isolate topic from subscriber message
  // first jump over method, then get the remaining substring after the first
  // delimiter
  strtok(request, "!");
  topic = strtok(NULL, "");

  // validate topic
  if (validate_topic(topic, true) != 0) {
    return 1;
  }

  // get instance that stores subscribers for requested topic
  topic_struct = find_topic_sub(topic);
  if (topic_struct == NULL) {
    snprintf(
        log_buffer, LOG_BUFFER_SIZE,
        "Topic '%s' not found, nothing to unsubscribe host %s:%d to topic from",
        topic, inet_ntoa(sub_address->sin_addr), ntohs(sub_address->sin_port));
    fprintln_and_log(stderr, log_buffer);
    return 0;
  }

  // search subscriber via IP address and port
  for (i = 0; i < SUB_ADDRESSES_LENGTH; i++) {
    if (is_same_address(&topic_struct->sub_addresses[i], sub_address)) {
      // matching address found, unregister it by resetting data of entry
      set_addr_empty(&topic_struct->sub_addresses[i]);
      snprintf(log_buffer, LOG_BUFFER_SIZE,
               "Host %s:%d has been unsubscribed from topic '%s'",
               inet_ntoa(sub_address->sin_addr), ntohs(sub_address->sin_port),
               topic);
      fprintln_and_log(stderr, log_buffer);

      // in addition, check if the topic now has no subscribers, in which case
      // it can be removed to make space for other topics
      remove_unused_topic(topic_struct);

      return 0;
    }
  }

  snprintf(log_buffer, LOG_BUFFER_SIZE,
           "Host %s:%d was not subscribed to topic '%s', nothing to do",
           inet_ntoa(sub_address->sin_addr), ntohs(sub_address->sin_port),
           topic);
  fprintln_and_log(stderr, log_buffer);
  return 0;
}

int main() {
  struct sockaddr_in *current_sub_addr;
  int sock_fd;
  struct sockaddr_in broker_addr, client_addr;
  socklen_t broker_size, client_size;
  char buffer[512];
  int nbytes, length, i, j;
  char *method;

  // initialize topic subs list to be recognizably empty
  for (i = 0; i < TOPIC_SUBS_MAP_LENGTH; i++) {
    strcpy(topic_subs_map[i].topic, empty_topic);
    for (j = 0; j < SUB_ADDRESSES_LENGTH; j++) {
      current_sub_addr = &topic_subs_map[i].sub_addresses[j];
      set_addr_empty(current_sub_addr);
    }
  }

  // already configure wildcard topic to ensure that it is always available
  strcpy(topic_subs_map[INDEX_WILDCARD_TOPIC].topic, "#");

  // create UPD socket
  sock_fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (sock_fd < 0) {
    perror("socket");
    return 1;
  }

  // configure address structure for broker
  broker_size = sizeof(broker_addr);
  memset((void *)&broker_addr, 0, sizeof(broker_addr));
  broker_addr.sin_family = AF_INET;
  broker_addr.sin_addr.s_addr = INADDR_ANY;
  broker_addr.sin_port = htons(broker_port);

  // bind address structure to socket
  bind(sock_fd, (struct sockaddr *)&broker_addr, broker_size);

  // open log file in append mode
  log_file = fopen(log_file_name, "a");
  if (log_file == NULL) {
    fprintf(stderr, "Could not open log file, proceeding anyway\n");
  }

  snprintf(log_buffer, LOG_BUFFER_SIZE, "Broker listening on port %u",
           broker_port);
  fprintln_and_log(stderr, log_buffer);

  // receive and forward messages in infinite loop
  while (1) {
    // receive message
    client_size = sizeof(client_addr);
    nbytes = recvfrom(sock_fd, buffer, sizeof(buffer) - 1, 0,
                      (struct sockaddr *)&client_addr, &client_size);
    if (nbytes < 0) {
      sprintf(log_buffer, "Failed to receive request");
      fprintln_and_log(stderr, log_buffer);
      continue;
    }
    buffer[nbytes] = '\0';

    snprintf(log_buffer, LOG_BUFFER_SIZE,
             "Received request '%s' from host %s:%d", buffer,
             inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
    fprintln_and_log(stderr, log_buffer);

    // identify method and proceed to appropriate logic
    if (strncmp(buffer, method_publish, strlen(method_publish)) == 0) {
      handle_publish(buffer, sock_fd);
    } else if (strncmp(buffer, method_subscribe, strlen(method_subscribe)) ==
               0) {
      handle_subscribe(buffer, &client_addr);
    } else if (strncmp(buffer, method_unsubscribe, strlen(method_subscribe)) ==
               0) {
      handle_unsubscribe(buffer, &client_addr);
    } else {
      snprintf(log_buffer, LOG_BUFFER_SIZE, "Request contains invalid method");
      fprintln_and_log(stderr, log_buffer);
      continue;
    }
  }

  return 0;
}