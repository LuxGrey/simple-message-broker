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

#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "smbconstants.h"

#define SUB_ADDRESSES_LENGTH 10
#define TOPIC_SUBS_MAP_LENGTH 10
#define INDEX_WILDCARD_TOPIC 0

const char *empty_topic = "";
const in_addr_t empty_address = INADDR_NONE;

typedef struct topic_subs_struct {
  char topic[TOPIC_LENGTH];
  struct sockaddr_in sub_addresses[SUB_ADDRESSES_LENGTH];
} topic_subs;

/**
 * A map where each entry maps a single topic to multiple subscriber addresses
 */
topic_subs topic_subs_map[TOPIC_SUBS_MAP_LENGTH];

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

  fprintf(stderr,
          "Last subscriber was unsubscribed from topic %s, removing topic\n",
          topic_struct->topic);
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
  fprintf(stderr, "No more free slots to register new topic %s\n", topic);
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
    perror("sendto");
    return 1;
  }

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
    fprintf(stderr, "Topic is not allowed to be an empty string\n");
    return 1;
  }

  // assert that topic is not too long to store
  if (strlen(topic) >= TOPIC_LENGTH) {
    fprintf(stderr, "Topic exceeds max length of %u\n", TOPIC_LENGTH);
    return 1;
  }

  // assert that topic does not contain the message delimiter character
  if (strchr(topic, msg_delim) != NULL) {
    fprintf(stderr,
            "Topic is not allowed to contain message delimiter character %c\n",
            msg_delim);
    return 1;
  }

  // assert that topic does not contain the wildcard character
  if (!wildcardAllowed && strchr(topic, topic_wildcard) != NULL) {
    fprintf(stderr, "Topic is not allowed to contain wildcard character %c\n",
            topic_wildcard);
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
    fprintf(stderr,
            "Message is not allowed to contain message delimiter "
            "character %c\n",
            msg_delim);
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
    fprintf(stderr, "Topic %s has no subscribers\n", topic);
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
      fprintf(stderr, "Subscriber is already subscribed to topic %s\n", topic);
      return 0;
    }
  }

  // attempt to add new subscriber to list for requested topic,
  // do so by finding an unused address entry
  for (i = 0; i < SUB_ADDRESSES_LENGTH; i++) {
    if (topic_struct->sub_addresses[i].sin_addr.s_addr == empty_address) {
      // copy address data of subscribing client to unused entry in map
      (*topic_struct).sub_addresses[i] = *sub_address;
      fprintf(stderr, "Subscriber registered for topic %s\n", topic);
      return 0;
    }
  }

  fprintf(stderr, "No more free slots to register subscriber for topic %s\n",
          topic);
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
    fprintf(stderr, "Topic %s not found, nothing to unsubscribe from\n", topic);
    return 0;
  }

  // search subscriber via IP address and port
  for (i = 0; i < SUB_ADDRESSES_LENGTH; i++) {
    if (is_same_address(&topic_struct->sub_addresses[i], sub_address)) {
      // matching address found, unregister it by resetting data of entry
      set_addr_empty(&topic_struct->sub_addresses[i]);
      fprintf(stderr, "Subscriber unregistered for topic %s\n", topic);

      // in addition, check if the topic now has no subscribers, in which case
      // it can be removed to make space for other topics
      remove_unused_topic(topic_struct);

      return 0;
    }
  }

  fprintf(stderr,
          "Subscriber was not subscribed to topic %s, nothing to unsubscribe\n",
          topic);
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

  fprintf(stderr, "Broker listening on port %u\n", broker_port);

  // receive and forward messages in infinite loop
  while (1) {
    // receive message
    client_size = sizeof(client_addr);
    nbytes = recvfrom(sock_fd, buffer, sizeof(buffer) - 1, 0,
                      (struct sockaddr *)&client_addr, &client_size);
    if (nbytes < 0) {
      fprintf(stderr, "Failed to receive request\n");
      continue;
    }
    buffer[nbytes] = '\0';
    printf("Received request:\n%s\n", buffer);

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
      fprintf(stderr, "Request contains invalid method\n");
      continue;
    }
  }

  return 0;
}