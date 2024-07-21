/**
 * smbbroker.c
 *
 * A broker program that is compatible with the publisher program smbpublisher
 * and the subscriber program smbpublisher.
 *
 * This program does not require any arguments.
 *
 * It will run in an infinite loop, accepting message publishes from any client,
 * which will then be immediately forwarded to any subscribers that are
 * currently subscribed to the topic of that message.
 *
 * Allows for subscribers to subscribe to the '#' topic, which will result in
 * the broker forwarding messages of any topic to such subscribers.
 */

#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "smbconstants.h"

#define TOPIC_LENGTH 20
#define SUB_ADDRESSES_LENGTH 10
#define TOPIC_SUBS_MAP_LENGTH 10
#define INDEX_WILDCARD_TOPIC 0

typedef struct topic_subs_struct {
  char topic[TOPIC_LENGTH];
  struct sockaddr_in sub_addresses[SUB_ADDRESSES_LENGTH];
} topic_subs;

/**
 * A map where each entry maps a single topic to multiple subscriber addresses
 */
topic_subs topic_subs_map[TOPIC_SUBS_MAP_LENGTH];

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
 * Attempts to find the provided topic in the topic subs list.
 *
 * If it is found, a pointer to the corresponding topic subs object is returned.
 * If it is not found, a new corresponding topic subs object will be set up in
 * the list and a pointer for that object will be returned.
 * If a new object cannot be set up because the list is full, NULL is returned.
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
    if (strncmp(topic_subs_map[i].topic, "", 1) == 0) {
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

int handle_publish(char *request, int sock_fd) {
  char *topic, *message;
  topic_subs *found_topic;
  int i;

  // isolate message components
  // first jump over method, get the topic as the next token
  // and then use the remaining substring as message contents
  strtok(request, "!");
  topic = strtok(NULL, "!");
  message = strtok(NULL, "");

  // assert that topic does not contain the message delimiter character
  if (strchr(topic, msg_delim) != NULL) {
    fprintf(stderr,
            "Topic is not allowed to contain message delimiter character %c\n",
            msg_delim);
    return 1;
  }

  // assert that topic does not contain wildcard character
  if (strchr(topic, topic_wildcard) != NULL) {
    fprintf(stderr, "Topic is not allowed to contain wildcard character %c\n",
            topic_wildcard);
    return 1;
  }

  // assert that message contents do not contain the message delimiter character
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
    if (found_topic->sub_addresses[i].sin_addr.s_addr != INADDR_NONE) {
      send_message(message, found_topic->sub_addresses[i], sock_fd);
    }
  }

  // try to find list of subscribers for current topic
  found_topic = find_topic_sub(topic);
  if (found_topic == NULL) {
    fprintf(stderr, "Topic %s has no subscribers, message will be discarded\n",
            topic);
    return 0;
  }

  // forward message to subscribers of current topic
  for (i = 0; i < SUB_ADDRESSES_LENGTH; i++) {
    if (found_topic->sub_addresses[i].sin_addr.s_addr != INADDR_NONE) {
      send_message(message, found_topic->sub_addresses[i], sock_fd);
    }
  }

  return 0;
}

int handle_subscribe(char *request, const struct sockaddr_in *sub_address) {
  char *topic;
  topic_subs *topic_struct;
  int i;

  // isolate topic from subscriber message
  // first jump over method, then get the remaining substring after the first
  // delimiter
  strtok(request, "!");
  topic = strtok(NULL, "");

  // assert that topic does not contain the message delimiter character
  if (strchr(topic, msg_delim) != NULL) {
    fprintf(stderr,
            "Topic is not allowed to contain message delimiter character %c\n",
            msg_delim);
    return 1;
  }

  // get instance that stores subscribers for requested topic
  topic_struct = find_or_insert_topic_sub(topic);
  if (topic_struct == NULL) {
    return 1;
  }

  // check via IP address if subscriber is already subscribed to requested topic
  for (i = 0; i < SUB_ADDRESSES_LENGTH; i++) {
    if (topic_struct->sub_addresses[i].sin_addr.s_addr ==
        sub_address->sin_addr.s_addr) {
      fprintf(stderr, "Subscriber is already subscribed to topic %s\n", topic);
      return 0;
    }
  }

  // attempt to add new subscriber to list for requested topic,
  // do so by finding an unused address entry
  for (i = 0; i < SUB_ADDRESSES_LENGTH; i++) {
    if (topic_struct->sub_addresses[i].sin_addr.s_addr == INADDR_NONE) {
      // copy address data of subscribing client to unused entry
      memcpy((void *)&topic_struct->sub_addresses[i], (void *)&sub_address,
             sizeof(sub_address));
      fprintf(stderr, "Subscriber registered for topic %s\n", topic);
      return 0;
    }
  }

  fprintf(stderr, "No more free slots to register subscriber for topic %s\n",
          topic);
  return 1;
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
    strcpy(topic_subs_map[i].topic, "");
    for (j = 0; j < SUB_ADDRESSES_LENGTH; j++) {
      current_sub_addr = &topic_subs_map[i].sub_addresses[j];
      memset((void *)current_sub_addr, 0, sizeof(*current_sub_addr));
      // explicitly set s_addr to an "empty" value that can be compared against
      // later
      current_sub_addr->sin_addr.s_addr = INADDR_NONE;
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
      fprintf(stderr, "Failed to receive message\n");
      continue;
    }
    buffer[nbytes] = '\0';
    printf("Received message:\n%s\n", buffer);

    // identify method and proceed to appropriate logic
    if (strncmp(buffer, method_publish, strlen(method_publish)) == 0) {
      handle_publish(buffer, sock_fd);
    } else if (strncmp(buffer, method_subscribe, strlen(method_subscribe)) ==
               0) {
      handle_subscribe(buffer, &client_addr);
    } else {
      fprintf(stderr, "Message contains invalid method\n");
      continue;
    }
  }

  return 0;
}