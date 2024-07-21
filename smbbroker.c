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

#define TOPIC_SIZE 20
#define SUB_ADDR_SIZE 10
#define TOPIC_SUBS_SIZE 10

typedef struct topic_subs_struct {
  char topic[TOPIC_SIZE];
  in_addr_t sub_addr[SUB_ADDR_SIZE];
} topic_subs;

topic_subs topic_subs_list[TOPIC_SUBS_SIZE];

/**
 * Attempts to find the provided topic in the topic subs list
 *
 * Returns a pointer to the found object or NULL if none could be found.
 */
topic_subs *find_topic_sub(const char *topic) {
  int i;

  // attempt to find corresponding topic subs objects in list
  for (i = 0; i < TOPIC_SUBS_SIZE; i++) {
    if (strncmp(topic_subs_list[i].topic, topic, sizeof(topic)) == 0) {
      // object found, return pointer
      return &topic_subs_list[i];
    }
  }

  return NULL;
}

/**
 * Sends the provided message to the provided IP address.
 *
 * Returns 0 if message was sent without issues, otherwise returns 1 on error.
 */
int send_message(int broker_fd, in_addr_t dest_ip_addr, const char *message) {
  struct sockaddr_in dest_addr;
  socklen_t dest_size;
  int length, nbytes;

  dest_size = sizeof(dest_addr);
  memset((void *)&dest_addr, 0, dest_size);
  dest_addr.sin_family = AF_INET;
  dest_addr.sin_addr.s_addr = dest_ip_addr;
  dest_addr.sin_port = htons(subscriber_port);

  length = strlen(message);
  nbytes = sendto(broker_fd, message, length, 0, (struct sockaddr *)&dest_addr,
                  dest_size);
  if (nbytes != length) {
    perror("sendto");
    return 1;
  }

  return 0;
}

int handle_publish(int broker_fd, char *message) {
  char *topic, *message_contents;
  topic_subs *found_topic;
  int i;

  // isolate message components
  // first jump over method, get the topic as the next token
  // and then use the remaining substring as message contents
  strtok(message, "!");
  topic = strtok(NULL, "!");
  message_contents = strtok(NULL, "");

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
  if (strchr(message_contents, msg_delim) != NULL) {
    fprintf(stderr,
            "Message contents are not allowed to contain message delimiter "
            "character %c\n",
            msg_delim);
    return 1;
  }

  // try to find list of subscribers for topic
  found_topic = find_topic_sub(topic);
  if (found_topic == NULL) {
    fprintf(stderr, "Topic %s has no subscribers, message will be discarded\n",
            topic);
    return 0;
  }

  // forward message to subscribers
  for (i = 0; i < TOPIC_SUBS_SIZE; i++) {
    if (found_topic->sub_addr[i] != 0) {
      send_message(broker_fd, found_topic->sub_addr[i], message_contents);
    }
  }

  return 0;
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
  topic_subs *found_topic;
  int i;

  // attempt to find corresponding topic subs objects in list
  if ((found_topic = find_topic_sub(topic)) != NULL) {
    return found_topic;
  }

  // could not find suitable object, so configure one for the new topic,
  // do this by searching for a free space
  for (i = 0; i < TOPIC_SUBS_SIZE; i++) {
    if (strncmp(topic_subs_list[i].topic, "", 1) == 0) {
      strcpy(topic_subs_list[i].topic, topic);
      return &topic_subs_list[i];
    }
  }

  // no remaining space to initialize object for new topic
  return NULL;
}

int handle_subscribe(char *message, const in_addr_t sub_address) {
  char *topic;
  topic_subs *topic_to_sub;
  int i;

  // isolate topic from subscriber message
  // first jump over method, then get the remaining substring after the first
  // delimiter
  strtok(message, "!");
  topic = strtok(NULL, "");

  // assert that topic does not contain the message delimiter character
  if (strchr(topic, msg_delim) != NULL) {
    fprintf(stderr,
            "Topic is not allowed to contain message delimiter character %c\n",
            msg_delim);
    return 1;
  }

  // find object that stores subscribers for requested topic
  topic_to_sub = find_or_insert_topic_sub(topic);
  if (topic_to_sub == NULL) {
    fprintf(stderr, "No more free slots to register new topic %s\n", topic);
    return 1;
  }

  // check if subscriber is already subscribed to requested topic
  for (i = 0; i < SUB_ADDR_SIZE; i++) {
    if (topic_to_sub->sub_addr[i] == sub_address) {
      fprintf(stderr, "Subscriber is already subscribed to topic %s\n", topic);
      return 0;
    }
  }

  // attempt to add new subscriber to list for requested topic
  for (i = 0; i < SUB_ADDR_SIZE; i++) {
    if (topic_to_sub->sub_addr[i] == 0) {
      topic_to_sub->sub_addr[i] = sub_address;
      fprintf(stderr, "Subscriber registered for topic %s\n", topic);
      return 0;
    }
  }

  fprintf(stderr, "No more fee slots to register subscriber for topic %s\n",
          topic);
  return 1;
}

int main(int argc, char **argv) {
  int broker_fd;
  struct sockaddr_in broker_addr, client_addr;
  socklen_t broker_size, client_size;
  char buffer[512];
  int nbytes, length, i, j;
  char *method;

  // initialize topic subs list to be recognizably empty
  for (i = 0; i < TOPIC_SUBS_SIZE; i++) {
    strcpy(topic_subs_list[i].topic, "");
    for (j = 0; j < SUB_ADDR_SIZE; j++) {
      topic_subs_list[i].sub_addr[j] = 0;
    }
  }
  // topic_subs_list[0].topic = "#";

  // create UPD socket
  broker_fd = socket(AF_INET, SOCK_DGRAM, 0);
  if (broker_fd < 0) {
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
  bind(broker_fd, (struct sockaddr *)&broker_addr, broker_size);

  fprintf(stderr, "Listening on port %u\n", broker_port);

  // receive and forward messages in infinite loop
  while (1) {
    // receive message
    client_size = sizeof(client_addr);
    nbytes = recvfrom(broker_fd, buffer, sizeof(buffer) - 1, 0,
                      (struct sockaddr *)&client_addr, &client_size);
    if (nbytes < 0) {
      fprintf(stderr, "Failed to receive message\n");
      continue;
    }
    buffer[nbytes] = '\0';
    printf("Received message:\n%s\n", buffer);

    // identify method and proceed to appropriate logic
    if (strncmp(buffer, method_publish, strlen(method_publish)) == 0) {
      handle_publish(broker_fd, buffer);
    } else if (strncmp(buffer, method_subscribe, strlen(method_subscribe)) ==
               0) {
      handle_subscribe(buffer, client_addr.sin_addr.s_addr);
    } else {
      fprintf(stderr, "Message contains invalid method\n");
      continue;
    }
  }

  return 0;
}