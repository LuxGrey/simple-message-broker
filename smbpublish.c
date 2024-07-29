/**
 * Requirements:
 * - call pattern: smbpublish broker topic message
 * - broker is hostname or IP-address of broker
 * - cannot use wildcards
 * - terminates after sending message to broker
 */

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "smbconstants.h"

int main(int argc, char **argv) {
  char *broker, *topic, *message;
  struct hostent *broker_hent;
  int sock_fd;
  struct sockaddr_in broker_addr, sender_addr;
  socklen_t broker_size, sender_size;
  char buffer[512];
  int nbytes, length;

  // assert expected number of program call arguments
  if (argc != 4) {
    fprintf(
        stderr,
        "Invalid call pattern. Expected pattern is:\n%s broker topic message\n",
        argv[0]);
    return 1;
  }

  broker = argv[1];
  topic = argv[2];
  message = argv[3];

  // assert that topic does not contain the wildcard character
  if (strchr(topic, topic_wildcard) != NULL) {
    fprintf(stderr, "Topic is not allowed to contain wildcard character %s\n",
            topic_wildcard);
    return 1;
  }

  // assert that topic and message do not contain the message delimiter
  // character
  if (strchr(topic, msg_delim) != NULL) {
    fprintf(stderr,
            "Topic is not allowed to contain message delimiter character %s\n",
            msg_delim);
    return 1;
  }
  if (strchr(message, msg_delim) != NULL) {
    fprintf(
        stderr,
        "Message is not allowed to contain message delimiter character %s\n",
        msg_delim);
    return 1;
  }

  // determine address of broker
  if ((broker_hent = gethostbyname(broker)) == NULL) {
    perror("gethostbyname");
    return 1;
  }

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
  memcpy((void *)&broker_addr.sin_addr.s_addr, (void *)broker_hent->h_addr,
         broker_hent->h_length);
  broker_addr.sin_port = htons(broker_port);

  // assemble message for broker
  sprintf(buffer, "%s%s%s%s%s", method_publish, msg_delim, topic, msg_delim,
          message);

  // publish message to broker
  fprintf(stderr, "Publishing message: %s\n", buffer);
  length = strlen(buffer);
  nbytes = sendto(sock_fd, buffer, length, 0, (struct sockaddr *)&broker_addr,
                  broker_size);
  if (nbytes != length) {
    perror("sendto");
    return 1;
  }

  // close socket and terminate
  close(sock_fd);
  return 0;
}