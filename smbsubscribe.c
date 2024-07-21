/**
 * smbsubscribe.c
 *
 * A subscriber program that is compatible with the broker program smbbroker.
 *
 * Broker address and a single topic to subscribe to are supplied as program
 * call arguments in the following format:
 * smbsubscribe broker topic
 * where broker is the host name or IP-address of the broker.
 *
 * After subscribing to the specified topic at the broker, the program
 * will run in an endless loop, waiting to receive messages from the broker,
 * which it will then print to stdout.
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
  char *broker, *topic;
  struct hostent *broker_hent;
  int sock_fd;
  struct sockaddr_in broker_addr, sender_addr;
  socklen_t broker_size, sender_size;
  char buffer[512];
  int nbytes, length;

  // assert expected number of program call arguments
  if (argc != 3) {
    fprintf(stderr,
            "Invalid call pattern. Expected pattern is:\n%s broker topic\n",
            argv[0]);
    return 1;
  }

  broker = argv[1];
  topic = argv[2];

  // assert that topic does not contain the message delimiter character
  if (strchr(topic, msg_delim) != NULL) {
    fprintf(stderr,
            "Topic is not allowed to contain message delimiter character %c\n",
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
  sprintf(buffer, "%s%s", method_subscribe, topic);

  // subscribe to topic at broker
  fprintf(stderr, "Subscribing to topic: %s\n", buffer);
  length = strlen(buffer);
  nbytes = sendto(sock_fd, buffer, length, 0, (struct sockaddr *)&broker_addr,
                  broker_size);
  if (nbytes != length) {
    perror("sendto");
    return 1;
  }

  // wait for messages from broker in infinite loop and print received messages
  // to stdout
  while (1) {
    nbytes = recvfrom(sock_fd, buffer, sizeof(buffer) - 1, 0,
                      (struct sockaddr *)&broker_addr, &broker_size);
    if (nbytes < 0) {
      perror("recvfrom");
      return 1;
    }
    buffer[nbytes] = '\0';
    printf("Received message:\n%s\n", buffer);
  }

  // close socket and terminate
  close(sock_fd);
  return 0;
}