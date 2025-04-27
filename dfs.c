#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include  <signal.h>
#include <errno.h>
#include <pthread.h>
#include <sys/stat.h>
#include <dirent.h>
#include "df.h"


int serverfd = -1;
char server_dir[128]; // Directory to store files

void INThandler(int);

int command_handler(int connfd, char* buf);
int put_handler(int connfd, char* buf);
int get_handler(int connfd, char* buf);
int list_handler(int connfd);

int main(int argc, char **argv) {
    int listenfd, connfd, n;
    pid_t childpid;
    socklen_t clilen;
    int portno;
    char buf[MAXLINE];
    struct sockaddr_in cliaddr, servaddr;

    // Check command line arguments
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <directory> <port>\n", argv[0]);
        exit(1);
    }
    
    // Get server directory and port number
    strncpy(server_dir, argv[1], strlen(argv[1]));
    portno = atoi(argv[2]);
    
    // Create directory if it doesn't exist
    struct stat st = {0};
    if (stat(server_dir, &st) == -1) {
        if (mkdir(server_dir, 0700) == -1) {
            perror("Failed to create server directory");
            exit(1);
        }
    }

    signal(SIGINT, INThandler);

    // Create a socket for the server
    if ((listenfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Error in creating the socket");
        return 1;
    }
    serverfd = listenfd;

    // preparation of the socket address
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(portno);

    // bind the socket
    if (bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
        perror("Error in binding");
        return 1;
    }

    // listen to the socket by creating a connection queue, then wait for clients
    listen(listenfd, LISTENQ);

    printf("Server running on port %d using directory %s\n", portno, server_dir);
    printf("Waiting for connections...\n");

    while (1) {
        clilen = sizeof(cliaddr);
        // accept a connection
        connfd = accept(listenfd, (struct sockaddr *)&cliaddr, &clilen);
        int iskeepalive = 0;

        if ((childpid = fork()) == 0) { // if it's 0, it's child process
            printf("Child process %d created to handle client request\n", getpid());

            // close listening socket
            close(listenfd);

            while ((n = recv(connfd, buf, MAXLINE, 0)) > 0) {
                printf("String received from the client: ");
                puts(buf);
                iskeepalive = command_handler(connfd, buf); // Pass server_dir to command handler
                bzero(buf, sizeof(buf));
            }

            if (n < 0)
                printf("Read error: %s\n", strerror(errno));
            
            exit(0);
        }
        
        if (!iskeepalive) {
            close(connfd);
            printf("Closed connection\n");
        }
    }
}

void *watchdog_timer(void *arg) {
    sleep(10);  // 10 second
    printf("Child processes took too long to terminate\n");
	close(serverfd);
    printf("Server socket closed.\n");
	return NULL;
}

/**
 * INThandler
 * Sources: https://stackoverflow.com/questions/4217037/catch-ctrl-c-in-c
 * Use: 	Handle CTRL C gracefully
 */
void  INThandler(int sig)
{
    signal(sig, SIG_IGN);
	printf("\nServer exiting...\n");
	pthread_t timer_thread;

	if (serverfd != -1) {
		pthread_create(&timer_thread, NULL, watchdog_timer, NULL); // start timer
		while (wait(NULL)>0) {} 	// wait for child processes to terminate
        close(serverfd);
        printf("Server socket closed.\n");
		pthread_cancel(timer_thread);	// end timer
    }
	 exit(0);
}

/**
 * get_filename_ext
 * Sources: https://stackoverflow.com/questions/5309471/getting-file-extension-in-c
 * Use:		Get the file extension
 */
const char *get_filename_ext(const char *filename) {
    const char *dot = strrchr(filename, '.');
    if(!dot || dot == filename) return "";
    return dot + 1;
}

/********************** REQUEST HEADER FORMAT **********************
 * HTTP/1.1 200 OK \r\n
 * Content-Type: <> \r\n # Tells about the type of content and the formatting of <file contents> 
 * Content-Length:<> \r\n # Numeric value of the number of bytes of <file contents>
 * \r\n<file contents>
*******************************************************************/

/**
 * Method:	command_handler
 * Uses:	Send requested data and header
 */
int command_handler(int connfd, char* buf) {
	// Get method code
	int method_code;
    sscanf(buf, "%d ", &method_code);

    // swithch cases
    switch (method_code)
    {
    case LIST:
        list_handler(connfd);
        break;
    case GET:
        get_handler(connfd, buf);
        break;
    case PUT:
        put_handler(connfd, buf);
        break;
    default:
        printf("Invalid request method\n");
        return -1;
    }

    return 0;
}


int put_handler(int connfd, char* buf) {
    char filename[128];
    long chunk_size = 0;

    // Find end of header
    char *header_end = strstr(buf, "\r\n\r\n");
    if (!header_end) {
        fprintf(stderr, "Error: Header not found\n");
        send(connfd, "Error: Header not found", 24, 0);
        return -1;
    }
    int header_len = header_end - buf + 4;

    // 2. Parse header
    if (sscanf(buf, "%*d %127s %ld", filename, &chunk_size) != 2) {
        fprintf(stderr, "Error: Invalid PUT request format\n");
        send(connfd, "Error: Invalid PUT request format", 34, 0);
        return -1;
    }

    // Create file path
    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/%s", server_dir, filename);
    FILE* file = fopen(filepath, "wb");
    if (!file) {
        fprintf(stderr, "Error: Failed to create file %s: %s\n", filepath, strerror(errno));
        send(connfd, "Error: Failed to create file", 29, 0);
        return -1;
    }

    // 4. Write any data already received after the header
    int data_in_buffer = strlen(buf) - header_len;
    long bytes_written = 0;
    if (data_in_buffer > 0) {
        int to_write = (chunk_size < data_in_buffer) ? chunk_size : data_in_buffer;
        fwrite(buf + header_len, 1, to_write, file);
        bytes_written += to_write;
    }

    // Read rest of chunk from the socket
    char data_buf[4096];
    while (bytes_written < chunk_size) {
        int to_read = (chunk_size - bytes_written > sizeof(data_buf)) ? sizeof(data_buf) : (chunk_size - bytes_written);
        int n = recv(connfd, data_buf, to_read, 0);
        if (n <= 0) {
            fprintf(stderr, "Error: Failed to receive file data for %s\n", filename);
            fclose(file);
            send(connfd, "Error: Failed to receive file data", 35, 0);
            return -1;
        }
        fwrite(data_buf, 1, n, file);
        bytes_written += n;
    }
    fclose(file);

    printf("Saved file %s (%ld bytes)\n", filename, bytes_written);
    send(connfd, "OK", 2, 0); // Send ACK
    return 0;
}

int get_handler(int connfd, char* buf) {
    // Get filename from request
    char filename[128];
    sscanf(buf, "%*d %127s", filename);

    // Create file path
    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s/%s", server_dir, filename);

    // Open file
    FILE *fp = fopen(filepath, "rb");
    if (!fp) {
        fprintf(stderr, "get %s failed to open file.\n", filepath);
        send(connfd, "Error: Failed to open file", 29, 0);
        return -1;
    }

    // Get file size
    fseek(fp, 0, SEEK_END);
    long filesize = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    // Prepare header: "filename.chunk chunk_size\r\n\r\n"
    char header[512];
    snprintf(header, sizeof(header), "%s %ld\r\n\r\n", filename, filesize);

    // Put file into memory
    unsigned char *filedata = malloc(filesize + strlen(header));
    if (!filedata) {
        fprintf(stderr, "%s put failed.\n", filename);
        fclose(fp);
        return -1;
    }

    // Copy header to filedata
    memcpy(filedata, header, strlen(header));

    // Read file into filedata
    if (fread(filedata + strlen(header), 1, filesize, fp) != filesize) {
        fprintf(stderr, "%s put failed.\n", filename);
        free(filedata);
        fclose(fp);
        return -1;
    }
    fclose(fp);

    // Send data
    send(connfd, filedata, filesize + strlen(header), 0);

    // Wait for acknowledgment
    char ack[16];
    int ack_len = recv(connfd, ack, sizeof(ack)-1, 0);
    if (ack_len <= 0 || strncmp(ack, "OK", 2) != 0) {
        fprintf(stderr, "No ACK from server or error, client %d\n", connfd);
        free(filedata);
        return -1;
    }

    free(filedata);
    printf("%s put succeeded.\n", filename);
    return 0;
}

int list_handler(int connfd) { 
    DIR *dir;
    struct dirent *ent;
    char response[MAXLINE];
    int response_len = 0;

    // Open directory
    dir = opendir(server_dir);
    if (dir == NULL) {
        fprintf(stderr, "Error opening directory %s\n", server_dir);
        send(connfd, "Error: Could not open directory\n", 32, 0);
        return -1;
    }

    // Read directory entries
    while ((ent = readdir(dir)) != NULL) {
        // Skip . and .. entries
        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0)
            continue;

        // Add file to response
        snprintf(response + response_len, MAXLINE - response_len, "%s\n", ent->d_name);
        response_len = strlen(response);
    }
    closedir(dir);

    // Add END marker
    strcat(response, "-END-\n");

    // Send response
    if (send(connfd, response, strlen(response), 0) < 0) {
        fprintf(stderr, "Error sending list response\n");
        return -1;
    }

    return 0;
}
