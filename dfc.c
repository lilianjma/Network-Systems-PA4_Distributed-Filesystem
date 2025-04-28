#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include <arpa/inet.h>
#include <openssl/md5.h>
#include <unistd.h>
#include <stdint.h> // For uint32_t
#include <math.h>   // For ceil
#include <errno.h>  // For errno
#include <fcntl.h>  // For fcntl

#include "df.h" // Assume this defines MAX_SERVERS, MAXLINE, BUF_SIZE, LIST, GET, PUT

// --- Global Variables ---
int server_socket[MAX_SERVERS]; // Store socket FDs for connected servers
int server_count = 0;           // Number of successfully connected servers

// --- Command Handling Functions ---
int connect_to_servers();
int list_files(int argc, char **argv); // Needs implementation based on 4-server logic
int get_file(char *filename); // Needs implementation based on 4-server logic
int put_file(char *filename);

// --- Helper Functions ---
int calculate_hash_index(const char *filename);
int send_get_to_server(int serverfd, const char *chunkname, FILE *fp);
int is_file_requested(const char *filename, char **requested_files, int num_requested);
int compare_strings(const void *a, const void *b); // Helper for qsort

// --- Main Function ---
int main(int argc, char **argv)
{
    int command = -1;
    if (argc < 2)
    {
        fprintf(stderr, "Usage: %s <command> [filename] ... [filename]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    // Command parsing
    if (strncmp(argv[1], "list", 4) == 0)
        command = LIST;
    else if (strncmp(argv[1], "get", 3) == 0 && argc >= 3)
        command = GET;
    else if (strncmp(argv[1], "put", 3) == 0 && argc >= 3)
        command = PUT;
    else
    {
        fprintf(stderr, "Usage: %s <command> [filename] ... [filename]\n", argv[0]);
        fprintf(stderr, "Commands:\n");
        fprintf(stderr, "  list  <filename> [filename...]\n");
        fprintf(stderr, "  list \n");
        fprintf(stderr, "  get <filename> [filename...]\n");
        fprintf(stderr, "  put <filename> [filename...]\n");
        exit(EXIT_SUCCESS);
    }

    // Init server sockets array
    for (int i = 0; i < MAX_SERVERS; ++i) {
        server_socket[i] = -1;
    }

    // Connect to servers
    if (connect_to_servers() < 0) {
         return EXIT_FAILURE;
    }
    // printf("Successfully connected to %d servers.\n", server_count); // TODO REMOVE

    // Check if PUT and not enough servers connected
    if (command == PUT && server_count != 4) {
        fprintf(stderr, "Error: Not enough servers connected (%d) for PUT operation (requires 4).\n", server_count);
        exit(EXIT_FAILURE);
    }
    // Check if GET or LIST and no servers connected
    if ((command == GET || command == LIST) && server_count == 0) {
        fprintf(stderr, "Error: No servers connected. Cannot perform GET or LIST.\n");
        exit(EXIT_FAILURE);
    }

    // Execute command
    switch (command)
    {
    case LIST:
        // printf("LIST command\n"); // TODO REMOVE
        list_files(argc, argv);
        break;
    case GET:
        for (int i = 2; i < argc; i++)
        {
            printf("Getting file: %s\n", argv[i]);
            if (get_file(argv[i]) < 0) fprintf(stderr, "get %s failed.\n", argv[i]); // print if get fails
        }
        break;
    case PUT:
        for (int i = 2; i < argc; i++)
        {
            printf("Put file: %s\n", argv[i]);
            if (put_file(argv[i]) < 0) fprintf(stderr, "put %s failed.\n", argv[i]); // print if put fails
        }
        break;
    default:
        // Should not happen
        fprintf(stderr, "Internal error: Invalid command code %d\n", command);
        exit(EXIT_FAILURE);
    }

    // Close sockets
    // printf("Closing connections...\n"); // TODO REMOVE
    for (int i = 0; i < server_count; ++i) { 
        if (server_socket[i] >= 0) {
            close(server_socket[i]);
        }
    }

    exit(EXIT_SUCCESS);
}

/**
 * Use: Connect to servers with 1 s timeout
 * Source: https://stackoverflow.com/questions/4181784/how-to-set-socket-timeout-in-c-when-making-multiple-connections
 */
int connect_to_servers()
{
    char *home = getenv("HOME");
    if (!home) {
        fprintf(stderr, "Error: HOME environment variable not set.\n");
        return -1; // Indicate failure
    }

    char config_path[512];
    snprintf(config_path, sizeof(config_path), "%s/dfc.conf", home);

    FILE *config = fopen(config_path, "r");
    if (!config) {
        perror("Error opening config file (~/dfc.conf)");
        return -1;
    }

    char line[256];
    int current_server_index = 0;

    while(fgets(line, sizeof(line), config) && current_server_index < MAX_SERVERS) {
        char server_name[64];
        char hostname[256];
        int port;

        // Parse line
        if (sscanf(line, " server %63s %255[^:]:%d", server_name, hostname, &port) == 3) {
            // Create socket
            int sockfd = socket(AF_INET, SOCK_STREAM, 0);
            if (sockfd < 0) {
                perror("Socket creation failed");
                continue;
            }

            // Set up server address
            struct sockaddr_in serv_addr;
            memset(&serv_addr, 0, sizeof(serv_addr));
            serv_addr.sin_family = AF_INET;
            serv_addr.sin_port = htons(port);
            if (inet_pton(AF_INET, hostname, &serv_addr.sin_addr) <= 0) {
                fprintf(stderr, "Invalid address/format for server %s: %s\n", server_name, hostname);
                close(sockfd);
                continue;
            }

            // Connect to server
            if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
                fprintf(stderr, "Connection failed for %s (%s:%d): %s\n", server_name, hostname, port, strerror(errno));
                close(sockfd);
                continue;
            }

            // Set socket timeouts
            struct timeval timeout;
            timeout.tv_sec = 1; // 1 second timeout for send/recv
            timeout.tv_usec = 0;
            if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
                perror("setsockopt failed");
            }
            if (setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0) {
                perror("setsockopt failed");
            }

            // Store the connected socket
            server_socket[current_server_index] = sockfd;
            current_server_index++;
            // printf("Successfully connected to %s (%s:%d)\n", server_name, hostname, port); // TODO REMOVE

        } else if (strlen(line) > 1 && line[0] != '#') {
             fprintf(stderr, "Warning: Skipping malformed line in config: %s", line);
        }
    }

    fclose(config);
    server_count = current_server_index; // Update global count

    if (server_count == 0) {
        fprintf(stderr, "Error: No servers could be connected from config file.\n");
        return -1; // Indicate failure
    }

    return 0; // Success
}


/**
 * Use: Calculate hash index
 */
int calculate_hash_index(const char *filename) {
    unsigned char digest[MD5_DIGEST_LENGTH];
    MD5_CTX ctx;
    MD5_Init(&ctx);
    MD5_Update(&ctx, filename, strlen(filename));
    MD5_Final(digest, &ctx);

    // Combine first 4 bytes into an integer for the hash value
    uint32_t hash_val = 0;
    for (int i = 0; i < 4; i++) {
        hash_val = (hash_val << 8) | digest[i];
    }

    // Apply modulus
    return hash_val % 4;
}

int put_file(char *filename) {
   FILE *fp = fopen(filename, "rb");
    if (!fp) {
        fprintf(stderr, "%s put failed.\n", filename);
        return -1;
    }

    // Get file size
    fseek(fp, 0, SEEK_END);
    long filesize = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    // Calculate chunk sizes
    long chunk_estimate = filesize / 4;
    long remainder = filesize % 4;
    long chunk_sizes[4];
    for (int i = 0; i < 4; ++i) {
        chunk_sizes[i] = chunk_estimate;
        if (i < remainder) chunk_sizes[i]++;
    }

    // Put file into memory
    unsigned char *filedata = malloc(filesize);
    if (!filedata) {
        fprintf(stderr, "%s put failed.\n", filename);
        fclose(fp);
        return -1;
    }

    if (fread(filedata, 1, filesize, fp) != filesize) {
        fprintf(stderr, "%s put failed.\n", filename);
        free(filedata);
        fclose(fp);
        return -1;
    }
    fclose(fp);

    // Hash filename to get index
    int hash_index = calculate_hash_index(filename);

    // Send data by chunk
    long offset = 0;
    for (int chunk = 0; chunk < 4; chunk++) {
        int server1 = chunk_distribution[hash_index][chunk][0];
        int server2 = chunk_distribution[hash_index][chunk][1];

        // Prepare header: "2 filename.chunk chunk_size\r\n\r\n"
        char header[512];
        snprintf(header, sizeof(header), "%d %s.%d %ld\r\n\r\n", PUT, filename, chunk, chunk_sizes[chunk]);

        // Send to server1
        send(server_socket[server1], header, strlen(header), 0);
        send(server_socket[server1], filedata + offset, chunk_sizes[chunk], 0);

        // Wait for acknowledgment
        char ack[16];
        int ack_len = recv(server_socket[server1], ack, sizeof(ack)-1, 0);
        if (ack_len <= 0 || strncmp(ack, "OK", 2) != 0) {
            fprintf(stderr, "No ACK from server or error, server %d\n", server1);
            fclose(fp);
            free(filedata);
            return -1;
        }

        // Send to server2 
        send(server_socket[server2], header, strlen(header), 0);
        send(server_socket[server2], filedata + offset, chunk_sizes[chunk], 0);

        // Wait for acknowledgment
        ack_len = recv(server_socket[server2], ack, sizeof(ack)-1, 0);
        if (ack_len <= 0 || strncmp(ack, "OK", 2) != 0) {
            fprintf(stderr, "No ACK from server or error, server %d\n", server2);
            fclose(fp);
            free(filedata);
            return -1;
        }
        
        // Increment offset for the next chunk
        offset += chunk_sizes[chunk];
    }

    free(filedata);
    // printf("%s put succeeded.\n", filename); // TODO REMOVE
    return 0;
}

// Comparison function for qsort
int compare_strings(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}


int list_files(int argc, char **argv) {
    // Array to store all filenames.chunks
    char **all_files = malloc(MAXLINE * sizeof(char *));
    int file_count = 0;
    int max_files = MAXLINE;

    // If specific files were requested, sort them for binary search
    char **requested_files = NULL;
    int num_requested = 0;
    if (argc > 2) {
        num_requested = argc - 2;
        requested_files = malloc(num_requested * sizeof(char *));
        for (int i = 0; i < num_requested; i++) {
            requested_files[i] = argv[i + 2];
        }
        qsort(requested_files, num_requested, sizeof(char *), compare_strings);
    }

    // Send LIST request to all connected servers
    for (int i = 0; i < server_count; i++) {
        if (server_socket[i] < 0) continue;

        // Send LIST request
        char request[512];
        snprintf(request, sizeof(request), "%d\r\n\r\n", LIST);
        if (send(server_socket[i], request, strlen(request), 0) < 0) {
            fprintf(stderr, "Error sending LIST request to server %d\n", i);
            continue;
        }

        // Receive response
        char response[MAXLINE] = "";
        int response_len = 0;
        while (1) {
            int n = recv(server_socket[i], response + response_len, MAXLINE - response_len - 1, 0);
            if (n <= 0) break;
            response_len += n;
            response[response_len] = '\0';
            if (strstr(response, "-END-")) break;
        }

        // Process response
        char *line = strtok(response, "\n");
        while (line != NULL) {
            if (strcmp(line, "-END-") != 0) {
                if (file_count < max_files) {
                    all_files[file_count] = strdup(line);
                    file_count++;
                } else {
                    fprintf(stderr, "Warning: Too many files to list, ignoring extra files.\n");
                }
            }
            line = strtok(NULL, "\n");
        }
    }

    // Sort filenames
    qsort(all_files, file_count, sizeof(char *), compare_strings);

    // Process unique filenames and check for all chunks
    for (int i = 0; i < file_count; i++) {
        char current_file[256];
        int chunks_found[4] = {0, 0, 0, 0};

        // Copy filename to current_file minus the chunk number
        strcpy(current_file, all_files[i]);
        current_file[strlen(current_file)-2] = '\0';

        // while loop runs until we find the last file or the current file is checked
        while (1) {
            // find last dot to get chunk number
            char *dot = strrchr(all_files[i], '.');
            if (dot != NULL) {
                int chunk_num;
                if (sscanf(dot + 1, "%d", &chunk_num) == 1) {
                    if (chunk_num >= 0 && chunk_num < 4) {
                        chunks_found[chunk_num] = 1;
                    }
                }
            }

            // check if this is the last file
            if (i == file_count-1) {
                break;
            }
            
            // check if next file is the same as current_file
            char next_file[256]; 
            strcpy(next_file, all_files[i+1]); 
            next_file[strlen(next_file)-2] = '\0';
            if (strcmp(current_file, next_file) != 0) {
                break;
            }
            i++;
        }

        // check if current file is complete
        int complete = 1;
        for (int k = 0; k < 4; k++) {
            if (!chunks_found[k]) {
                complete = 0;
            }
        }

        // print file status if it was requested or no specific files were requested
        if (argc <= 2 || is_file_requested(current_file, requested_files, num_requested)) {
            if (complete) {
                printf("%s\n", current_file);
            } else {
                printf("%s [incomplete]\n", current_file);
            }
        }
    }

    // Cleanup
    for (int i = 0; i < file_count; i++) {
        free(all_files[i]);
    }
    free(all_files);
    if (requested_files) free(requested_files);

    return 0;
}


int get_file(char *filename) {
    int hash_index = calculate_hash_index(filename);

    // Open the file
    FILE *file = fopen(filename, "wb");
    if (!file) {
        fprintf(stderr, "Failed to create output file %s\n", filename);
        return -1;
    }

    for (int i = 0; i < 4; i++) {
        int server1 = chunk_distribution[hash_index][i][0];
        int server2 = chunk_distribution[hash_index][i][1];

        char chunkname[256];
        snprintf(chunkname, sizeof(chunkname), "%s.%d", filename, i);

        int got_chunk = 0;
        // Try both servers for this chunk
        
        if (send_get_to_server(server_socket[server1], chunkname, file) == 0) {
            got_chunk = 1;
        } else if (send_get_to_server(server_socket[server2], chunkname, file) == 0) {
            got_chunk = 1;
        }

        if (!got_chunk) {
            fprintf(stderr, "%s is incomplete without chunk %d\n", filename, i);
            fclose(file);
            remove(filename);
            return -1;
        }
    }

    fclose(file);
    // printf("GET %s terminated.\n", filename);
    return 0;
}

/**
 * Use: Send GET request to server
 * Returns: 0 on success, -1 on failure
 */
int send_get_to_server(int serverfd, const char *chunkname, FILE *file) {
    // Prepare GET request header
    long chunk_size;
    char request[512];
    snprintf(request, sizeof(request), "%d %s\r\n\r\n", GET, chunkname);

    // Send GET request
    if (send(serverfd, request, strlen(request), 0) < 0) return -1;

    char buf[MAXLINE];
    int n = recv(serverfd, buf, sizeof(buf), 0);
    if (n <= 0) {
        fprintf(stderr, "Error: Failed to receive header from server\n");
        send(serverfd, "Error: Failed to receive header", 32, 0);
        return -1;
    }

    // Find end of header
    char *header_end = strstr(buf, "\r\n\r\n");
    if (!header_end) {
        fprintf(stderr, "Error: Header not found\n");
        send(serverfd, "Error: Header not found", 24, 0);
        return -1;
    }
    int header_len = header_end - buf + 4;

    // Parse header
    if (sscanf(buf, "%*s %ld", &chunk_size) != 1) {
        fprintf(stderr, "Error: Invalid GET request format\n");
        send(serverfd, "Error: Invalid GET request format", 34, 0);
        return -1;
    }

    // Write any data already received after the header
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
        int n = recv(serverfd, data_buf, to_read, 0);
        if (n <= 0) {
            fprintf(stderr, "Error: Failed to receive file data for %s\n", chunkname);
            send(serverfd, "Error: Failed to receive file data", 35, 0);
            return -1;
        }
        fwrite(data_buf, 1, n, file);
        bytes_written += n;
    }

    // printf("Saved file %s (%ld bytes)\n", chunkname, bytes_written); // TODO REMOVE
    send(serverfd, "OK", 2, 0); // Send ACK
    return 0;
}

/**
 * Helper function to check if a file was requested, uses binary search
 */
int is_file_requested(const char *filename, char **requested_files, int num_requested) {
    int left = 0;
    int right = num_requested - 1;
    
    while (left <= right) {
        int mid = (left + right) / 2;
        int cmp = strcmp(filename, requested_files[mid]);
        if (cmp == 0) return 1;
        if (cmp < 0) right = mid - 1;
        else left = mid + 1;
    }
    return 0;
}
