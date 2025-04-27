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

// --- Function Prototypes ---
int connect_to_servers();
int list_files(); // Needs implementation based on 4-server logic
int get_file(char *filename); // Needs implementation based on 4-server logic
int put_file(char *filename);
int calculate_hash_index(const char *filename);
int get_target_server_index(int hash_index, int pair_index); // Helper for distribution table
int send_get_to_server(int serverfd, const char *chunkname, FILE *fp);

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
    if (strncmp(argv[1], "list", 4) == 0 && argc == 2)
        command = LIST;
    else if (strncmp(argv[1], "get", 3) == 0 && argc >= 3)
        command = GET;
    else if (strncmp(argv[1], "put", 3) == 0 && argc >= 3)
        command = PUT;
    else
    {
        fprintf(stderr, "Usage: %s <command> [filename] ... [filename]\n", argv[0]);
        fprintf(stderr, "Commands:\n");
        fprintf(stderr, "  list\n");
        fprintf(stderr, "  get <filename> [filename...]\n");
        fprintf(stderr, "  put <filename> [filename...]\n");
        exit(EXIT_SUCCESS);
    }

    // Initialize server sockets array
    for (int i = 0; i < MAX_SERVERS; ++i) {
        server_socket[i] = -1; // Initialize to invalid socket descriptor
    }

    // Connect to servers
    if (connect_to_servers() < 0) {
         return EXIT_FAILURE;
    }
    printf("Successfully connected to %d servers.\n", server_count);

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

    // --- Execute Command ---
    switch (command)
    {
    case LIST:
        list_files();
        break;
    case GET:
        for (int i = 2; i < argc; i++)
        {
            printf("Getting file: %s\n", argv[i]);
            if (get_file(argv[i]) < 0) {
                // Error message printed by get_file
            }
        }
        break;
    case PUT:
        for (int i = 2; i < argc; i++)
        {
            printf("Put file: %s\n", argv[i]);
            if (put_file(argv[i]) < 0) {
                // Error message printed by put_file, matches required format
                // fprintf(stderr, "%s put failed.\n", argv[i]); // Already printed by put_file on failure
            }
        }
        break;
    default:
        // Should not happen due to initial parsing
        fprintf(stderr, "Internal error: Invalid command code %d\n", command);
        exit(EXIT_FAILURE);
    }

    // --- Cleanup: Close sockets ---
    printf("Closing connections...\n");
    for (int i = 0; i < server_count; ++i) { // Iterate up to actual connected count
        if (server_socket[i] >= 0) {
            close(server_socket[i]);
        }
    }

    exit(EXIT_SUCCESS);
}

// --- connect_to_servers (With 1-second connect timeout) ---
int connect_to_servers()
{
    char *home = getenv("HOME");
    if (!home) {
        fprintf(stderr, "Error: HOME environment variable not set.\n");
        return -1; // Indicate failure
    }

    FILE *config = fopen(CONFIG_PATH, "r"); // TODO CHANGE BEFORE SUBMISSION
    if (!config) {
        perror("Error opening config file (~/dfc.conf)");
        return -1; // Indicate failure
    }

    char line[256];
    int current_server_index = 0; // Use a separate index for connected servers

    while(fgets(line, sizeof(line), config) && current_server_index < MAX_SERVERS) {
        char server_name[64];
        char hostname[256];
        int port;

        // Parse line
        if (sscanf(line, " server %63s %255[^:]:%d", server_name, hostname, &port) == 3) {
            int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
            if (sock_fd < 0) {
                perror("Socket creation failed");
                continue; // Skip this server
            }

            struct sockaddr_in serv_addr;
            memset(&serv_addr, 0, sizeof(serv_addr));
            serv_addr.sin_family = AF_INET;
            serv_addr.sin_port = htons(port);

            if (inet_pton(AF_INET, hostname, &serv_addr.sin_addr) <= 0) {
                fprintf(stderr, "Invalid address/format for server %s: %s\n", server_name, hostname);
                close(sock_fd);
                continue;
            }

            // --- Timeout for connect() using non-blocking socket and select ---
            int flags = fcntl(sock_fd, F_GETFL, 0);
            if (flags == -1 || fcntl(sock_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
                 perror("fcntl failed for non-blocking");
                 close(sock_fd);
                 continue;
            }

            int conn_ret = connect(sock_fd, (struct sockaddr *)&serv_addr, sizeof(serv_addr));
            if (conn_ret < 0 && errno != EINPROGRESS) {
                fprintf(stderr, "Connect error for %s (%s:%d): %s\n", server_name, hostname, port, strerror(errno));
                close(sock_fd);
                continue;
            }

            if (conn_ret < 0 && errno == EINPROGRESS) {
                fd_set write_fds;
                struct timeval timeout;
                timeout.tv_sec = 1; // 1 second timeout
                timeout.tv_usec = 0;

                FD_ZERO(&write_fds);
                FD_SET(sock_fd, &write_fds);

                int sel_ret = select(sock_fd + 1, NULL, &write_fds, NULL, &timeout);

                if (sel_ret > 0) {
                    int opt_val;
                    socklen_t opt_len = sizeof(opt_val);
                    if (getsockopt(sock_fd, SOL_SOCKET, SO_ERROR, &opt_val, &opt_len) < 0 || opt_val != 0) {
                        fprintf(stderr, "Connection failed for %s (%s:%d): %s\n", server_name, hostname, port, strerror(opt_val));
                        close(sock_fd);
                        continue;
                    }
                    // Connection successful!
                } else { // sel_ret == 0 (timeout) or sel_ret < 0 (error)
                    if (sel_ret == 0) fprintf(stderr, "Connection timeout for %s (%s:%d)\n", server_name, hostname, port);
                    else perror("select() failed during connect");
                    close(sock_fd);
                    continue;
                }
            }
            // If conn_ret == 0, connection was immediate.

            // Set socket back to blocking (usually simpler for subsequent operations)
            if (fcntl(sock_fd, F_SETFL, flags) == -1) {
                 perror("fcntl failed to set back to blocking");
                 // Proceed, but subsequent calls might need non-blocking handling
            }

            // --- Set RCV/SND timeouts for subsequent operations ---
            struct timeval op_timeout;
            op_timeout.tv_sec = 1; // 1 second timeout for send/recv
            op_timeout.tv_usec = 0;
            if (setsockopt(sock_fd, SOL_SOCKET, SO_RCVTIMEO, &op_timeout, sizeof(op_timeout)) < 0) {
                 perror("setsockopt(SO_RCVTIMEO) failed"); // Non-fatal
            }
            if (setsockopt(sock_fd, SOL_SOCKET, SO_SNDTIMEO, &op_timeout, sizeof(op_timeout)) < 0) {
                 perror("setsockopt(SO_SNDTIMEO) failed"); // Non-fatal
            }

            // Store the connected socket
            server_socket[current_server_index] = sock_fd;
            current_server_index++;
            printf("Successfully connected to %s (%s:%d)\n", server_name, hostname, port);

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


// --- calculate_hash_index (Calculates HASH % num_servers) ---
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

// --- get_target_server_index (Implements the distribution table) ---
// Takes hash_index (x, 0-3) and pair_index (0-3)
// Returns the index (0-3) into the server_socket array
int get_target_server_index(int hash_index, int pair_index) {

    // Generalizing: Target index = (pair_index + hash_index) % 4
    if (hash_index < 0 || hash_index > 3 || pair_index < 0 || pair_index > 3) {
        fprintf(stderr, "Internal Error: Invalid index for get_target_server_index (%d, %d)\n", hash_index, pair_index);
        return -1; // Should not happen
    }
    return (pair_index + hash_index) % 4;
}


// --- put_file (Modified for 4 Chunks/Pairs/Servers) ---
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

        // Send to server2 (only if different from server1)
        if (server2 != server1) {
            send(server_socket[server2], header, strlen(header), 0);
            send(server_socket[server2], filedata + offset, chunk_sizes[chunk], 0);

            // Wait for acknowledgment
            char ack[16];
            int ack_len = recv(server_socket[server2], ack, sizeof(ack)-1, 0);
            if (ack_len <= 0 || strncmp(ack, "OK", 2) != 0) {
                fprintf(stderr, "No ACK from server or error, server %d\n", server2);
                fclose(fp);
                free(filedata);
                return -1;
            }
        } else {
            printf("Skipping duplicate server for %s\n", filename);
        }
        
        // Increment offset for the next chunk
        offset += chunk_sizes[chunk];
    }

    free(filedata);
    printf("%s put succeeded.\n", filename);
    return 0;
}

// Comparison function for qsort
int compare_strings(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

// --- list_files (Stub - Needs Implementation) ---
int list_files() {
    // Array to store all filenames.chunks
    char **all_files = malloc(MAXLINE * sizeof(char *));
    int file_count = 0;
    int max_files = MAXLINE;

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
            if (strstr(response, "END\r\n\r\n")) break;
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

        // print file status
        if (complete) {
            printf("%s\n", current_file);
        } else {
            printf("%s [incomplete]\n", current_file);
        }
    }

    // Cleanup
    for (int i = 0; i < file_count; i++) {
        free(all_files[i]);
    }
    free(all_files);

    return 0;
}

// --- get_file (Stub - Needs Implementation) ---
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
    printf("GET %s terminated.\n", filename);
    return 0;
}

// Returns 0 on success, -1 on failure
int send_get_to_server(int serverfd, const char *chunkname, FILE *file) {
    // Prepare GET request header
    long chunk_size;
    char request[512];
    snprintf(request, sizeof(request), "%d %s\r\n\r\n", GET, chunkname);
    printf("Sending request: %s", request);

    // Send GET request
    if (send(serverfd, request, strlen(request), 0) < 0) return -1;

    // Receive header in chunks until we find the end
    char buf[MAXLINE] = {0};
    int total_received = 0;
    int found_end = 0;

    while (total_received < sizeof(buf) - 1 && !found_end) {
        int n = recv(serverfd, buf + total_received, sizeof(buf) - total_received - 1, 0);
        if (n <= 0) {
            fprintf(stderr, "Error: Failed to receive header from server\n");
            return -1;
        }
        total_received += n;
        buf[total_received] = '\0';
        printf("Received %d bytes, total %d: %s\n", n, total_received, buf);
        
        // Check if we've received the complete header
        if (strstr(buf, "\r\n\r\n")) {
            found_end = 1;
            printf("Found end of header\n");
        }
    }

    if (!found_end) {
        fprintf(stderr, "Error: Incomplete header received\n");
        return -1;
    }

    // Find end of header
    char *header_end = strstr(buf, "\r\n\r\n");
    if (!header_end) {
        fprintf(stderr, "Error: Header not found in: %s\n", buf);
        return -1;
    }
    int header_len = header_end - buf + 4;
    printf("Header length: %d\n", header_len);

    // Parse header
    char resp_chunkname[256];
    if (sscanf(buf, "%255s %ld", resp_chunkname, &chunk_size) != 2) {
        fprintf(stderr, "Error: Invalid header format: %s\n", buf);
        return -1;
    }
    printf("Parsed header: chunkname=%s, size=%ld\n", resp_chunkname, chunk_size);

    // Write any data already received after the header
    int data_in_buffer = total_received - header_len;
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
            return -1;
        }
        fwrite(data_buf, 1, n, file);
        bytes_written += n;
    }

    // Send ACK
    if (send(serverfd, "OK", 2, 0) < 0) return -1;

    return 0;
}
