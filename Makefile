# Compiler and flags
CC = gcc
CFLAGS = -Wall -g

# Directories
OBJ_DIR = obj

# Files
SRC_CLIENT = client.c
SRC_SERVER = proxy.c
OBJ_CLIENT = $(OBJ_DIR)/dfc.o
OBJ_SERVER = $(OBJ_DIR)/dfs.o
EXEC_CLIENT = ./dfc
EXEC_SERVER = ./dfs

# Targets
all: $(EXEC_CLIENT) $(EXEC_SERVER)

$(EXEC_CLIENT): $(OBJ_CLIENT)
	$(CC) $(OBJ_CLIENT) -o $(EXEC_CLIENT) -lcrypto

$(EXEC_SERVER): $(OBJ_SERVER)
	$(CC) $(OBJ_SERVER) -o $(EXEC_SERVER) -lcrypto

$(OBJ_DIR)/%.o: %.c
	@mkdir -p $(OBJ_DIR)
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(EXEC_CLIENT) $(EXEC_SERVER) $(OBJ_DIR)/*

.PHONY: all clean
