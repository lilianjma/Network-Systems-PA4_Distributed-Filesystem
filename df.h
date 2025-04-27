// Server and Client definitions
#define BUF_SIZE			64*1024
#define MAXLINE 8*1024 /*max text line length*/
#define LIST 0
#define GET 1
#define PUT 2
#define MAX_SERVERS 4

// Server definitions
#define LISTENQ 			200	 /*maximum number of client connections*/

// Client definitions
#define CONFIG_PATH "./dfc.conf" //TODO CHANGE BEFORE SUBMISSION
static const int chunk_distribution[4][4][2] = {
    { {0,1}, {1,2}, {2,3}, {3,0} },
    { {3,0}, {0,1}, {1,2}, {2,3} },
    { {2,3}, {3,0}, {0,1}, {1,2} },
    { {1,2}, {2,3}, {3,0}, {0,1} }
};