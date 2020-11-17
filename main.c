#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <sys/types.h> 
#include <sys/stat.h>
#include <time.h>
#include <pthread.h>
#include <atomic> 

#define UDP_BUFFER_SIZE 65507
#define RECV_BUFFER_SIZE (5*1024*1024)
#define WRITE_BUFFER_TO_DISK_SIZE (0.8*RECV_BUFFER_SIZE)

#define NAME_WIDTH 64
#define TIME_WIDTH 13

#define HEADER_LENGTH (NAME_WIDTH+TIME_WIDTH)

#define RECV_BUFFER_COUNT 2

std::atomic<int> writeFlag(0);

char *g_bufferToWrite = NULL;
int g_bufferToWriteLength = 0;
char g_fileNameToSave[64] = {0};
char *g_recvBuffer[RECV_BUFFER_COUNT] = {0};

int writeFile(char *name, char *buffer, uint32_t bufferLength);
void* writeFileThread(void*)
{
    while (writeFlag != 2)
    {
        if (writeFlag == 1)
        {
            if (g_bufferToWrite)
            {
                writeFlag = 0;
                writeFile(g_fileNameToSave, g_bufferToWrite, g_bufferToWriteLength);
                printf("write data in thread\n");
            }
        }
    }
}

char *getCurrentTime(char timestamp[], int len)
{
     time_t now = time(0);
     struct tm ttm;
     struct tm *tmptr = localtime_r(&now, &ttm);
     snprintf(timestamp, len, "%04d-%02d-%02d", ttm.tm_year + 1900,
		 ttm.tm_mon + 1, ttm.tm_mday); 
     //snprintf(timestamp, len, "%04d-%02d-%02dT%02d:%02d:%02d", ttm.tm_year + 1900, ttm.tm_mon + 1, ttm.tm_mday, ttm.tm_hour, ttm.tm_min, ttm.tm_sec); 
     return timestamp;
}

int createDir(char *name)
{
    if (access(name, W_OK) != 0)  
    {  
        if (mkdir((const char*)name, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1)  
        {   
            perror("mkdir error");   
            return -1;   
        }
    }
    return 0;
}

int writeFile(char *name, char *buffer, uint32_t bufferLength)
{
    char fileName[512] = {0};
    strcpy(fileName, name);

    if (0 != createDir(fileName))
    {
        return -1;
    }

    char currentTime[64] = {0};
    getCurrentTime(currentTime, sizeof(currentTime));
    strcat(fileName, "/");
    strcat(fileName, currentTime);

    printf("write file:%s,len:%d\n", fileName, bufferLength);
    FILE *file = fopen(fileName, "ab");
    if (file)
    {
        fwrite(buffer, 1, bufferLength, file);
        fclose(file);
    }
    return 0;
}

int parsePackage(const char *pack, int packLen, char name[], char time[], char **gzip, int *gzipLen)
{
    if (packLen <= HEADER_LENGTH)
    {
       return 0;
    }

    memcpy(name, pack, NAME_WIDTH);
    char *buf = (char*)name;
    for (int i=0; i<NAME_WIDTH; i++)
    {
        if (*buf == ' ')
        {
           *buf = '\0';
           break;
        }
        buf++;
    }
    
    memcpy(time, pack+NAME_WIDTH, TIME_WIDTH);
    buf = (char*)time;
    for (int i=0; i<TIME_WIDTH; i++)
    {
        if (*buf == ' ')
        {
           *buf = '\0';
           break;
        }
        buf++;
    }
    
    *gzip = (char*)pack+HEADER_LENGTH;
    *gzipLen = packLen-HEADER_LENGTH;
    return *gzipLen;
}
 
int main(int argc, char** argv)
{
    if (argc != 2)
    {
        perror("Usage: asc <udp port>\n Example:\n   UdpFileWriter 8899\n");
        return EXIT_FAILURE;
    }

    const char *port = argv[1];
 
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(atoi(port));
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
 
    int sock;
    if ((sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
    {
        perror("socket");
        return EXIT_FAILURE;
    }

    //port bind to server
    if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror("bind");
        return EXIT_FAILURE;
    }

    pthread_t tid;
    if (pthread_create(&tid, NULL, writeFileThread, NULL) != 0) {
        printf("pthread_create error.");
        exit(EXIT_FAILURE);
    }

    printf("Welcome! This is a UDP server, I can only received message from client and write to file\n");

    char *buff = (char*)malloc(UDP_BUFFER_SIZE);
    struct sockaddr_in clientAddr;
    memset(&clientAddr,0,sizeof(clientAddr));
    size_t len = 0;
    socklen_t socklen = sizeof(clientAddr);
    int gzipLen = 0;
    char name[NAME_WIDTH+1] = {0};
    char time[TIME_WIDTH+1] = {0};
    char *gzip;

    for (int i=0; i<RECV_BUFFER_COUNT; i++)
    {
        g_recvBuffer[i] = (char*)malloc(RECV_BUFFER_SIZE);
    }

    int recvBufferIndex = 0;
    int recvBufferLength = 0;

int testIndex = 0;
    while (1)
    {
        len = recvfrom(sock, buff, UDP_BUFFER_SIZE, 0, (struct sockaddr*)&clientAddr, &socklen);
        if (len > 0)
        {
            if (len > HEADER_LENGTH)
            {
                memset(name, 0, sizeof(name));
                memset(time, 0, sizeof(time));
                parsePackage(buff, len, name, time, &gzip, &gzipLen);
                if (gzipLen>0)
                {
                    uint32_t len1 = (uint32_t)(gzipLen+TIME_WIDTH);
					char *ptr = g_recvBuffer[recvBufferIndex];
                    memcpy(ptr + recvBufferLength, (void*)(&len1), sizeof(uint32_t));
                    recvBufferLength += sizeof(uint32_t);
                    memcpy(ptr + recvBufferLength, time, TIME_WIDTH);
                    recvBufferLength += TIME_WIDTH;
                    memcpy(ptr + recvBufferLength, gzip, gzipLen);
					recvBufferLength += gzipLen;
                    g_bufferToWriteLength = recvBufferLength;

                    if (strlen(g_fileNameToSave) == 0)
                    {
                        strcpy(g_fileNameToSave, name);
                    }
                    if (recvBufferLength >= WRITE_BUFFER_TO_DISK_SIZE
                            || strcmp(g_fileNameToSave, name) != 0)
                    {
                        g_bufferToWrite = g_recvBuffer[recvBufferIndex];
                        strcpy(g_fileNameToSave, name);
                        recvBufferIndex++;
                        if (recvBufferIndex == RECV_BUFFER_COUNT)
                        {
                            recvBufferIndex = 0;
                        }
                        recvBufferLength = 0;
                        writeFlag = 1;
                    }
                   //printf("name:%s,time:%s,gliblen:%d\n", name, time, gzipLen);
                   //writeFile(name, time, gzip, gzipLen);
testIndex++;
printf("count:%d\n", testIndex);

                }
                else
                {
                    printf("error length:%d\n", (int)len);
                }
            }
            else
            {
		printf("error 111 length:%d\n", (int)len);
            }
            //buff[len] = 0;
            //printf("%s %u says: %s\n", inet_ntoa(clientAddr.sin_addr), ntohs(clientAddr.sin_port), buff);
        }
        else
        {
            perror("recv");
            break;
        }
    }
    writeFlag = 2;
    char* rev = NULL;
    pthread_join(tid, (void **)&rev);
    printf("%s return.\n", rev);
    free(buff);
    buff = 0;
    for (int i=0; i<RECV_BUFFER_COUNT; i++)
    {
        free(g_recvBuffer[i]);
        g_recvBuffer[i] = 0;
    }
    return 0;
}
