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
#include <map>
#include <string>
#include <mutex> 
#include <condition_variable>
#include <pthread.h>

#define UDP_BUFFER_SIZE 65507
#define RECV_BUFFER_SIZE (5*1024*1024)
#define WRITE_BUFFER_TO_DISK_SIZE ((uint32_t)(RECV_BUFFER_SIZE-2*1024))
#define RECV_BUFFER_COUNT 2
#define SAVE_DIR ("data/")
#define NAME_WIDTH 64
#define DATA_WIDTH 128

int g_currentBufferIndex = 0;
char *g_bufferToSave = NULL;
std::mutex m_mutex;
std::condition_variable m_dataCondition;
char *g_recvBuffer[RECV_BUFFER_COUNT] = {0};

int createDir(char *name);
int writeFile(char *name, char *buffer, uint32_t bufferLength);

char *getCurrentDay(char timestamp[], int len)
{
    time_t now = time(0);
    struct tm ttm;
    localtime_r(&now, &ttm);
    snprintf(timestamp, len, "%04d%02d%02d", ttm.tm_year + 1900,
             ttm.tm_mon + 1, ttm.tm_mday); 
    //snprintf(timestamp, len, "%04d-%02d-%02dT%02d:%02d:%02d", ttm.tm_year + 1900, ttm.tm_mon + 1, ttm.tm_mday, ttm.tm_hour, ttm.tm_min, ttm.tm_sec); 
    return timestamp;
}

char *getCurrentTime(char timestamp[], int len)
{
    time_t now = time(0);
    struct tm ttm;
    localtime_r(&now, &ttm);
    snprintf(timestamp, len, "%04d-%02d-%02dT%02d:%02d:%02d", ttm.tm_year + 1900, ttm.tm_mon + 1, ttm.tm_mday, ttm.tm_hour, ttm.tm_min, ttm.tm_sec); 
    return timestamp;
}

int createDir(const char *name)
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

int findPos(const char *data, uint32_t dataLength, char c)
{
    char *ptr = (char*)data;
    for (uint32_t i=0; i<dataLength; i++)
    {
        if (*ptr == c)
        {
            return i;
        }
        else if (*ptr == '\0')
        {
            return i-1;
        }
        
        ptr++;
    }
    return -1;
}

//pasre data, pack:start of name. nameEndPtr:end of name pos. dataEndPtr:end of data
char* parsePackage(const char *pack, char **nameEndPtr, char **dataEndPtr)
{
    char *ptr = (char*)pack;
    if (strlen(ptr) == 0)
    {
        printf("end parse string\n");
        return NULL;
    }
    
    int nameEndPos = findPos(ptr, strlen(ptr), ',');
    if (nameEndPos == -1)
    {
        printf("error:can not find name(,):%s\n", ptr);
        return NULL;
    }
    
    ptr += nameEndPos;//name end
    *nameEndPtr = ptr;
    ptr += 1; //,
    
    int dataEndPos = findPos(ptr, strlen(ptr), '\n');
    if (dataEndPos == -1)
    {
        printf("error:can not find end(\n):%s\n", ptr);
        return NULL;
    }
    
    ptr += dataEndPos;
    *dataEndPtr = ptr;
    ptr += 1;
    
    return ptr;
}


std::string getFilePath(const char *name, const char *date)
{
    char fileName[512] = {SAVE_DIR};
    strcat(fileName, name);
    strcat(fileName, "-");
    strcat(fileName, date);
    strcat(fileName, ".csv");
    return fileName;
}

FILE *getFileHandle(const char *name, const char *currentTime, std::map<std::string, FILE*> *pFiles)
{
    FILE *file = NULL;
    std::map<std::string, FILE*>::iterator it = pFiles->find(name);
    if (it == pFiles->end())
    {
        std::string filePath = getFilePath(name, currentTime);
        file = fopen(filePath.c_str(), "ab");
        if (file)
        {
            (*pFiles)[name] = file;
        }
        else
        {
            printf("error to open file %s\n", filePath.c_str());
        }
    }
    else
    {
        file = it->second;
    }
    return file;
}

void closeFilesHandle(std::map<std::string, FILE*> *pFiles)
{
    printf("begine to close file\n");
    std::map<std::string, FILE*>::iterator it = pFiles->begin();
    while (it != pFiles->end())
    {
        FILE *file = it->second;
        if (file)
        {
            fclose(file);
            printf("close file %s\n", it->first.c_str());
        }
        it++;
    }
}

void flush()
{
    char *ptr = g_bufferToSave;
    char name[NAME_WIDTH] = {0};
    char data[DATA_WIDTH] = {0};
    
    char *nameEndPtr = NULL;
    char *dataEndPtr = NULL;
    
    std::map<std::string, FILE*> files;
    
    char currentDay[64] = {0};
    getCurrentDay(currentDay, sizeof(currentDay));
    
    char currentTime[64] = {0};
    getCurrentTime(currentTime, sizeof(currentTime));
    printf("\n***begine to save file at %s\n", currentTime);
    uint32_t count = 0;
    while (ptr != NULL)
    {
        char *tmp = parsePackage(ptr, &nameEndPtr, &dataEndPtr);
        if (!tmp)
        {
            break;
        }
        
        memset(name, 0, sizeof(name));
        memcpy(name, ptr, nameEndPtr-ptr);
        
        memset(data, 0, sizeof(data));
        memcpy(data, nameEndPtr+1, dataEndPtr-nameEndPtr-1);
        strcat(data, "\n");
        
        ptr = tmp;
        
        FILE *file = getFileHandle(name, currentDay, &files);
        if (!file)
        {
            printf("error to open file %s!!!!!\n", name);
            continue;
        }
        fwrite(data, strlen(data), 1, file);
        count++;
        //printf("index:%d,name:%s,data:%s", g_count, name, data);
    }
    
    closeFilesHandle(&files);
    getCurrentTime(currentTime, sizeof(currentTime));
    printf("***save count:%d, at %s\n", count, currentTime);
    memset(g_bufferToSave, 0, RECV_BUFFER_SIZE);
    g_bufferToSave = NULL;
}

void* writeFileThread(void*)
{
    while (1)
    {
        std::unique_lock<std::mutex> lk(m_mutex);
        m_dataCondition.wait(lk, [&]{return g_bufferToSave != NULL;});
        flush();
        lk.unlock();
    }
    return 0;
}

int main(int argc, char** argv)
{
    if (argc != 2)
    {
        perror("Usage: asc <udp port>\n Example:\n   UdpFileWriter 8899\n");
        return EXIT_FAILURE;
    }
    
    if (0 != createDir(SAVE_DIR))
    {
        printf("error:create dir %s failed", SAVE_DIR);
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
    if (pthread_create(&tid, NULL, writeFileThread, NULL) != 0) 
    {
        printf("pthread_create error.");
        exit(EXIT_FAILURE);
    }
    
    printf("This is a huobi UDP server, I can only received message from client and write to file\n");
    
    struct sockaddr_in clientAddr;
    memset(&clientAddr,0,sizeof(clientAddr));
    size_t len = 0;
    socklen_t socklen = sizeof(clientAddr);
    
    char *buff = (char*)(malloc(UDP_BUFFER_SIZE));
    for (int i=0; i<RECV_BUFFER_COUNT; i++)
    {
        g_recvBuffer[i] = (char*)malloc(RECV_BUFFER_SIZE);
    }
    
    uint32_t recvBufferWritePos = 0;
    uint32_t recvBufferIndex = 0;
    while (1)
    {
        len = recvfrom(sock, buff, UDP_BUFFER_SIZE, 0, (struct sockaddr*)&clientAddr, &socklen);
        if (len > 0)
        {
            char *ptr = g_recvBuffer[recvBufferIndex];
            memcpy(ptr + recvBufferWritePos, buff, len);
            recvBufferWritePos += len;
            memcpy(ptr + recvBufferWritePos, "\n\0", 2);
            recvBufferWritePos += 1;
            
            if (recvBufferWritePos >= WRITE_BUFFER_TO_DISK_SIZE) //buffer is full
            {
                std::lock_guard<std::mutex> lk(m_mutex);
                g_bufferToSave = g_recvBuffer[recvBufferIndex];
                
                recvBufferWritePos = 0;
                recvBufferIndex++;
                if (recvBufferIndex >= RECV_BUFFER_COUNT)
                {
                    recvBufferIndex = 0;
                }
                m_dataCondition.notify_one();
            }
        }
        else
        {
            perror("recv");
            break;
        }
    }
    char* rev = NULL;
    pthread_join(tid, (void **)&rev);
    printf("%s return.\n", rev);
    
    free(buff);
    buff = NULL;
    for (int i=0; i<RECV_BUFFER_COUNT; i++)
    {
        free(g_recvBuffer[i]);
        g_recvBuffer[i] = 0;
    }
    return 0;
}
