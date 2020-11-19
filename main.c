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
#include <thread>

#define UDP_BUFFER_SIZE 65507
#define RECV_BUFFER_SIZE (5*1024*1024)
#define WRITE_BUFFER_TO_DISK_SIZE ((uint32_t)(0.8*RECV_BUFFER_SIZE))
#define NAME_WIDTH 64
#define RECV_BUFFER_COUNT 2

int writeFile(char *name, char *buffer, uint32_t bufferLength);
char *getCurrentTime(char timestamp[], int len);
int createDir(char *name);
int writeFile(char *name, char *buffer, uint32_t bufferLength);
void flushBufferWriters();

class BufferWriter;
BufferWriter *findBufferWriter(const char *name);

std::map<std::string, BufferWriter *> g_bufferWriters;

class BufferWriter 
{
public:
    BufferWriter(const char *name) 
    {
        m_count = 0;
        m_isBufferFull = 0;
        m_recvBufferPos = 0;
        m_bufferToWrite = NULL;
        memset(m_name, 0, sizeof(m_name));
        strcpy(m_name, name);
        for (int i=0; i<RECV_BUFFER_COUNT; i++)
        {
            m_recvBuffer[i] = new char[RECV_BUFFER_SIZE];
        }
        
        m_threadHandle = new std::thread(&BufferWriter::loopFlush, this);
    }
    
    ~BufferWriter()
    {   
        stop();
        for (int i=0; i<RECV_BUFFER_COUNT; i++)
        {
            delete m_recvBuffer[i];
            m_recvBuffer[i] = NULL;
        }
    }
    
    
    int buffer(const char *data, uint32_t dataLength)
    {
        char *ptr = m_recvBuffer[m_recvBufferIndex];
        memcpy(ptr + m_recvBufferPos, data, dataLength);
        m_recvBufferPos += dataLength;
        memcpy(ptr + m_recvBufferPos, "\n", 1);
        m_recvBufferPos += 1;
        m_bufferToWriteLength = m_recvBufferPos;
        
        m_count++;
        if (m_recvBufferPos >= WRITE_BUFFER_TO_DISK_SIZE)
        {
            std::lock_guard<std::mutex> lk(m_mutex);
            m_bufferToWrite = m_recvBuffer[m_recvBufferIndex];
            m_recvBufferIndex++;
            if (m_recvBufferIndex == RECV_BUFFER_COUNT)
            {
                m_recvBufferIndex = 0;
            }
            m_recvBufferPos = 0;
            m_isBufferFull = 1;
            m_dataCondition.notify_one();
        }
        return 0;
    }
    
    void loopFlush()
    {
        while (m_stop == 0)
        {
            std::unique_lock<std::mutex> lk(m_mutex);
            m_dataCondition.wait(lk, [&]{return m_isBufferFull == 1;});
            flush();
            lk.unlock();
        }
    }
    
    int flush()
    {
        if (m_bufferToWrite && m_bufferToWriteLength>0)
        {
            writeFile(m_name, m_bufferToWrite, m_bufferToWriteLength);
            printf("write data in thread (%s,%d,%d)\n", m_name, m_bufferToWriteLength, m_count);
            m_bufferToWrite = NULL;
            m_bufferToWriteLength = 0;
            m_count = 0;
            m_isBufferFull = 0;
            return 0;
        }
        return -1;
    }
    
    void stop()
    {
        m_isBufferFull = 1;
        std::lock_guard<std::mutex> lk(m_mutex);
        m_dataCondition.notify_one();
        if (m_threadHandle)
        {
            m_threadHandle->join();
        }
        delete m_threadHandle;
        m_threadHandle = NULL;
    }
    
private:
    char *m_recvBuffer[RECV_BUFFER_COUNT] = {0};
    char *m_bufferToWrite = NULL;
    char m_name[NAME_WIDTH] = {0};
    uint32_t m_bufferToWriteLength = 0;
    uint32_t m_recvBufferIndex = 0;
    uint32_t m_recvBufferPos = 0;
    uint32_t m_count = 0;
    
    int m_stop = 0;
    
    std::thread *m_threadHandle = NULL;
    std::mutex m_mutex;
    int m_isBufferFull = 0;
    std::condition_variable m_dataCondition;
};

BufferWriter *findBufferWriter(const char *name)
{
    BufferWriter *bufferWriter = NULL;
    std::map<std::string, BufferWriter*>::iterator it = g_bufferWriters.find(name);
    if (it == g_bufferWriters.end())
    {
        bufferWriter = new BufferWriter(name);
        g_bufferWriters[name] = bufferWriter;
    }
    else
    {
        bufferWriter = it->second;
    }
    return bufferWriter;			
}

void destroyBufferWriters()
{
    std::map<std::string, BufferWriter*>::iterator it = g_bufferWriters.begin();
    while (it != g_bufferWriters.end())
    {
        BufferWriter *pBufferWriter = it->second;
        std::string name = it->first;
        if (pBufferWriter)
        {
            delete pBufferWriter;
            pBufferWriter = NULL;
            printf("%s destroyed\n", name.c_str());
        }
        it++;
    }
    g_bufferWriters.clear();
}

void flushBufferWriters()
{
    std::map<std::string, BufferWriter*>::iterator it = g_bufferWriters.begin();
    while (it != g_bufferWriters.end())
    {
        BufferWriter *pBufferWriter = it->second;
        std::string name = it->first;
        if (pBufferWriter)
        {
            pBufferWriter->flush();
        }
        it++;
    }
}

char *getCurrentTime(char timestamp[], int len)
{
    time_t now = time(0);
    struct tm ttm;
    localtime_r(&now, &ttm);
    snprintf(timestamp, len, "%04d%02d%02d", ttm.tm_year + 1900,
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
    char fileName[512] = {"data/"};
    if (0 != createDir(fileName))
    {
        printf("error:create dir %s failed", fileName);
        return -1;
    }
    
    strcat(fileName, name);
    char currentTime[64] = {0};
    getCurrentTime(currentTime, sizeof(currentTime));
    strcat(fileName, "-");
    strcat(fileName, currentTime);
    strcat(fileName, ".csv");
    
    printf("write file:%s,len:%d\n", fileName, bufferLength);
    FILE *file = fopen(fileName, "ab");
    if (file)
    {
        fwrite(buffer, 1, bufferLength, file);
        fclose(file);
    }
    return 0;
}

int parsePackage(const char *pack, int packLen, char name[], int nameLen, char **dataPos)
{
    char *ptr = (char*)pack;
    for (int i=0; i<packLen && i<nameLen; i++,ptr++)
    {
        if (*ptr == ',')
        {
            name[i] = '\0';
            *dataPos = ptr+1;
            return 0;
        }
        name[i] = *ptr;
    }
    return -1;
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
    
    printf("Welcome! This is a UDP server, I can only received message from client and write to file\n");
    
    char *buff = new char[UDP_BUFFER_SIZE];
    struct sockaddr_in clientAddr;
    memset(&clientAddr,0,sizeof(clientAddr));
    size_t len = 0;
    socklen_t socklen = sizeof(clientAddr);
    
    char name[NAME_WIDTH] = {0};
    while (1)
    {
        memset(buff, 0, UDP_BUFFER_SIZE);
        len = recvfrom(sock, buff, UDP_BUFFER_SIZE, 0, (struct sockaddr*)&clientAddr, &socklen);
        if (len > 0)
        {
            memset(name, 0, NAME_WIDTH);
            char *dataPos = 0;
            int ret = parsePackage(buff, len, name, NAME_WIDTH, &dataPos);
            if (ret == 0)
            {
                BufferWriter *bufferWriter = findBufferWriter(name);
                bufferWriter->buffer(dataPos, strlen(dataPos));
            }
            else
            {
                printf("error 111 length:%d(%s)\n", (int)len, buff);
            }
        }
        else
        {
            perror("recv");
            break;
        }
    }
    delete buff;
    buff = NULL;
    destroyBufferWriters();
    return 0;
}
