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
#include <sstream>
#include <pthread.h>
#include <signal.h>

#define BUFFER_SIZE      10    // in Megabytes
#define RECV_BUFFER_SIZE (BUFFER_SIZE * 1024 * 1024)
#define MARGIN_LEN       1024
#define SAVE_DIR         ("data/")
#define TIME_STR_LEN     64
#define TIMESTAMP_WIDTH  13

using FILEMAP = std::map<std::string, FILE*>;

int g_currentBufferIndex = 0;
char *g_saveBuffer = nullptr;
char *g_recvBuffer = nullptr;

std::mutex m_mutex;
std::condition_variable m_dataCondition;
int g_stop = 0;
int g_write = 0;

int g_sock = 0;

int createDir(char *name);
int writeFile(char *name, char *recvbufer, uint32_t recvbuferLength);

char *getDate(char timestamp[], char* ts, int len)
{
    time_t t = (std::stoll(ts) / 1000);
    struct tm ttm;
    localtime_r(&t, &ttm);
    snprintf(timestamp, len, "%04d%02d%02d", ttm.tm_year + 1900,
             ttm.tm_mon + 1, ttm.tm_mday); 
    return timestamp;
}

char *getTimestamp(char timestamp[], time_t* t, int len)
{
    struct tm ttm;
    localtime_r(t, &ttm);
    snprintf(timestamp, len, "%04d-%02d-%02dT%02d:%02d:%02d",
       ttm.tm_year + 1900, ttm.tm_mon + 1, ttm.tm_mday, ttm.tm_hour,
       ttm.tm_min, ttm.tm_sec); 
    return timestamp;
}

int createDir(const char *name)
{
    if (access(name, W_OK) != 0)  
    {  
        if (mkdir((const char*)name, S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH)
              == -1)
        {   
            perror("mkdir error");   
            return -1;   
        }
    }
    return 0;
}

std::string getFilePath(const char *name, const char *date)
{
    std::string fileName;
    std::stringstream ss;
    ss << SAVE_DIR << '/' << name << '-' << date << ".csv";
    fileName = ss.str();
    return fileName;
}

FILE *getFileHandle(const char *name, const char *strime, FILEMAP *pFiles)
{
    FILE *file = nullptr;
    auto it = pFiles->find(name);
    if (it == pFiles->end())
    {
        std::string&& filePath = getFilePath(name, strime);
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

void closeFilesHandle(FILEMAP *pFiles)
{
    printf("begine to close file\n");
    for(auto it = pFiles->begin(); it != pFiles->end(); ++it)
    {
        FILE *file = it->second;
        if (file)
        {
            fclose(file);
            printf("close file %s\n", it->first.c_str());
        }
    }
    pFiles->clear();
}

void save_data()
{
    char *ptr = g_saveBuffer;
    
    char *dt = nullptr;
    char *name = nullptr;
    char *line_end = strchr(ptr, '\n');
    
    FILEMAP files;
    
    time_t t = time(0);
    char strtime[TIME_STR_LEN] = {0};
    getTimestamp(strtime, &t, TIME_STR_LEN);
    printf("\n***begine to save file at %s\n", strtime);
    
    uint32_t count = 0;
    while (line_end != nullptr)
    {
        // get name
        name = ptr;
        char* t = strchr(ptr, ',');
        if (!t) {
            printf("data error go get name %u\n", count);
            break;
        }
        *t++ = '\0';

        // data ptr
        ptr = dt = t;
        t = strchr(ptr, ',');
        if (!t) {
            printf("data error go get timestamp %u\n", count);
            break;
        }

        // get the date
        *t = '\0';
        getDate(strtime, dt, TIME_STR_LEN);
        *t = ',';

        // find the data-end
        *line_end = '\0';

        FILE *file = getFileHandle(name, strtime, &files);
        if (!file)
        {
            printf("error to open file %s!!!!!\n", name);
            continue;
        }
        if (fwrite(ptr, strlen(ptr), 1, file) != 1) {
            printf("error to fwrite data %u\n", count);
            g_stop = 1;
            break;
        }
        count++;

#ifdef DEBUG
        printf("index:%d,name:%s,data:%s\n", count, name, ptr);
#endif //DEBUG

        ptr = line_end + 1;
        line_end = strchr(ptr, '\n');
    }

    closeFilesHandle(&files);

    t = time(0);
    getTimestamp(strtime, &t, TIME_STR_LEN);
    printf("***save count:%d, at %s\n", count, strtime);
}

void* writeFileThread(void*)
{
    while (1)
    {
        std::unique_lock<std::mutex> lk(m_mutex);
        m_dataCondition.wait(lk, [&]{return (g_write || g_stop);});
        save_data();
        g_write = 0;
        lk.unlock();
        if (g_stop) break;
    }
    printf("recvbufer thread exit!!!!\n");
    return 0;
}

void signalHander(int signum) 
{
    printf("catch signal %d\n", signum);
    g_stop = 1;
    if (g_sock)
        close(g_sock);
}

int main(int argc, char** argv)
{
    if (argc != 2)
    {
        perror("Usage: asc <udp port>\n Example:\n   UdpFileWriter 8899\n");
        return EXIT_FAILURE;
    }

    char* all_buf = (char*)malloc(RECV_BUFFER_SIZE * 2);
    g_recvBuffer = all_buf;
    g_saveBuffer = all_buf + RECV_BUFFER_SIZE;
    char* end_pos = g_recvBuffer + RECV_BUFFER_SIZE - MARGIN_LEN;
    char* recvbuf = g_recvBuffer;
    uint32_t recvbuf_len = RECV_BUFFER_SIZE;

    signal(SIGINT, signalHander);
    signal(SIGSTOP, signalHander);
    signal(SIGKILL, signalHander);
    signal(SIGQUIT, signalHander);
    
    if (0 != createDir(SAVE_DIR))
    {
        printf("error:create dir %s failed!!!", SAVE_DIR);
        return EXIT_FAILURE;
    }
    
    const char *port = argv[1];
    
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(atoi(port));
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    
    if ((g_sock = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
    {
        perror("socket");
        return EXIT_FAILURE;
    }
    
    //port bind to server
    if (bind(g_sock, (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror("bind error");
        return EXIT_FAILURE;
    }
    
    pthread_t tid;
    if (pthread_create(&tid, nullptr, writeFileThread, nullptr) != 0) 
    {
        printf("pthread_create error!!!");
        exit(EXIT_FAILURE);
    }
    
    struct sockaddr_in clientAddr;
    memset(&clientAddr,0,sizeof(clientAddr));
    int len = 0;
    socklen_t socklen = sizeof(clientAddr);
    
#ifdef DEBUG 
    uint32_t k = 0;
#endif //DEBUG

    while (g_stop == 0)
    {
        len = recvfrom(g_sock, recvbuf, recvbuf_len, 0,
		       (struct sockaddr*)&clientAddr, &socklen);
        if (len > 0)
        {
            recvbuf += len;
            *recvbuf++ = '\n'; //append a new-line
	    recvbuf_len -= (len + 1);

#ifdef DEBUG
            if ((recvbuf - g_recvBuffer) / 1024 > k) {
                k = (recvbuf - g_recvBuffer) / 1024;
                printf("%ld ", (recvbuf - g_recvBuffer));
                fflush(stdout);
            }
#endif // DEBUG

#ifdef DEBUG
            if (k > 9) // try 10K data to swap recvbufer
#else
            if (recvbuf >= end_pos) //recvbufer is full
#endif //DEBUG
            {
                // std::lock_guard<std::mutex> lk(m_mutex);

                // swap recv and save recvbufer
                char* tmp = g_saveBuffer;
                g_saveBuffer = g_recvBuffer;
                g_recvBuffer = tmp;

                // set new recvbuf and recvbufer-len of recvfrom to max length
                recvbuf = g_recvBuffer;
                end_pos = g_recvBuffer + RECV_BUFFER_SIZE - MARGIN_LEN;
                recvbuf_len = RECV_BUFFER_SIZE;

#ifdef DEBUG
                k = 0;
                printf("\nrecv recvbufer is %lx, save recvbufer %lx, end pos %lx\n",
                    (uint64_t)(g_recvBuffer), (uint64_t)(g_saveBuffer),
                    (uint64_t)(end_pos));
#endif //DEBUG
                g_write = 1;
                m_dataCondition.notify_one();
            }
        }
        else
        {
            perror("recv");
            break;
        }
    }

    {   //to release lock
        printf("about to exit process...\n");
        g_saveBuffer = g_recvBuffer;
        g_write = 1;
        m_dataCondition.notify_one();
    }

    pthread_join(tid, nullptr);
    free(all_buf);
    printf("process exited!!!");
    return 0;
}
