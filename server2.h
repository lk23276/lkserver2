#pragma once
// =============================================================
// server2.h  -  基于 C++ 类、线程池、单反应堆(Reactor) + 多反应堆
//               (主从 Reactor) 模型, 以及 select/poll/epoll 三种
//               I/O 多路转接机制的高并发 HTTP 静态服务器。
//
// 本文件是对 SImpleHttp 中 Server.h / Server.cpp / main.cpp 的重构,
// 功能与原实现保持一致, 仅做架构层面的优化与面向对象封装。
// 命名风格与中文注释尽量沿用原代码。
// =============================================================

#include <map>
#include <vector>
#include <string>
#include <utility>
#include <pthread.h>
#include <sys/select.h>   // fd_set
#include <poll.h>         // pollfd
#include <sys/epoll.h>    // epoll_event
#include <sys/types.h>    // off_t

using namespace std;

// ============ 宏定义 ============
// 反应堆使用的 I/O 多路转接机制: SELECT / POLL / EPOLL
#define SELECT_DISP 1
#define POLL_DISP   2
#define EPOLL_DISP  3

// 文件描述符感兴趣 / 被激活的事件
enum {
    READ_EVENT  = 0x01,   // 读事件
    WRITE_EVENT = 0x02,   // 写事件
    ERROR_EVENT = 0x04    // 异常事件
};

// 描述符在反应堆中的状态
enum class FdStatus {
    NEW = 0,    // 新建, 还未添加到检测集合
    ADDED = 1,  // 已经添加到检测集合
    DELETED = 2 // 已经从检测集合中删除
};

// 前向声明
class EventLoop;
class Channel;
class Dispatcher;

// 事件回调函数类型, 与原代码的 void* arg 风格保持一致
typedef void (*EventCallback)(void*);

// ============ 应用层输出缓冲 Buffer (写半包续发用) ============
// 连续内存 + 读游标, 用于把"一次 send 没发完的数据"缓存, 待 EPOLLOUT 续发
class Buffer {
public:
    void append(const char* data, size_t len) {
        m_buf.insert(m_buf.end(), data, data + len);
    }
    const char* peek() const { return m_buf.data() + m_readIndex; }
    size_t readableBytes() const { return m_buf.size() - m_readIndex; }
    void retrieve(size_t n) {
        m_readIndex += n;
        if (m_readIndex >= m_buf.size()) { m_buf.clear(); m_readIndex = 0; }
    }
private:
    std::vector<char> m_buf;
    size_t m_readIndex = 0;
};

// ============ 通道 Channel ============
// 对"一个文件描述符 + 它感兴趣的事件 + 对应的回调函数"的统一封装
class Channel {
public:
    Channel(int fd, int events,
            EventCallback read_cb, EventCallback write_cb,
            EventCallback destroy_cb, void* arg);
    ~Channel();

    int getSocket() const;
    int getEvent() const;
    void setEvent(int ev);
    int getRevents() const;
    void setRevents(int ev);

    FdStatus getStatus() const;
    void setStatus(FdStatus s);

    void setReadCallback(EventCallback cb);
    void setWriteCallback(EventCallback cb);
    void setErrorCallback(EventCallback cb);
    void setDestroyCallback(EventCallback cb);
    void setArg(void* arg);

    // 根据被激活的事件, 调用对应的回调函数
    void handleEvent();

private:
    int m_fd;                 // 文件描述符
    int m_events;             // 感兴趣的事件
    int m_revents;            // 被激活的事件
    EventCallback m_read_cb;  // 读事件回调
    EventCallback m_write_cb; // 写事件回调
    EventCallback m_error_cb; // 异常事件回调
    EventCallback m_destroy_cb; // 销毁回调
    void* m_arg;              // 回调参数
    FdStatus m_status;        // 在反应堆中的状态
};

// ============ 抽象分发器 Dispatcher (select / poll / epoll 的统一接口) ============
// 三种 I/O 多路转接机制都继承自该类, 对上层 EventLoop 提供一致的接口
class Dispatcher {
public:
    Dispatcher(EventLoop* evLoop);
    virtual ~Dispatcher();

    // 检测 fd 状态, 把激活的事件通过 EventLoop::eventActivate 交给反应堆处理
    virtual bool dispatch(int timeout = -1) = 0;
    // 添加 / 删除 / 修改一个 fd 的检测
    virtual bool add(Channel* ch) = 0;
    virtual bool remove(Channel* ch) = 0;
    virtual bool modify(Channel* ch) = 0;
    // 释放检测相关资源
    virtual bool clear() = 0;

    EventLoop* getEvLoop();

protected:
    EventLoop* m_evLoop;  // 所属反应堆
    int m_name;           // 1=select 2=poll 3=epoll
};

// 工厂函数: 根据类型创建对应的分发器 (select / poll / epoll)
Dispatcher* dispatcherInstance(int flag, EventLoop* evLoop);

// ---- select 分发器 ----
class SelectDispatcher : public Dispatcher {
public:
    SelectDispatcher(EventLoop* evLoop);
    ~SelectDispatcher();
    bool dispatch(int timeout = -1) override;
    bool add(Channel* ch) override;
    bool remove(Channel* ch) override;
    bool modify(Channel* ch) override;
    bool clear() override;
private:
    fd_set m_rSet;
    fd_set m_wSet;
    int m_maxFd;
};

// ---- poll 分发器 ----
class PollDispatcher : public Dispatcher {
public:
    PollDispatcher(EventLoop* evLoop);
    ~PollDispatcher();
    bool dispatch(int timeout = -1) override;
    bool add(Channel* ch) override;
    bool remove(Channel* ch) override;
    bool modify(Channel* ch) override;
    bool clear() override;
private:
    pollfd* m_pfds;        // pollfd 数组
    int m_pfdsSize;        // 数组容量
};

// ---- epoll 分发器 (Linux 默认, 效率最高, 与原代码一致) ----
class EpollDispatcher : public Dispatcher {
public:
    EpollDispatcher(EventLoop* evLoop);
    ~EpollDispatcher();
    bool dispatch(int timeout = -1) override;
    bool add(Channel* ch) override;
    bool remove(Channel* ch) override;
    bool modify(Channel* ch) override;
    bool clear() override;
private:
    int m_epfd;
    struct epoll_event* m_evs;
    int m_evsSize;
};

// ============ 反应堆 EventLoop ============
// 一个 EventLoop 持有一个 Dispatcher, 循环检测 fd 事件并分发回调。
// 主反应堆(Main Reactor)负责监听/接收新连接;
// 子反应堆(Sub Reactor)负责已连接 fd 的读写(多反应堆模型)。
class EventLoop {
public:
    EventLoop(int dispatcherType, const string& threadName, bool isMain = false);
    ~EventLoop();

    bool run();             // 启动反应堆循环
    bool stop();            // 退出循环
    bool addChannel(Channel* ch);       // 添加通道
    bool removeChannel(Channel* ch);    // 删除通道
    bool modifyChannel(Channel* ch);    // 修改通道关注的事件
    bool eventActivate(int fd, int events); // 处理被激活的事件

    Dispatcher* getDispatcher();
    void setThreadID(pthread_t tid);
    pthread_t getThreadID() const;
    const string& getThreadName() const;

    void wakeup();                       // 唤醒阻塞在 dispatch 上的本反应堆
    static void handleWakeup(void* arg); // eventfd 读回调, 仅消费唤醒计数

    // 供分发器在遍历通道表时加锁 (select/poll 每次检测都需重建集合)
    void lockMap();
    void unlockMap();
    map<int, Channel*>& getChannelMap();

private:
    int m_dispatcherType;
    Dispatcher* m_dispatcher;
    map<int, Channel*> m_channelMap; // fd -> Channel
    bool m_isQuit;
    bool m_isLooping;
    pthread_t m_threadID;
    string m_threadName;
    int m_timeout;          // 分发器超时(ms); 现统一为 -1, 靠 eventfd 唤醒(消除 2 秒延迟)
    pthread_mutex_t m_mutex;
    int m_wakeupFd;            // eventfd 唤醒 fd
    Channel* m_wakeupChannel;  // 唤醒通道, 注册到本反应堆的 epoll
};

// ============ 子反应堆线程 (one loop per thread) ============
class EventLoopThread {
public:
    EventLoopThread(const string& name, int dispatcherType);
    ~EventLoopThread();
    EventLoop* getEventLoop(); // 启动线程并等待其反应堆创建完成, 返回反应堆
    pthread_t getTid() const;  // 取得线程 id(优雅关闭时 join 用)
private:
    static void* threadFunc(void* arg);
    EventLoop* m_evLoop;
    pthread_t m_tid;
    pthread_mutex_t m_mutex;
    pthread_cond_t m_cond;
    string m_name;
    int m_dispatcherType;
};

// ============ 子反应堆线程池 (多反应堆模型) ============
// 内部维护若干个 EventLoopThread, 每个线程跑一个子反应堆。
// 主反应堆 accept 到新连接后, 通过 getNextLoop() 轮询分配给某个子反应堆。
class EventLoopThreadPool {
public:
    EventLoopThreadPool(EventLoop* mainLoop, int threadNum, int dispatcherType);
    ~EventLoopThreadPool();
    bool run();                   // 启动所有子反应堆线程
    EventLoop* getNextLoop();     // 轮询获取一个子反应堆 (负载均衡)
    bool stopAll();               // 通知所有子反应堆退出并回收线程(优雅关闭用)
private:
    EventLoop* m_mainLoop;
    int m_threadNum;
    int m_dispatcherType;
    vector<EventLoopThread*> m_threads;
    vector<EventLoop*> m_loops;
    int m_index;  // 轮询下标
};

// ============ 客户端连接 TcpConnection ============
// 封装一个已连接客户端 fd 及其事件处理 (原 recHttpR 逻辑)。
// 连接在哪个子反应堆上注册, 就在哪个子反应堆线程中处理其读写。
class TcpConnection {
public:
    TcpConnection(int fd, EventLoop* evLoop);
    ~TcpConnection();

    // 读事件回调 (对应原 recHttpR)
    static void recHttpR(void* arg);
    // 销毁回调
    static void destroyConnection(void* arg);

    Channel* getChannel();

    void send(const char* data, size_t len);   // 应用层发送(带写半包续发)
    static void handleWrite(void* arg);         // EPOLLOUT 续发回调
    void enableWriting();                       // 注册 EPOLLOUT
    void disableWriting();                      // 注销 EPOLLOUT
    void beginSendFile(int fd, off_t size);     // 以 sendfile 零拷贝方式发送文件(配合 EPOLLOUT 续发)
    void setCloseAfterWrite(bool v);            // 输出缓冲发完后关闭连接(避免截断响应)
    void tryCloseAfterWrite();                  // 正文同步发完且无待续发时立即关闭(配合 Connection: close)

private:
    void doSendFile();      // 内部: sendfile 循环, EAGAIN 时武装 EPOLLOUT 等待续发

    int m_fd;
    EventLoop* m_evLoop;
    Channel* m_channel;
    char m_buf[4096];  // 接收缓冲区, 与原代码一致
    Buffer m_outputBuffer;  // 输出缓冲(写半包续发)
    bool m_writing;         // 是否有数据待 EPOLLOUT 续发
    bool m_closeAfterWrite; // 输出缓冲发完后关闭连接(对端已 FIN 时避免丢弃尾部)
    int m_fileFd;           // 待 sendfile 发送的文件 fd, -1 表示无
    off_t m_fileOffset;     // sendfile 当前偏移(内核按实际发送量推进)
    off_t m_fileSize;       // 文件总大小
};

// ============ 接收器 Acceptor (运行在主反应堆) ============
// 负责 listen fd 的读事件, 即接受新连接, 并分发给子反应堆。
class Acceptor {
public:
    Acceptor(EventLoop* mainLoop, int lfd, EventLoopThreadPool* pool, int dispatcherType);
    ~Acceptor();
    int init();  // 初始化并注册监听通道到主反应堆
    Channel* getChannel();  // 取得监听通道(优雅关闭时从主反应堆移除)
    // 读事件回调 (对应原 acceptC)
    static void acceptC(void* arg);
private:
    int m_lfd;
    EventLoop* m_mainLoop;
    EventLoopThreadPool* m_pool;
    Channel* m_channel;
    int m_dispatcherType;
};

// ============ 与原始代码保持同名的工具函数 (功能不变) ============
int initLis(unsigned short port);          // 初始化监听的套接字
int parseRL(const char* line, TcpConnection* conn);    // 解析请求行
int sendFile(TcpConnection* conn, const char* fileName);// 发送文件
int seadHeadM(TcpConnection* conn, int status, const char* descr, const char* type, long long length); // 发送响应头
const char* getFT(const char* name);       // 获取文件类型(MIME)
int sendDir(TcpConnection* conn, const char* dirName); // 发送目录列表
int hexToDec(char c);                       // 十六进制字符转十进制
void decodeMsg(char* to, char* from);      // URL 解码
int setNonBlock(int fd);                    // 设置非阻塞
