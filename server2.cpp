#include "server2.h"
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/select.h>
#include <sys/time.h>
#include <poll.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <sys/sendfile.h>
#include <dirent.h>
#include <cctype>
#include <sys/stat.h>
#include <algorithm>
#include <sys/socket.h>    // send / MSG_NOSIGNAL
#include <sys/eventfd.h>  // eventfd 唤醒 fd
#include <sys/types.h>    // ssize_t / off_t
#include <cstdint>        // uint64_t
#include <new>            // std::nothrow
using namespace std;

// =============================================================
// Channel 实现
// =============================================================
Channel::Channel(int fd, int events,
                 EventCallback read_cb, EventCallback write_cb,
                 EventCallback destroy_cb, void* arg)
    : m_fd(fd), m_events(events), m_revents(0),
      m_read_cb(read_cb), m_write_cb(write_cb),
      m_error_cb(NULL), m_destroy_cb(destroy_cb),
      m_arg(arg), m_status(FdStatus::NEW) {
}

Channel::~Channel() {
}

int Channel::getSocket() const { return m_fd; }
int Channel::getEvent() const { return m_events; }
void Channel::setEvent(int ev) { m_events = ev; }
int Channel::getRevents() const { return m_revents; }
void Channel::setRevents(int ev) { m_revents = ev; }

FdStatus Channel::getStatus() const { return m_status; }
void Channel::setStatus(FdStatus s) { m_status = s; }

void Channel::setReadCallback(EventCallback cb) { m_read_cb = cb; }
void Channel::setWriteCallback(EventCallback cb) { m_write_cb = cb; }
void Channel::setErrorCallback(EventCallback cb) { m_error_cb = cb; }
void Channel::setDestroyCallback(EventCallback cb) { m_destroy_cb = cb; }
void Channel::setArg(void* arg) { m_arg = arg; }

void Channel::handleEvent() {
    if (m_revents & ERROR_EVENT) {
        if (m_error_cb) m_error_cb(m_arg);
    }
    if (m_revents & READ_EVENT) {
        if (m_read_cb) m_read_cb(m_arg);
    }
    if (m_revents & WRITE_EVENT) {
        if (m_write_cb) m_write_cb(m_arg);
    }
}

// =============================================================
// Dispatcher 抽象基类 + 工厂
// =============================================================
Dispatcher::Dispatcher(EventLoop* evLoop)
    : m_evLoop(evLoop), m_name(0) {
}

Dispatcher::~Dispatcher() {
}

EventLoop* Dispatcher::getEvLoop() { return m_evLoop; }

Dispatcher* dispatcherInstance(int flag, EventLoop* evLoop) {
    if (flag == SELECT_DISP) {
        return new SelectDispatcher(evLoop);
    } else if (flag == POLL_DISP) {
        return new PollDispatcher(evLoop);
    } else {
        // 默认使用 epoll, 与原代码一致
        return new EpollDispatcher(evLoop);
    }
}

// =============================================================
// SelectDispatcher 实现
// =============================================================
SelectDispatcher::SelectDispatcher(EventLoop* evLoop)
    : Dispatcher(evLoop) {
    m_name = SELECT_DISP;
    FD_ZERO(&m_rSet);
    FD_ZERO(&m_wSet);
    m_maxFd = 0;
}

SelectDispatcher::~SelectDispatcher() {
}

bool SelectDispatcher::add(Channel* ch) {
    // select 每次检测都从通道表重建集合, 这里无需额外操作
    return true;
}

bool SelectDispatcher::remove(Channel* ch) {
    return true;
}

bool SelectDispatcher::modify(Channel* ch) {
    return true;
}

bool SelectDispatcher::clear() {
    FD_ZERO(&m_rSet);
    FD_ZERO(&m_wSet);
    return true;
}

bool SelectDispatcher::dispatch(int timeout) {
    FD_ZERO(&m_rSet);
    FD_ZERO(&m_wSet);
    m_maxFd = 0;

    // 遍历通道表, 把需要检测读/写的 fd 加入集合
    m_evLoop->lockMap();
    for (auto& kv : m_evLoop->getChannelMap()) {
        Channel* ch = kv.second;
        int fd = ch->getSocket();
        if (fd > m_maxFd) m_maxFd = fd;
        int ev = ch->getEvent();
        if (ev & READ_EVENT)  FD_SET(fd, &m_rSet);
        if (ev & WRITE_EVENT) FD_SET(fd, &m_wSet);
    }
    m_evLoop->unlockMap();

    struct timeval tv;
    struct timeval* ptv = NULL;
    if (timeout != -1) {
        tv.tv_sec = timeout / 1000;
        tv.tv_usec = (timeout % 1000) * 1000;
        ptv = &tv;
    }

    int ret = ::select(m_maxFd + 1, &m_rSet, &m_wSet, NULL, ptv);
    if (ret == -1) {
        perror("select");
        return false;
    } else if (ret == 0) {
        return true; // 超时, 本轮无事件
    }

    // 找出被激活的 fd, 收集后统一交给反应堆处理 (避免在持锁时调用回调)
    m_evLoop->lockMap();
    vector<pair<int, int>> active;
    for (auto& kv : m_evLoop->getChannelMap()) {
        Channel* ch = kv.second;
        int fd = ch->getSocket();
        int revents = 0;
        if (FD_ISSET(fd, &m_rSet))  revents |= READ_EVENT;
        if (FD_ISSET(fd, &m_wSet))  revents |= WRITE_EVENT;
        if (revents) active.push_back(make_pair(fd, revents));
    }
    m_evLoop->unlockMap();

    for (auto& p : active) {
        m_evLoop->eventActivate(p.first, p.second);
    }
    return true;
}

// =============================================================
// PollDispatcher 实现
// =============================================================
PollDispatcher::PollDispatcher(EventLoop* evLoop)
    : Dispatcher(evLoop) {
    m_name = POLL_DISP;
    m_pfdsSize = 1024;
    m_pfds = new pollfd[m_pfdsSize];
}

PollDispatcher::~PollDispatcher() {
    delete[] m_pfds;
}

bool PollDispatcher::add(Channel* ch) {
    return true;
}

bool PollDispatcher::remove(Channel* ch) {
    return true;
}

bool PollDispatcher::modify(Channel* ch) {
    return true;
}

bool PollDispatcher::clear() {
    return true;
}

bool PollDispatcher::dispatch(int timeout) {
    // 根据通道表重建 pollfd 数组
    m_evLoop->lockMap();
    int i = 0;
    for (auto& kv : m_evLoop->getChannelMap()) {
        if (i >= m_pfdsSize) break; // 简化: 超出容量则忽略
        Channel* ch = kv.second;
        m_pfds[i].fd = ch->getSocket();
        m_pfds[i].events = 0;
        m_pfds[i].revents = 0;
        int ev = ch->getEvent();
        if (ev & READ_EVENT)  m_pfds[i].events |= POLLIN;
        if (ev & WRITE_EVENT) m_pfds[i].events |= POLLOUT;
        i++;
    }
    int nfds = i;
    m_evLoop->unlockMap();

    int ret = ::poll(m_pfds, nfds, timeout);
    if (ret == -1) {
        perror("poll");
        return false;
    } else if (ret == 0) {
        return true; // 超时
    }

    // 收集被激活的 fd
    m_evLoop->lockMap();
    vector<pair<int, int>> active;
    for (int j = 0; j < nfds; j++) {
        if (m_pfds[j].revents & (POLLIN | POLLERR | POLLHUP)) {
            int revents = READ_EVENT;
            if (m_pfds[j].revents & (POLLERR | POLLHUP)) revents |= ERROR_EVENT;
            active.push_back(make_pair(m_pfds[j].fd, revents));
        } else if (m_pfds[j].revents & POLLOUT) {
            active.push_back(make_pair(m_pfds[j].fd, WRITE_EVENT));
        }
    }
    m_evLoop->unlockMap();

    for (auto& p : active) {
        m_evLoop->eventActivate(p.first, p.second);
    }
    return true;
}

// =============================================================
// EpollDispatcher 实现
// =============================================================
EpollDispatcher::EpollDispatcher(EventLoop* evLoop)
    : Dispatcher(evLoop) {
    m_name = EPOLL_DISP;
    m_epfd = epoll_create(1);
    if (m_epfd == -1) {
        perror("epoll_create");
    }
    m_evsSize = 1024;
    m_evs = new struct epoll_event[m_evsSize];
}

EpollDispatcher::~EpollDispatcher() {
    if (m_epfd != -1) close(m_epfd);
    delete[] m_evs;
}

// 把项目自定义的逻辑事件位(READ_EVENT/WRITE_EVENT/ERROR_EVENT)翻译成 epoll 的 EPOLLxxx 常量。
// 二者的数值并不相同: READ_EVENT=0x01(恰好等于 EPOLLIN), 但 WRITE_EVENT=0x02 而 EPOLLOUT=0x004,
// ERROR_EVENT=0x04 而 EPOLLERR=0x008。若直接把逻辑位塞给 epoll_ctl 注册"写事件",
// 实际注册的是 EPOLLPRI(0x02, 带外数据), 导致 EPOLLOUT 永不触发、
// handleWrite 续发回调失效 -> 大文件(图片)写半包后剩余数据永远发不出去(响应被截断)。
static uint32_t toEpollEvents(int ev) {
    uint32_t e = 0;
    if (ev & READ_EVENT)  e |= EPOLLIN;
    if (ev & WRITE_EVENT) e |= EPOLLOUT;
    if (ev & ERROR_EVENT) e |= EPOLLERR;
    return e;
}

bool EpollDispatcher::add(Channel* ch) {
    int fd = ch->getSocket();
    struct epoll_event ev;
    ev.data.fd = fd;
    // 使用边沿触发 EPOLLET, 与原代码行为一致
    ev.events = toEpollEvents(ch->getEvent()) | EPOLLET;
    int ret = epoll_ctl(m_epfd, EPOLL_CTL_ADD, fd, &ev);
    if (ret == -1) {
        perror("epoll_ctl add");
        return false;
    }
    return true;
}

bool EpollDispatcher::remove(Channel* ch) {
    int fd = ch->getSocket();
    int ret = epoll_ctl(m_epfd, EPOLL_CTL_DEL, fd, NULL);
    if (ret == -1) {
        perror("epoll_ctl del");
        return false;
    }
    return true;
}

bool EpollDispatcher::modify(Channel* ch) {
    int fd = ch->getSocket();
    struct epoll_event ev;
    ev.data.fd = fd;
    // 同样必须翻译逻辑事件位; 否则 enableWriting() 注册不到 EPOLLOUT, 写续发失效
    ev.events = toEpollEvents(ch->getEvent()) | EPOLLET;
    int ret = epoll_ctl(m_epfd, EPOLL_CTL_MOD, fd, &ev);
    if (ret == -1) {
        perror("epoll_ctl mod");
        return false;
    }
    return true;
}

bool EpollDispatcher::clear() {
    if (m_epfd != -1) close(m_epfd);
    m_epfd = -1;
    return true;
}

bool EpollDispatcher::dispatch(int timeout) {
    int ret = epoll_wait(m_epfd, m_evs, m_evsSize, timeout);
    if (ret == -1) {
        perror("epoll_wait");
        return false;
    }
    for (int i = 0; i < ret; i++) {
        int fd = m_evs[i].data.fd;
        int events = 0;
        if (m_evs[i].events & EPOLLIN)  events |= READ_EVENT;
        if (m_evs[i].events & EPOLLOUT) events |= WRITE_EVENT;
        if (m_evs[i].events & (EPOLLERR | EPOLLHUP)) events |= ERROR_EVENT;
        m_evLoop->eventActivate(fd, events);
    }
    return true;
}

// =============================================================
// EventLoop 反应堆实现
// =============================================================
EventLoop::EventLoop(int dispatcherType, const string& threadName, bool isMain)
    : m_dispatcherType(dispatcherType), m_dispatcher(NULL),
      m_isQuit(false), m_isLooping(false),
      m_threadID(0), m_threadName(threadName),
      m_wakeupFd(-1), m_wakeupChannel(NULL) {
    pthread_mutex_init(&m_mutex, NULL);
    // 子反应堆不再依赖 2 秒超时轮询; 所有反应堆统一永久阻塞在 dispatch, 由 eventfd 唤醒
    m_timeout = -1;
    m_dispatcher = dispatcherInstance(m_dispatcherType, this);
    // 创建 eventfd 唤醒通道: 主反应堆跨线程加入新连接后 write 1 即可立即唤醒本反应堆,
    // 不必等待 2 秒超时或任何真实事件
    m_wakeupFd = eventfd(0, EFD_NONBLOCK);
    if (m_wakeupFd == -1) {
        perror("eventfd");
    } else {
        m_wakeupChannel = new Channel(m_wakeupFd, READ_EVENT,
                                     EventLoop::handleWakeup, NULL, NULL, this);
        addChannel(m_wakeupChannel);
    }
    (void)isMain;
}

EventLoop::~EventLoop() {
    if (m_dispatcher) {
        m_dispatcher->clear();
        delete m_dispatcher;
    }
    if (m_wakeupChannel) { delete m_wakeupChannel; m_wakeupChannel = NULL; }
    if (m_wakeupFd != -1) { close(m_wakeupFd); m_wakeupFd = -1; }
    pthread_mutex_destroy(&m_mutex);
}

bool EventLoop::run() {
    m_isLooping = true;
    while (!m_isQuit) {
        m_dispatcher->dispatch(m_timeout);
    }
    m_isLooping = false;
    return true;
}

bool EventLoop::stop() {
    m_isQuit = true;
    wakeup();  // 唤醒可能阻塞在 dispatch 上的本反应堆, 使其尽快检查 m_isQuit 并退出
    return true;
}

void EventLoop::wakeup() {
    if (m_wakeupFd == -1) return;
    uint64_t one = 1;
    ssize_t n = ::write(m_wakeupFd, &one, sizeof(one));
    (void)n;
}

void EventLoop::handleWakeup(void* arg) {
    EventLoop* loop = (EventLoop*)arg;
    uint64_t buf = 0;
    // 读空 eventfd 计数器(非阻塞, 读到 EAGAIN 表示已空), 仅用于消费唤醒信号
    while (::read(loop->m_wakeupFd, &buf, sizeof(buf)) > 0) {
        // 无需处理
    }
}

void EventLoop::lockMap() { pthread_mutex_lock(&m_mutex); }
void EventLoop::unlockMap() { pthread_mutex_unlock(&m_mutex); }
map<int, Channel*>& EventLoop::getChannelMap() { return m_channelMap; }

Dispatcher* EventLoop::getDispatcher() { return m_dispatcher; }
void EventLoop::setThreadID(pthread_t tid) { m_threadID = tid; }
pthread_t EventLoop::getThreadID() const { return m_threadID; }
const string& EventLoop::getThreadName() const { return m_threadName; }

bool EventLoop::addChannel(Channel* ch) {
    lockMap();
    m_channelMap[ch->getSocket()] = ch;
    unlockMap();
    ch->setStatus(FdStatus::ADDED);
    return m_dispatcher->add(ch);
}

bool EventLoop::removeChannel(Channel* ch) {
    lockMap();
    auto it = m_channelMap.find(ch->getSocket());
    if (it != m_channelMap.end()) {
        m_channelMap.erase(it);
    }
    unlockMap();
    ch->setStatus(FdStatus::DELETED);
    return m_dispatcher->remove(ch);
}

bool EventLoop::modifyChannel(Channel* ch) {
    return m_dispatcher->modify(ch);
}

bool EventLoop::eventActivate(int fd, int events) {
    lockMap();
    auto it = m_channelMap.find(fd);
    Channel* ch = (it != m_channelMap.end()) ? it->second : NULL;
    unlockMap();
    if (ch == NULL) {
        return false;
    }
    ch->setRevents(events);
    ch->handleEvent();
    return true;
}

// =============================================================
// EventLoopThread 实现 (one loop per thread)
// =============================================================
EventLoopThread::EventLoopThread(const string& name, int dispatcherType)
    : m_evLoop(NULL), m_name(name), m_dispatcherType(dispatcherType) {
    pthread_mutex_init(&m_mutex, NULL);
    pthread_cond_init(&m_cond, NULL);
}

EventLoopThread::~EventLoopThread() {
    pthread_mutex_destroy(&m_mutex);
    pthread_cond_destroy(&m_cond);
    if (m_evLoop) delete m_evLoop;
}

void* EventLoopThread::threadFunc(void* arg) {
    EventLoopThread* t = (EventLoopThread*)arg;
    t->m_evLoop = new EventLoop(t->m_dispatcherType, t->m_name, false);
    // 通知主线程: 子反应堆已创建就绪
    pthread_mutex_lock(&t->m_mutex);
    pthread_cond_signal(&t->m_cond);
    pthread_mutex_unlock(&t->m_mutex);
    // 进入子反应堆循环 (本线程内)
    t->m_evLoop->run();
    return NULL;
}

EventLoop* EventLoopThread::getEventLoop() {
    pthread_mutex_lock(&m_mutex);
    if (m_evLoop == NULL) {
        pthread_create(&m_tid, NULL, threadFunc, this);
        // 等待子线程把 EventLoop 创建好
        pthread_cond_wait(&m_cond, &m_mutex);
    }
    pthread_mutex_unlock(&m_mutex);
    return m_evLoop;
}

pthread_t EventLoopThread::getTid() const {
    return m_tid;
}

// =============================================================
// EventLoopThreadPool 实现 (多反应堆线程池)
// =============================================================
EventLoopThreadPool::EventLoopThreadPool(EventLoop* mainLoop, int threadNum, int dispatcherType)
    : m_mainLoop(mainLoop), m_threadNum(threadNum),
      m_dispatcherType(dispatcherType), m_index(0) {
}

EventLoopThreadPool::~EventLoopThreadPool() {
    for (auto t : m_threads) delete t;
}

bool EventLoopThreadPool::run() {
    if (m_threadNum <= 0) return false;
    for (int i = 0; i < m_threadNum; i++) {
        string name = "SubReactor-" + to_string(i);
        EventLoopThread* t = new EventLoopThread(name, m_dispatcherType);
        // getEventLoop 会启动线程并等待其反应堆就绪
        EventLoop* loop = t->getEventLoop();
        m_threads.push_back(t);
        m_loops.push_back(loop);
    }
    return true;
}

EventLoop* EventLoopThreadPool::getNextLoop() {
    if (m_loops.empty()) return m_mainLoop;
    EventLoop* loop = m_loops[m_index];
    m_index = (m_index + 1) % m_threadNum;
    return loop;
}

// 优雅关闭: 通知每个子反应堆退出(设 m_isQuit + 唤醒), 再 join 回收线程
bool EventLoopThreadPool::stopAll() {
    for (auto loop : m_loops) {
        loop->stop();   // 设置退出标志并 wakeup 其 eventfd, 使其尽快退出 run 循环
    }
    for (auto t : m_threads) {
        pthread_join(t->getTid(), NULL);   // 等待子反应堆线程结束
    }
    return true;
}

// =============================================================
// TcpConnection 实现
// =============================================================
TcpConnection::TcpConnection(int fd, EventLoop* evLoop)
    : m_fd(fd), m_evLoop(evLoop), m_writing(false), m_closeAfterWrite(false),
      m_fileFd(-1), m_fileOffset(0), m_fileSize(0) {
    // 注册读 + 写回调到子反应堆; 写回调(handleWrite)用于 EPOLLOUT 续发
    m_channel = new Channel(m_fd, READ_EVENT,
                            TcpConnection::recHttpR, TcpConnection::handleWrite,
                            TcpConnection::destroyConnection, this);
    m_evLoop->addChannel(m_channel);
}

TcpConnection::~TcpConnection() {
    if (m_fileFd >= 0) {   // 连接销毁时若还有未发完的文件, 释放其文件 fd
        close(m_fileFd);
        m_fileFd = -1;
    }
    if (m_channel) {
        delete m_channel;
        m_channel = NULL;
    }
}

Channel* TcpConnection::getChannel() { return m_channel; }

// 应用层发送: 先尝试直接写, 写不完则缓存到输出缓冲并注册 EPOLLOUT 续发
void TcpConnection::send(const char* data, size_t len) {
    // m_writing: 缓冲里还有待续发数据; m_fileFd>=0: 还有文件待 sendfile 发送。
    // 两种情况都先排队, 由 handleWrite 依序发送, 保证"响应头在前、文件体在后"。
    if (m_writing || m_fileFd >= 0) {
        m_outputBuffer.append(data, len);
        return;
    }
    // MSG_NOSIGNAL: 对端已断开时返回 -1/EPIPE 而非触发 SIGPIPE 杀进程
    ssize_t n = ::send(m_fd, data, len, MSG_NOSIGNAL);
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            n = 0;  // 内核发送缓冲满, 交给 EPOLLOUT 续发
        } else {
            perror("send");
            destroyConnection(this);
            return;
        }
    }
    if (static_cast<size_t>(n) < len) {
        // 部分写: 剩余数据存入输出缓冲, 注册 EPOLLOUT
        m_outputBuffer.append(data + n, len - n);
        m_writing = true;
        enableWriting();
    }
}

// 开始以 sendfile 零拷贝方式发送一个文件: 仅记录 fd/偏移/大小,
// 数据由 sendfile 从文件直接送入 socket, 不经过应用层缓冲、不读入内存。
// 若此刻仍有响应头等缓冲数据未发完, 先只登记, 由 handleWrite 在缓冲清空后启动。
void TcpConnection::beginSendFile(int fd, off_t size) {
    m_fileFd = fd;
    m_fileOffset = 0;
    m_fileSize = size;
    if (m_writing) {
        return;        // 前面还有数据没发完, 等 handleWrite 调用 doSendFile
    }
    doSendFile();      // 缓冲已空, 立即开始
}

// sendfile 续发循环: 非阻塞 socket 下缓冲满返回 EAGAIN, 此时武装 EPOLLOUT 让出线程,
// 待内核可写时回到 handleWrite 继续从 m_fileOffset 发送 —— 不自旋、不阻塞。
void TcpConnection::doSendFile() {
    while (m_fileOffset < m_fileSize) {
        ssize_t n = sendfile(m_fd, m_fileFd, &m_fileOffset,
                             (size_t)(m_fileSize - m_fileOffset));
        if (n > 0) {
            continue;                                  // 已发出 n 字节, 继续
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            m_writing = true;
            enableWriting();                           // 武装 EPOLLOUT, 等可写续发
            return;
        } else if (n < 0 && errno == EINTR) {
            continue;                                  // 被信号打断, 重试
        } else if (n < 0) {
            perror("sendfile");
            destroyConnection(this);
            return;
        } else {
            break;                                     // n == 0(极少见)
        }
    }
    // 文件发送完毕: 关闭文件 fd, 解除 EPOLLOUT 收尾
    close(m_fileFd);
    m_fileFd = -1;
    m_fileOffset = 0;
    m_fileSize = 0;
    m_writing = false;
    disableWriting();
    if (m_closeAfterWrite) {
        destroyConnection(this);
    }
}

// EPOLLOUT 触发: 先发完应用层缓冲(响应头/目录列表), 再 sendfile 续发文件
void TcpConnection::handleWrite(void* arg) {
    TcpConnection* conn = (TcpConnection*)arg;
    // 1. 先发完应用层缓冲
    while (conn->m_outputBuffer.readableBytes() > 0) {
        ssize_t n = ::send(conn->m_fd, conn->m_outputBuffer.peek(),
                           conn->m_outputBuffer.readableBytes(), MSG_NOSIGNAL);
        if (n > 0) {
            conn->m_outputBuffer.retrieve(n);
        } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return;  // 缓冲满, 保持 EPOLLOUT 武装, 等下次边缘触发
        } else if (n < 0) {
            perror("send");
            destroyConnection(conn);
            return;
        } else {
            return;  // n == 0(极少见), 直接返回等待下次事件
        }
    }
    // 2. 缓冲已清空: 若还有文件待发, 交给 doSendFile 零拷贝续发(其内部处理收尾)
    if (conn->m_fileFd >= 0) {
        conn->doSendFile();
        return;
    }
    // 3. 全部发完 -> 解除 EPOLLOUT, 避免忙等烧 CPU
    conn->m_writing = false;
    conn->disableWriting();
    // 若对端已 FIN 且要求"发完再关", 此时数据已空, 安全关闭连接(不会丢弃尾部)
    if (conn->m_closeAfterWrite) {
        destroyConnection(conn);
    }
}

void TcpConnection::enableWriting() {
    m_channel->setEvent(READ_EVENT | WRITE_EVENT);
    m_evLoop->modifyChannel(m_channel);
}

void TcpConnection::disableWriting() {
    m_channel->setEvent(READ_EVENT);
    m_evLoop->modifyChannel(m_channel);
}

void TcpConnection::setCloseAfterWrite(bool v) {
    m_closeAfterWrite = v;
}

// 正文发送完毕后调用: 若要求"发完关闭"且已无待续发数据(缓冲为空且无待发文件), 立即关闭;
// 若仍有数据在途(会走 EPOLLOUT 续发), 则交给 handleWrite 在清空后关闭
void TcpConnection::tryCloseAfterWrite() {
    if (m_closeAfterWrite && !m_writing && m_fileFd < 0 &&
        m_outputBuffer.readableBytes() == 0) {
        destroyConnection(this);
    }
}

// 读事件回调, 逻辑与原 recHttpR 完全一致
void TcpConnection::recHttpR(void* arg) {
    TcpConnection* conn = (TcpConnection*)arg;
    printf("开始接受数据了...\n");
    int len, totle;
    char tmp[1024] = { 0 };
    memset(conn->m_buf, 0, sizeof(conn->m_buf));
    len = 0;
    totle = 0;
    while ((len = recv(conn->m_fd, tmp, sizeof tmp, 0)) > 0) {
        if (totle + len < (int)sizeof(conn->m_buf)) {
            memcpy(conn->m_buf + totle, tmp, len);
        }
        totle += len;
    }
    // 判断数据是否被接受完毕
    if (len == -1 && errno == EAGAIN) {
        // 解析请求行
        char* pt = strstr(conn->m_buf, "\r\n");
        if (pt != NULL) {
            int reqlen = pt - conn->m_buf;
            conn->m_buf[reqlen] = '\0';
            parseRL(conn->m_buf, conn);
        }
    } else if (len == 0) {
        // 客户端断开了连接(FIN)
        // 若还有数据在输出缓冲/内核发送缓冲中未发出, 不能直接 close(会丢弃图片尾部),
        // 改为标记"发完再关", 由 handleWrite 在缓冲清空后统一关闭
        if (conn->m_writing || conn->m_fileFd >= 0 ||
            conn->m_outputBuffer.readableBytes() > 0) {
            conn->m_closeAfterWrite = true;
            return;
        }
        conn->m_evLoop->removeChannel(conn->m_channel);
        close(conn->m_fd);
        delete conn;
        return;
    } else {
        perror("recv");
    }
}

void TcpConnection::destroyConnection(void* arg) {
    TcpConnection* conn = (TcpConnection*)arg;
    conn->m_evLoop->removeChannel(conn->m_channel);
    close(conn->m_fd);
    delete conn;
}

// =============================================================
// Acceptor 实现 (主反应堆)
// =============================================================
Acceptor::Acceptor(EventLoop* mainLoop, int lfd, EventLoopThreadPool* pool, int dispatcherType)
    : m_lfd(lfd), m_mainLoop(mainLoop), m_pool(pool), m_channel(NULL),
      m_dispatcherType(dispatcherType) {
}

Acceptor::~Acceptor() {
    if (m_channel) delete m_channel;
}

int Acceptor::init() {
    // 监听 fd 设为非阻塞, 配合边沿触发/多连接循环 accept
    setNonBlock(m_lfd);
    // 把监听通道注册到主反应堆, 读事件回调为 acceptC
    m_channel = new Channel(m_lfd, READ_EVENT,
                            Acceptor::acceptC, NULL, NULL, this);
    m_mainLoop->addChannel(m_channel);
    return 0;
}

Channel* Acceptor::getChannel() {
    return m_channel;
}

// 读事件回调, 逻辑与原 acceptC 一致: 接受连接 -> 分发给子反应堆
void Acceptor::acceptC(void* arg) {
    Acceptor* ac = (Acceptor*)arg;
    while (1) {
        int cfd = accept(ac->m_lfd, NULL, NULL);
        if (cfd == -1) {
            if (errno == EAGAIN) {
                break; // 非阻塞, 已无更多待接受连接
            }
            if (errno == EINTR) {
                continue; // 被信号打断, 重试
            }
            perror("accept");
            break;
        }
        // 2.设置非阻塞
        setNonBlock(cfd);
        // 3.轮询从子反应堆线程池中选取一个, 把新连接交给它处理
        EventLoop* subLoop = ac->m_pool->getNextLoop();
        TcpConnection* conn = new TcpConnection(cfd, subLoop);
        // 新连接已加入子反应堆的 epoll; 立即 write(eventfd,1) 唤醒正在 epoll_wait 的子反应堆,
        // 无需等待 2 秒超时或任何真实事件
        subLoop->wakeup();
        printf("acceptC -> 新连接 cfd=%d 交给 %s 处理\n",
               cfd, subLoop->getThreadName().c_str());
    }
}

// =============================================================
// 以下工具函数与原 Server.cpp 中保持一致 (功能不变)
// =============================================================

// 设置文件描述符为非阻塞
int setNonBlock(int fd) {
    int flag = fcntl(fd, F_GETFL);
    if (flag == -1) {
        perror("fcntl get");
        return -1;
    }
    flag |= O_NONBLOCK;
    if (fcntl(fd, F_SETFL, flag) == -1) {
        perror("fcntl set");
        return -1;
    }
    return 0;
}

// 初始化监听的套接字
int initLis(unsigned short port) {
    //1.设置监听的套接字
    int lfd, ret, opt;
    lfd = socket(AF_INET, SOCK_STREAM, 0);
    if (lfd == -1) {
        perror("socket");
        return -1;
    }
    //2.设置端口复用
    opt = 1;
    ret = setsockopt(lfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof opt);
    if (ret == -1) {
        perror("setsockopt");
        return -1;
    }
    //3.绑定
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    ret = bind(lfd, (struct sockaddr*)&addr, sizeof addr);
    if (ret == -1) {
        perror("bind");
        return -1;
    }
    //4.设置监听
    ret = listen(lfd, 128);
    if (ret == -1) {
        perror("listen");
        return -1;
    }
    //5.返回fd
    return lfd;
}

// 解析请求行 get /xxx/1.jpg http/1.1
int parseRL(const char* line, TcpConnection* conn) {
    int ret;
    char method[12];
    char path[1024];
    // 加宽度限定, 防止超长请求行溢出 method/path 缓冲区
    // (method 最多 11 字符, path 最多 1023 字符, 与 method[12]/path[1024] 匹配)
    sscanf(line, "%11[^ ] %1023[^ ]", method, path);
    printf("method: %s, path: %s\n", method, path);
    if (strcasecmp(method, "get") != 0) {
        return -1;
    }
    decodeMsg(path, path);
    // 处理客户端请求的静态资源（目录或者文件）
    char* file;
    file = NULL;
    if (strcmp(path, "/") == 0) {
        file = "./";
    } else {
        file = path + 1;
    }
    // 获取文件属性
    struct stat st;
    ret = stat(file, &st);
    if (ret == -1) {
        // 文件不存在 -- 回复404
        seadHeadM(conn, 404, "Not Found", getFT(".html"), -1);
        sendFile(conn, "404.html");
        conn->setCloseAfterWrite(true);   // 配合 Connection: close, 正文发完后关闭
        conn->tryCloseAfterWrite();       // 若已同步发完则立即关, 否则由 handleWrite 关闭
        return 0;
    }
    // 判断文件类型
    if (S_ISDIR(st.st_mode)) {
        // 把这个目录中的内容发送给客户端
        seadHeadM(conn, 200, "OK", getFT(".html"), -1);
        sendDir(conn, file);
        conn->setCloseAfterWrite(true);   // 配合 Connection: close, 正文发完后关闭
        conn->tryCloseAfterWrite();       // 若已同步发完则立即关, 否则由 handleWrite 关闭
    } else {
        // 把文件的内容发送给客户端
        seadHeadM(conn, 200, "OK", getFT(file), st.st_size);
        sendFile(conn, file);
    }
    return 0;
}

const char* getFT(const char* name) {
    // a.jpg a.mp4 a.html
    const char* dot = strrchr(name, '.');
    if (dot == NULL)
        return "text/plain; charset=utf-8"; // 纯文本
    if (strcmp(dot, ".html") == 0 || strcmp(dot, ".htm") == 0)
        return "text/html; charset=utf-8";
    if (strcmp(dot, ".jpg") == 0 || strcmp(dot, ".jpeg") == 0)
        return "image/jpeg";
    if (strcmp(dot, ".gif") == 0)
        return "image/gif";
    if (strcmp(dot, ".png") == 0)
        return "image/png";
    if (strcmp(dot, ".css") == 0)
        return "text/css";
    if (strcmp(dot, ".au") == 0)
        return "audio/basic";
    if (strcmp(dot, ".wav") == 0)
        return "audio/wav";
    if (strcmp(dot, ".avi") == 0)
        return "video/x-msvideo";
    if (strcmp(dot, ".mov") == 0 || strcmp(dot, ".qt") == 0)
        return "video/quicktime";
    if (strcmp(dot, ".mpeg") == 0 || strcmp(dot, ".mpe") == 0)
        return "video/mpeg";
    if (strcmp(dot, ".vrml") == 0 || strcmp(dot, ".wrl") == 0)
        return "model/vrml";
    if (strcmp(dot, ".midi") == 0 || strcmp(dot, ".mid") == 0)
        return "audio/midi";
    if (strcmp(dot, ".mp3") == 0)
        return "audio/mpeg";
    if (strcmp(dot, ".ogg") == 0)
        return "application/ogg";
    if (strcmp(dot, ".pac") == 0)
        return "application/x-ns-proxy-autoconfig";

    return "text/plain; charset=utf-8";
}

int sendDir(TcpConnection* conn, const char* dirName) {
    char buf[4096] = { 0 };
    // 安全写法: snprintf 带缓冲区长度, 防止超长目录名溢出
    snprintf(buf, sizeof(buf), "<html><head><title>%s</title></head><body><table>", dirName);
    conn->send(buf, strlen(buf));
    struct dirent** namelist;
    int i, num;
    num = scandir(dirName, &namelist, NULL, alphasort);
    if (num < 0) {
        perror("scandir");
        return -1;
    }
    for (i = 0; i < num; i++) {
        // 取出文件名 namelist指向的是一个指针数组
        char* name = namelist[i]->d_name;
        struct stat st;
        char subPath[1024] = { 0 };
        snprintf(subPath, sizeof(subPath), "%s/%s", dirName, name);
        stat(subPath, &st);
        if (S_ISDIR(st.st_mode)) {
            // a标签<a href="">name</a>
            snprintf(buf, sizeof(buf), "<tr><td><a href=\"%s/\">%s</a></td><td>%ld</td></tr>",
                name, name, st.st_size);
        } else {
            snprintf(buf, sizeof(buf), "<tr><td><a href=\"%s\">%s</a></td><td>%ld</td></tr>",
                name, name, st.st_size);
        }
        conn->send(buf, strlen(buf));
        free(namelist[i]);
    }
    snprintf(buf, sizeof(buf), "</table></body></html>");
    conn->send(buf, strlen(buf));
    free(namelist);
    return 0;
}

int sendFile(TcpConnection* conn, const char* fileName) {
    //1.打开文件(只读)
    int fd;
    fd = open(fileName, O_RDONLY);
    if (fd < 0) {                       // 安全写法: 检查 open 失败(原 assert(fd>0) 在 Release 下失效)
        perror("open");
        return -1;
    }
    //2.取文件大小(仅用于发送边界 / content-length; 数据不读入内存)
    off_t size = lseek(fd, 0, SEEK_END);
    if (size < 0) {
        perror("lseek");
        close(fd);
        return -1;
    }
    lseek(fd, 0, SEEK_SET);
    //3.零拷贝发送: 把"待发送文件"(fd)交给连接, 由 sendfile + EPOLLOUT 续发完成。
    //  既不像原 sendfile 循环那样在 EAGAIN 上自旋/阻塞卡死事件循环,
    //  也不像"整文件读入内存"那样占用 O(文件大小) 内存(GB 级文件 OOM 风险)。
    conn->beginSendFile(fd, size);
    return 0;
}

int seadHeadM(TcpConnection* conn, int status, const char* descr, const char* type, long long length) {
    // 状态行
    char buf[4096] = { 0 };
    // 安全写法: snprintf 带剩余缓冲区长度, 防止超长响应头溢出
    snprintf(buf, sizeof(buf), "http/1.1 %d %s\r\n", status, descr);
    // 响应头
    snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), "content-type: %s\r\n", type);
    // length>=0: 正常文件, 发送准确的 content-length(浏览器据此判断正文边界)
    // length<0 : 404/目录等无法预知长度, 发送 Connection: close, 由调用方在正文发完后关闭连接,
    //            避免发送 content-length: -1 这种非法头部导致浏览器无法界定正文
    if (length >= 0) {
        snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), "content-length: %lld\r\n\r\n", length);
    } else {
        snprintf(buf + strlen(buf), sizeof(buf) - strlen(buf), "connection: close\r\n\r\n");
    }
    conn->send(buf, strlen(buf));   // 走应用层 Buffer + EPOLLOUT 续发, 不再裸 send
    return 0;
}

// 将字符转换为整形数
int hexToDec(char c) {
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return c - 'a' + 10;
    if (c >= 'A' && c <= 'F')
        return c - 'A' + 10;
    return 0;
}

// to:输出缓冲区, from:编码后的url字符串
void decodeMsg(char* to, char* from) {
    for (; *from != '\0'; )   // 删掉for头部的++from，手动控制指针移动
    {
        if (from[0] == '%' && isxdigit(from[1]) && isxdigit(from[2]))
        {
            // 解析 %XX
            *to = hexToDec(from[1]) * 16 + hexToDec(from[2]);
            to++;
            from += 3;    // 一次性跳过 % X X 三个字符
        }
        else
        {
            *to = *from;
            to++;
            from++;
        }
    }
    *to = '\0';   // 追加字符串结束标记
}
