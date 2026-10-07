#define _GNU_SOURCE                 // 显式启用 GNU 扩展, 保证 signalfd/eventfd 等声明可见
#include <cstdio>
#include <cstdlib>
#include <unistd.h>
#include <string>
#include <signal.h>        // sigemptyset / sigaddset / sigprocmask / SIGINT / SIGTERM
#include <sys/signalfd.h>  // signalfd 优雅关闭
#include "server2.h"

// 优雅关闭上下文: 把"停止 accept / 停止子反应堆 / 退出主循环"所需的句柄打包,
// 作为 signalfd 读回调的参数传入(回调跑在主反应堆线程, 可安全操作 epoll/加锁)
struct ShutdownCtx {
    EventLoop* mainLoop;
    Acceptor* acceptor;
    EventLoopThreadPool* pool;
    int sfd;
    Channel* sigChannel;
    bool shuttingDown;
};

// signalfd 读回调: 收到 SIGINT/SIGTERM 时在事件循环上下文安全执行关闭流程,
// 而不是被内核的默认动作直接枪毙进程
void onSignalHandler(void* arg) {
    ShutdownCtx* ctx = static_cast<ShutdownCtx*>(arg);
    if (ctx->shuttingDown) return;        // 防止信号重复触发导致重复执行
    ctx->shuttingDown = true;

    // 排空 signalfd 中已就绪的信号
    struct signalfd_siginfo si;
    while (::read(ctx->sfd, &si, sizeof(si)) > 0) { /* 仅消费, 不处理具体内容 */ }

    printf("\n收到信号 %d, 开始优雅关闭 ...\n", si.ssi_signo);
    // 1. 停止接受新连接: 从主反应堆移除监听通道
    ctx->mainLoop->removeChannel(ctx->acceptor->getChannel());
    // 2. 通知所有子反应堆退出并回收线程(各子反应堆的 eventfd 会被 wakeup 立即唤醒)
    ctx->pool->stopAll();
    // 3. 主反应堆退出: run 循环在本次 dispatch 返回后会检测 m_isQuit 并结束
    ctx->mainLoop->stop();
}

int main(int argc, char* argv[])
{
    int lfd;
    if (argc < 3)
    {
        printf("./server2 port path [select|poll|epoll]\n");
        return -1;
    }
    unsigned short port;
    port = atoi(argv[1]);
    // 切换服务器的工作路径
    chdir(argv[2]);

    // 选择 I/O 多路转接机制: 默认 epoll (与原代码一致)
    // 也可通过第 3 个参数指定 select / poll / epoll
    int dispatcherType = EPOLL_DISP;
    if (argc >= 4) {
        string mode = argv[3];
        if (mode == "select")      dispatcherType = SELECT_DISP;
        else if (mode == "poll")   dispatcherType = POLL_DISP;
        else if (mode == "epoll")  dispatcherType = EPOLL_DISP;
    }

    // 初始化用于监听的套接字
    lfd = initLis(port);

    // 优雅关闭前置: 阻塞 SIGINT/SIGTERM, 使其不会触发内核默认动作(直接终止进程)。
    // 必须在线程创建前完成, 子反应堆线程会继承此信号掩码; 信号之后统一由 signalfd 接管。
    sigset_t sigMask;
    sigemptyset(&sigMask);
    sigaddset(&sigMask, SIGINT);
    sigaddset(&sigMask, SIGTERM);
    sigprocmask(SIG_BLOCK, &sigMask, NULL);

    // 1) 主反应堆 (Main Reactor): 只负责监听/接受新连接
    EventLoop* mainLoop = new EventLoop(dispatcherType, "MainReactor", true);

    // 2) 子反应堆线程池 (多反应堆模型): 每个线程跑一个子反应堆, 处理已连接 fd 的读写
    //    线程数可根据 CPU 核数调整, 这里默认 4 个
    int subReactorNum = 4;
    EventLoopThreadPool* pool = new EventLoopThreadPool(mainLoop, subReactorNum, dispatcherType);
    pool->run();

    // 3) 接收器: 把监听通道注册到主反应堆, 新连接由 acceptC 轮询分发给子反应堆
    Acceptor* acceptor = new Acceptor(mainLoop, lfd, pool, dispatcherType);
    acceptor->init();

    // 优雅关闭: 把被阻塞的 SIGINT/SIGTERM 变成可读 fd, 注册进主反应堆。
    // 回调 onSignalHandler 跑在主反应堆线程, 可安全调用 removeChannel / stopAll / stop,
    // 不必在 async-signal-unsafe 的 signal handler 里直接碰 epoll/加锁。
    ShutdownCtx shutdownCtx;
    shutdownCtx.mainLoop = mainLoop;
    shutdownCtx.acceptor = acceptor;
    shutdownCtx.pool = pool;
    // 必须 SFD_NONBLOCK: 否则 onSignalHandler 里 while(read>0) 排空信号后,
    // 第二轮 read 会永久阻塞(阻塞模式无信号时 read 挂起), 导致主反应堆死锁、Ctrl+C 无法关闭。
    // SFD_CLOEXEC: 避免 signalfd 被 exec 的子进程继承。
    shutdownCtx.sfd = signalfd(-1, &sigMask, SFD_NONBLOCK | SFD_CLOEXEC);
    shutdownCtx.sigChannel = NULL;
    shutdownCtx.shuttingDown = false;
    if (shutdownCtx.sfd == -1) {
        perror("signalfd");   // 极少见; 失败时信号仍被阻塞, 仅是无法优雅关闭, 服务照常运行
    } else {
        shutdownCtx.sigChannel = new Channel(shutdownCtx.sfd, READ_EVENT,
                                            onSignalHandler, NULL, NULL, &shutdownCtx);
        mainLoop->addChannel(shutdownCtx.sigChannel);
    }

    // 4) 启动主反应堆循环 (接受连接)
    printf("server2 启动: port=%d, 模式=", port);
    if (dispatcherType == SELECT_DISP) printf("select");
    else if (dispatcherType == POLL_DISP) printf("poll");
    else printf("epoll");
    printf(", 子反应堆线程数=%d\n", subReactorNum);

    mainLoop->run();

    // 主反应堆已退出(收到信号并完成优雅关闭) -> 清理资源。
    // 此时所有子线程已被 pool->stopAll() join, 无并发访问, 删除是安全的。
    printf("server2 已优雅退出\n");
    if (shutdownCtx.sigChannel) delete shutdownCtx.sigChannel;
    if (shutdownCtx.sfd != -1) close(shutdownCtx.sfd);
    delete acceptor;
    delete pool;
    delete mainLoop;
    close(lfd);
    return 0;
}
