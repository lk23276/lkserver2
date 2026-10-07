# server2 —— 基于 Reactor 模型的高并发 HTTP 静态服务器（C++）

> A high-concurrency static HTTP server built with C++ and the Reactor pattern (Main-Reactor / Sub-Reactor, one-loop-per-thread).
>
> 本项目是作者系统学习 C++ 与 Linux 网络编程的实战工程。代码以源码实证 + 工程化改造的方式，覆盖了从「能跑」到「生产可用」的多项关键优化。

---

## 一、项目简介

`server2` 在 `SImpleHttp`（select/poll/epoll 三态单反应堆原型）基础上重构而来，采用**面向对象封装**，实现了：

- **主从 Reactor（多线程 Reactor）**：1 个主反应堆负责 `accept` 新连接，N 个子反应堆（默认 4 个）负责已连接套接字的读写，通过 `getNextLoop()` 轮询做负载均衡。
- **三种 I/O 多路转接可切换**：`select` / `poll` / `epoll`（默认 `epoll`，Linux 最高效），统一抽象为 `Dispatcher` 接口。
- **静态资源服务**：支持文件（`sendFile`）、目录列表（`sendDir`）、HTTP 响应头构造（`seadHeadM`）、URL 解码、MIME 类型识别。

---

## 二、架构与核心机制

```
                ┌─────────────────────────────────────────────┐
  客户端连接 ──► │  MainReactor (主反应堆)                       │
                │   └─ Acceptor::acceptC 接受连接              │
                │        │  getNextLoop() 轮询分发              │
                │        ▼                                     │
                │  SubReactor-0  SubReactor-1  ...  SubReactor-3│
                │   (one loop per thread，各跑一个 EventLoop)   │
                └─────────────────────────────────────────────┘
```

| 机制           | 实现位置                                                      | 说明                                                                 |
| ------------ | --------------------------------------------------------- | ------------------------------------------------------------------ |
| 主从 Reactor   | `Acceptor::acceptC` → `getNextLoop` → `new TcpConnection` | 单主线程 accept，规避惊群；内部轮询分发                                            |
| eventfd 精确唤醒 | `EventLoop::wakeup` / `handleWakeup`                      | 跨线程加入新连接 / 优雅关闭时 `write(eventfd,1)` 立即唤醒，消除原 2 秒 `epoll_wait` 超时延迟 |
| 写半包续发        | `TcpConnection::send` / `handleWrite` + `Buffer`          | 应用层输出缓冲 + `EPOLLOUT`，部分写缓存续发；发完即注销 `EPOLLOUT` 防忙等                  |
| SIGPIPE 防护   | `send(..., MSG_NOSIGNAL)`                                 | 对端断开时返回 `-1/EPIPE` 而非被杀进程                                          |
| 优雅关闭         | `signalfd` + `onSignalHandler`                            | `SIGINT/SIGTERM` → 停 accept → 停子反应堆 `join` → 退主循环，安全退出             |
| 安全格式化        | `snprintf` / `sscanf` 宽度限定                                | 防缓冲区溢出（原 `sprintf` 已替换）                                            |

---

## 三、目录结构

```
server2/
├── main.cpp          # 入口：初始化、信号掩码、signalfd 注册、启动主反应堆
├── server2.h         # 类声明：Buffer / Channel / Dispatcher / EventLoop /
│                     #   EventLoopThread / EventLoopThreadPool / TcpConnection / Acceptor
├── server2.cpp       # 实现 + 工具函数（parseRL / sendFile / seadHeadM / sendDir 等）
├── Makefile          # Linux 下编译（g++ + -pthread）
└── server2.vcxproj   # Visual Studio 工程（连接 Linux 虚拟机远程编译用）
```

---

## 四、构建与运行

### 方式一：Linux 命令行（Makefile）

```bash
# 在含有源码的目录下
make                # 生成可执行文件 server2
./server2 607 ./    # 监听 607 端口，以当前目录作为 Web 根目录
# 可选第三个参数指定多路转接机制：
./server2 607 ./ epoll     # 默认
./server2 607 ./ poll
./server2 607 ./ select
```

- 端口：第一个参数（示例 `607`）。
- 根目录：第二个参数（示例 `./`，server2 会 `chdir` 到这里）。
- 子反应堆线程数：源码 `main.cpp` 中 `subReactorNum`（默认 4），可按 CPU 核数调整。

### 方式二：Visual Studio 远程 Linux

用 Visual Studio 打开 `server2.vcxproj` / 解决方案，连接到 Linux 虚拟机后远程编译运行（工程已定义 `_GNU_SOURCE`，`eventfd`/`signalfd` 等 GNU 扩展声明可见；需在工程属性中显式链接 `pthread`）。

### 压测示例

```bash
ab -n 10000 -c 100 http://192.168.144.68:607/
# 或用 wrk 观察 QPS / p99 / 错误率
```

---

## 五、优雅关闭

启动后在终端按 `Ctrl+C`（或 `kill -TERM <pid>`），服务端会：

1. 从主反应堆移除监听通道，停止接受新连接；
2. 通知所有子反应堆退出并 `pthread_join` 回收线程；
3. 主反应堆退出，统一清理资源后打印 `server2 已优雅退出`。

（基于 `signalfd` 在事件循环上下文处理信号，避免在 async-signal-unsafe 的 signal handler 里直接操作 epoll / 加锁。）

---

## 六、已知遗留与可升级点（TODO）

以下问题在工程中已知存在，作为后续优化方向（面试可表述为「我还能怎么升级这个项目」）：

- [ ] `m_isQuit` 改为 `std::atomic<bool>`，消除跨线程读写的数据竞争；
- [ ] `recHttpR` 接收缓冲固定 4096 字节，超长请求会被截断 → 改用动态 `Buffer`；
- [ ] `sendDir` 中 `stat` 返回值未检查；
- [ ] 404 / 目录响应使用 `content-length: -1`（畸形 HTTP）→ 改用 `chunked` 编码或动态计算长度；
- [ ] `sendFile` 整文件读入内存，大文件有 OOM 风险 → 结合 `EPOLLOUT` 背压的零拷贝 `sendfile`；
- [ ] 优雅关闭时显式清理所有活跃 `TcpConnection`。

---

## 七、许可证

本工程为学习用途，可自由参考与修改。
