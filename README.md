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
| 写半包续发        | `TcpConnection::send` / `handleWrite` + `Buffer`          | 响应头/目录等应用层输出缓冲 + `EPOLLOUT`，部分写缓存续发；发完即注销 `EPOLLOUT` 防忙等            |
| 零拷贝文件发送      | `TcpConnection::beginSendFile` / `doSendFile`（`sendfile`） | 文件由内核直接从页缓存送入 socket，不经用户态缓冲；`EAGAIN` 时武装 `EPOLLOUT` 续发，不自旋/不阻塞、不占内存 |
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

### 构建

**方式一：Linux 命令行（Makefile）**

```bash
make        # 生成可执行文件
```

**方式二：Visual Studio 远程 Linux**

用 Visual Studio 打开 `server2.vcxproj` / 解决方案，连接 Linux 虚拟机后远程编译（工程已定义 `_GNU_SOURCE`，`eventfd` / `signalfd` 等 GNU 扩展声明可见；需在工程属性中显式链接 `pthread`）。

### 运行

```bash
./server2 <端口号> <要展示的目录>
```

- **第一个参数**：**端口号**，服务监听的端口。
- **第二个参数**：**要展示的目录**，即 Web 根目录。服务启动后会切换到（`chdir`）该目录；**网页中展示的就是该目录中的内容**——访问目录会列出目录内容，访问具体文件则返回该文件。
- **注意**：该目录下**必须包含 `404.html`**，用于在请求的资源不存在时返回自定义的 404 页面。

### 压测示例

```bash
ab -n <请求数> -c <并发数> http://<服务器IP>:<端口>/
# 或用 wrk 观察 QPS / p99 / 错误率
```

### 运行环境

本项目在上述 Linux 虚拟机上编译、运行与验证：

| 项目 | 配置 |
| ---- | ---- |
| 操作系统 | Linux **CentOS-Stream-10** |
| 内存 | Memory 2GB |
| 处理器 | Processors 1 |
| 硬盘 | Hard Disk (SCSI) 50GB |

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
- [x] 404 / 目录响应改用 `Connection: close`（原 `content-length: -1` 畸形头已移除），正文发完后关闭连接；
- [x] `sendFile` 改为零拷贝 `sendfile` + `EPOLLOUT` 续发（不再整文件读入内存，消除大文件 OOM 风险）；
- [ ] 优雅关闭时显式清理所有活跃 `TcpConnection`。

---

## 七、许可证

本工程为学习用途，可自由参考与修改。
