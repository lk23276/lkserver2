# ============================================================================
# Makefile for server2 高并发 HTTP 服务器
# 平台要求 : Linux / WSL (依赖 epoll / eventfd 等 Linux 专有接口, Windows 无法编译)
# 用法     : make        构建可执行文件 server2
#            make clean  清理构建产物 (.o 与可执行文件)
#            make run    PORT=<端口号> ROOT=<要展示的目录>   前台启动 (可直接按 Ctrl+C 优雅关闭)
#            make start  PORT=<端口号> ROOT=<要展示的目录>   后台启动 (日志 server2.log, PID 写 server2.pid)
#            make stop   向进程发 SIGINT, 走 signalfd 优雅关闭链路
# ============================================================================

CXX      := g++
CXXFLAGS := -std=c++11 -Wall -Wextra -pthread -O2 -D_GNU_SOURCE
TARGET   := server2
SRCS     := server2.cpp main.cpp
OBJS     := $(SRCS:.cpp=.o)
DEPS     := server2.h

# 默认目标
all: $(TARGET)

# 链接: 把目标文件链成可执行文件
$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^

# 编译: 每个 .cpp 生成 .o, 依赖头文件 (头文件改动会触发重编)
%.o: %.cpp $(DEPS)
	$(CXX) $(CXXFLAGS) -c $< -o $@

# 运行参数: 由使用者通过命令行传入, 避免在仓库中硬编码任何具体端口 / 路径
PORT ?=
ROOT ?=

# 运行(前台): 构建后在终端前台启动, 便于直接按 Ctrl+C 触发 signalfd 优雅关闭
# 用法: make run PORT=<端口号> ROOT=<要展示的目录>
run: $(TARGET)
	@[ -n "$(PORT)" ] || { echo "用法: make run PORT=<端口号> ROOT=<要展示的目录>"; exit 1; }
	@[ -n "$(ROOT)" ] || { echo "用法: make run PORT=<端口号> ROOT=<要展示的目录>"; exit 1; }
	./$(TARGET) $(PORT) $(ROOT)

# 运行(后台): 构建后在后台启动, 日志写 server2.log, 进程 PID 写入 server2.pid
# 用法: make start PORT=<端口号> ROOT=<要展示的目录>
start: $(TARGET)
	@[ -n "$(PORT)" ] || { echo "用法: make start PORT=<端口号> ROOT=<要展示的目录>"; exit 1; }
	@[ -n "$(ROOT)" ] || { echo "用法: make start PORT=<端口号> ROOT=<要展示的目录>"; exit 1; }
	./$(TARGET) $(PORT) $(ROOT) > server2.log 2>&1 & echo $$! > server2.pid

# 停止: 向进程发 SIGINT(-INT), 走 signalfd 优雅关闭链路
#       优先读取 server2.pid; 若没有 pid 文件(如前台 run 启动), 则按进程名查找
stop:
	@if [ -f server2.pid ]; then \
		kill -INT `cat server2.pid` && rm -f server2.pid; \
	else \
		pkill -INT server2 || echo "未找到运行中的 server2"; \
	fi

# 清理生成的文件
clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean run start stop
