# ============================================================================
# Makefile for server2 高并发 HTTP 服务器
# 平台要求 : Linux / WSL (依赖 epoll / eventfd 等 Linux 专有接口, Windows 无法编译)
# 用法     : make        构建可执行文件 server2
#            make clean  清理构建产物 (.o 与可执行文件)
#            make run    构建并在后台启动 (端口 607, 日志写 server2.log)
# ============================================================================

CXX      := g++
CXXFLAGS := -std=c++11 -Wall -Wextra -pthread -O2
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

# 运行: 构建后在后台启动, 标准输出/错误重定向到 server2.log
run: $(TARGET)
	./$(TARGET) > server2.log 2>&1 &

# 清理生成的文件
clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean run
