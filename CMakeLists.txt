cmake_minimum_required(VERSION 3.10)
project(cx-mq C)

# 设置C标准
set(CMAKE_C_STANDARD 99)

# 包含头文件目录
include_directories(src/base src/config)

# 收集源文件
file(GLOB BASE_SRC src/base/*.c)
file(GLOB CONFIG_SRC src/config/*.c)

# 主程序入口
add_executable(cx-mq src/main.c ${BASE_SRC} ${CONFIG_SRC})

# 客户端C
add_executable(client_c src/client/client_c.c ${BASE_SRC} ${CONFIG_SRC})

# 客户端P
add_executable(client_p src/client/client_p.c ${BASE_SRC} ${CONFIG_SRC})

# 可选：添加编译选项
# target_compile_options(cx-mq PRIVATE -Wall -Wextra)
