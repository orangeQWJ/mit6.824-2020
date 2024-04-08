#!/bin/bash

# 初始化计数器
success_count=0
fail_count=0

# 循环运行程序20次
for i in {1..1000}; do
    echo "运行次数: $i"

    # 运行程序并捕获输出
    output=$(go test -run 2B  2>&1)
    exit_code=$?

    # 检查退出码
    if [ $exit_code -eq 0 ]; then
        ((success_count++))
    else
        ((fail_count++))
        echo "失败 $fail_count: $output"
    fi
done

# 打印结果
echo "成功次数: $success_count"
echo "失败次数: $fail_count"


