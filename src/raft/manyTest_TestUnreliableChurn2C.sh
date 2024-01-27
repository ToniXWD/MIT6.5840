#!/bin/bash

# 初始化计数器
count=0
success_count=0
fail_count=0

# 设置测试次数
max_tests=100

for ((i=1; i<=max_tests; i++))
do
    echo "Running test iteration $i of $max_tests..."
    
    # 运行 go 测试命令
    go test -v -run TestUnreliableChurn2C &> outputTestUnreliableChurn2C.log
    # go test -v -race -run TestUnreliableChurn2C &> outputTestUnreliableChurn2C.log
    
    # 检查 go 命令的退出状态
    if [ "$?" -eq 0 ]; then
        # 测试成功
        success_count=$((success_count+1))
        echo "Test iteration $i passed."
        # 如果想保存通过的测试日志，取消下面行的注释
        # mv outputTestUnreliableChurn2C.log "success_UnreliableChurn2C_$i.log"
    else
        # 测试失败
        fail_count=$((fail_count+1))
        echo "Test iteration $i failed, check 'failureTestUnreliableChurn2C_$i.log' for details."
        mv outputTestUnreliableChurn2C.log "failureTestUnreliableChurn2C_$i.log"
    fi
done

# 报告测试结果
echo "Testing completed: $max_tests iterations run."
echo "Successes: $success_count"
echo "Failures: $fail_count"