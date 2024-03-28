#!/bin/bash
# 解决多进程和内存溢出问题

bp_index=0      # 设置初始的 bp_index 值
mini_batch=5000 # 每跑5000条就释放内存
max_num=200000    # 一个csv文件20w条数据
num_iterations=$((max_num / mini_batch)) # 20w/5k=40

file_name="process_2_ocr.py"

log_name="process_2_ocr.log"

# 循环执行 a.py
for ((i = 0; i < num_iterations; i++)); do
  echo "mini_batch: $i"

  # 生成并行进程的参数
  for ((j = 5; j <= 6; j++)); do
    batch_num=$((j))
    log_name="process2ocr_$j.log"

    # 并行执行进程
    nohup python "$file_name" "--bp_index=$bp_index" "--mini_batch=$mini_batch" "--batch_num=$batch_num" >>"$log_name" 2>&1 &
  done
  wait

  # 更新 bp_index 值
  bp_index=$((bp_index + mini_batch))
  echo "Final_index: $bp_index"

  # 判断是否超过最大循环次数，如果是则退出循环
  if ((bp_index >= max_num)); then
    break
  fi
done

#bash run.sh > runsh.log 2>&1 &