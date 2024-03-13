#!/bin/bash
# 解决多进程和内存溢出问题

bp_index=0      # 设置初始的 bp_index 值
mini_batch=5000 # 每跑5000条就释放内存

max_num=200000    # 一个csv文件20w条数据
num_iterations=40 # 10w/5k=20
file_name="process_2_ocr.py"

# 循环执行 a.py
for i in $(seq 0 $num_iterations); do
  echo "mini_batch: $i"
  pids=()
  {
    # 执行 a.py 并传递 bp_index 参数
    batch_num=0 # 第几个20w文件
    log_name="process_2_ocr_$batch_num.log"
    nohup python "$file_name" --bp_index=$bp_index --mini_batch=$mini_batch --batch_num=$batch_num >>"$log_name" 2>&1
    # 获取后台任务的进程ID，并将其存储到数组中
    pids+=($!)
  }
  # 等待每个后台任务完成
  for pid in "${pids[@]}"; do
    wait "$pid"
    # 检查后台任务的退出状态
    if [ $? -eq 0 ]; then
      echo "代码块执行完成"
    else
      echo "代码块执行失败"
      break
    fi
  done

  # 将 bp_index 的值加1
  ((bp_index += mini_batch))

  # 判断 bp_index 是否超过 1000，如果是则停止循环
  if ((bp_index >= max_num)); then
    break
  fi
  echo "Final_index : $bp_index"
done

#bash run.sh > runsh.log 2>&1 &