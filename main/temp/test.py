import argparse
import time
import random

parser = argparse.ArgumentParser()
# linux有内存溢出问题，使用该变量
parser.add_argument("--bp_index", type=int, default=-1)
parser.add_argument("--mini_batch", type=int, default=50)
parser.add_argument("--n", type=int, default=1)
args = parser.parse_args()
bp_index = args.bp_index
mini_batch = args.mini_batch
n = args.n


def main():
    print("进程{}的第{}轮".format(n, bp_index))
    start_time = time.time()
    end_time = start_time + 20
    count = 0
    while time.time() < end_time:
        # 在这里编写你的程序逻辑
        # 这个循环将会持续运行 20 秒
        if count > mini_batch:
            break
        # 示例：每秒打印一次当前时间
        current_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print("当前时间：", current_time)

        # 增加适当的延迟，以控制循环速度
        sleep_time = random.uniform(1, 2)
        time.sleep(sleep_time)
        count += 1


if __name__ == "__main__":
    print("{},{},{}".format(bp_index, mini_batch, n))
    main()
