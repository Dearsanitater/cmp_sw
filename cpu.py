#cpu亲和学习
import os
import threading
import multiprocessing
 
def bind_to_core(core_id):
    """将当前进程绑定到指定CPU核心（Linux专用）"""
    # 获取当前进程ID
    pid = os.getpid()
    # 设置CPU亲和性：仅允许在core_id对应的核心上运行
    # mask是二进制掩码，core_id=0对应0b1，core_id=1对应0b10，以此类推
    mask = 1 << core_id
    os.sched_setaffinity(pid, {core_id})  # 也可以传集合{0,1}表示绑定到多个核心
    print(f"进程 {pid} 已绑定到CPU核心 {core_id}")
 
def worker():
    """示例工作函数"""
    bind_to_core(0)  # 绑定到第0个CPU核心
    while True:
        # 模拟耗时任务（比如你的强化学习训练）
        pass
 
if __name__ == '__main__':
    # 方法A：绑定主进程
    bind_to_core(1)  # 主进程绑定到核心1
    
    # 方法B：绑定子进程
    p = multiprocessing.Process(target=worker)
    p.start()
    
    # 方法C：绑定线程（注意：Python线程受GIL限制，核绑定效果有限）
    t = threading.Thread(target=worker)
    t.start()