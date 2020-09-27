## 匯入套件
import pandas as pd
import numpy as np
import time
from multiple_processes import (
    find_num_slice_processes,
    parallel_isin)

## 產生資料
# data 大小為一千萬，condition 大小為一千
# 目標要做出 data.isin(condition)
data = pd.DataFrame(np.arange(1e8))
condition = list(np.arange(1e3))

## data.insin(condition) 
# 單純使用 data.isin(condition)
# 並觀察會花費多久
start_time = time.time()
outcome_pd = data.isin(condition)
print('Elapsed time of DataFrame.isin: {}'.format(time.time() - start_time))

## parallel_isin(data, conditionm slice_num, num_processes)
# 使用 parallel_isin 並觀察花費時間
start_time = time.time()
outcome_pl = parallel_isin(
    data,    # 原始資料
    condition,    # isin 的內容
    slice_num=1000,    # 要將原始資料切成幾份下去平行跑
    num_processes=6)    # 要使用幾個核心來跑
print('Elapsed time of parallel_isin: {}'.format(time.time() - start_time))

## 尋找較佳的切割大小與程序數量
# 使用 find_num_slice_preocesses(data)
exp_outcome = find_num_slice_processes(data, condition)
print('\n\nOutcome:\n{}'.format(exp_outcome))
