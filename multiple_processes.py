
# %%
from multiprocessing import Pool
import time
import pandas as pd
import numpy as np
from tqdm import tqdm


def isin_function(inputs):
    (i_1, i_2) = inputs
    result = i_1.isin(i_2)
    
    return result


def parallel_job(inputs, num_processes):
    pool = Pool(num_processes)
    pool_outputs = pool.map_async(isin_function, inputs)

    # close 和 join 是確保主程序結束後，子程序仍然繼續進行
    pool.close()
    pool.join()

    return(pool_outputs.get())


def parallel_isin(data, condition, slice_num, num_processes):
    slice_size = int(data.shape[0] / slice_num)
    data_slice = [pd.DataFrame(x) for x in data.values.reshape(-1, slice_size)]
    condition_list = [condition] * int(slice_num)
    inputs = [(x, y) for (x, y) in zip(data_slice, condition_list)]
    p_result = parallel_job(inputs, num_processes)
    outcome_df = pd.concat(p_result)

    return outcome_df


def find_num_slice_processes(data, condition):
    slice_num_list = [10, 100, 1000, 10000]
    num_processes_list = [2, 4, 6, 8, 10]
    exp_outcome = []
    for num_processes in tqdm(num_processes_list):
        for slice_num in slice_num_list:
            start_time = time.time()
            slice_size = int(data.shape[0] / slice_num)
            data_slice = [pd.DataFrame(x) for x in data.values.reshape(-1, slice_size)]
            condition_list = [condition] * int(slice_num)
            inputs = [(x, y) for (x, y) in zip(data_slice, condition_list)]
            p_result = parallel_job(inputs, num_processes)
            outcome_df = pd.concat(p_result)

            exp_outcome.append([num_processes, slice_num, time.time()-start_time])
    
    exp_outcome_df = pd.DataFrame(data=exp_outcome, columns=['num. processes', 'num. slice', 'elapsed time'])

    return exp_outcome_df


# %%
if __name__ == '__main__':
    data = pd.DataFrame(np.arange(1e8))
    condition = list(np.arange(1e3))

    start_time = time.time()
    outcome_pl = parallel_isin(
        data,
        condition,
        slice_num=1000, 
        num_processes=6)  
    print('Parallel: {}'.format(time.time() - start_time))

    start_time = time.time()
    outcome_pd = data.isin(condition)
    print('Pandas isin: {}'.format(time.time() - start_time))

    exp_outcome = find_num_slice_processes(data, condition)
    print('\n\nOutcome:\n{}'.format(exp_outcome))
