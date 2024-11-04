import multiprocessing
from functools import partial
import numpy as np
import pandas as pd


def long_process_on_data_frame(data_frame, result, *args, **kwargs):
    data_frame = data_frame ## Here can be some long process and calculation

    ret = data_frame # This is what function can return

    result.append(ret) # Adding the to result obj 
    # This function can alos return something if you want
    


def multi_process_long_process_on_data_frame(data_frame):    
    manager = multiprocessing.Manager() # Setup a manager for capturing the result as a data structure
    return_list = manager.list()

    num_of_processes = multiprocessing.cpu_count() ## Get count of CPU cores
    data_split = np.array_split(data_frame, num_of_processes) ## Split Data Frame per CPU core

    processed = []
    for data in data_split:
        parameterize_long_process_on_data_frame = partial(long_process_on_data_frame, **{'data_frame' : data, 'result' : return_list}) ## create an instance of a function with inputting parameters
        process_ins = multiprocessing.Process(target=parameterize_long_process_on_data_frame)
        processed.append(process_ins)
        process_ins.start() ## Start a Subprocess

    for proc in processed:
        proc.join() ## Wait For all process to end
    
    result = pd.concat(return_list) ## Get and combine the results
    return result


""" Second Approach (Using Processor Pool)"""


def multi_process_with_processor_pool(data_frame):
    num_of_processes = multiprocessing.cpu_count()

    with multiprocessing.Pool(processes=num_of_processes) as pool:
        splited_data = np.array_split(data_frame, num_of_processes) ## Split Data Frame per CPU core

        data = pd.concat(pool.map(long_process_on_data_frame, splited_data)) ## Map the function on a data frame split
        pool.close()
        pool.join() ## Wait For all process to end
        return data
