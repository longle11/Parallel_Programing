
import threading as th
from numba import njit, prange
import time 
import multiprocessing
import numpy as np
import concurrent.futures

def cal_cell(args):
    current_row, numbers_of_cols, matrix_a, matrix_b, matrix_temp = args
    for current_col in numbers_of_cols:
        matrix_temp[current_row, current_col] = sum(matrix_a[current_row, :] * matrix_b[:, current_col])
def cal_value(current_row, matrix_a, matrix_b, matrix_temp):
    with concurrent.futures.ThreadPoolExecutor() as pool:
        pool.map(cal_cell, [(current_row, numbers_of_cols, matrix_a, matrix_b, matrix_temp) for numbers_of_cols in np.array_split(range(len(matrix_a)), multiprocessing.cpu_count())])
def cal_sub_matrices(args):
    chunk, matrix_a, matrix_b = args
    matrix_temp = np.zeros((len(matrix_a), len(matrix_a)))
    with concurrent.futures.ThreadPoolExecutor() as pool:
        pool.map(lambda current_row: cal_value(current_row, matrix_a, matrix_b, matrix_temp), chunk)  #lay ra cac dong tuong ung
    return {chunk[0]: matrix_temp[chunk[0]:chunk[0] + len(chunk),:]}
def parallel_multiply_matrices(matrix_a, matrix_b):
    n = len(matrix_a)
    arrs = np.zeros((n,n))
    with concurrent.futures.ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as pool:
        results = list(pool.map(cal_sub_matrices, [(chunk, matrix_a, matrix_b) for chunk in np.array_split(range(n), multiprocessing.cpu_count())]))
    for result in results:
        keys = result.keys()
        for key in keys:
            arrs[int(key):int(key)+len(result[key]),:] = result[key]
    return arrs

if __name__ == "__main__":
    n=1000
    A = np.random.randint(1,10,size=(n,n)).astype(np.int32)
    B = np.random.randint(1,10,size=(n,n)).astype(np.int32)
    start = time.time()
    res = parallel_multiply_matrices(A, B)

    end = time.time()
    print(res)
    print(f"Thời gian tốn: {end - start}")
    print()
    res1 = np.dot(A, B)
    print(np.dot(A, B))
    if (res == res1).all(): print("2 mảng bằng nhau")
    else: print("2 mảng không bằng nhau")
