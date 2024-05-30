import numpy as np
import multiprocessing
import concurrent.futures
import random

import threading
import time
# mergesort tuan tu
def mergesort(matrix_a):
    def merge(l_arr, r_arr):
        index_left, index_right, index_jump = 0, 0, 0
        temp_arr = np.zeros(len(l_arr) + len(r_arr))
        while index_left < len(l_arr) and (index_right < len(r_arr)): 
            if l_arr[index_left] < r_arr[index_right]:
                temp_arr[index_jump] = l_arr[index_left]
                index_left += 1
            else:
                temp_arr[index_jump] = r_arr[index_right]
                index_right += 1
            index_jump += 1
        while index_left < len(l_arr):
            temp_arr[index_jump] = l_arr[index_left]
            index_jump += 1
            index_left += 1
        while index_right < len(r_arr):
            temp_arr[index_jump] = r_arr[index_right]
            index_jump += 1
            index_right += 1
        return temp_arr
    #tien hanh phan chia mang thanh 2 phan
    if len(matrix_a) <= 1:
        return matrix_a
    mid = len(matrix_a) // 2
    l_arr = mergesort(matrix_a[:mid])
    r_arr = mergesort(matrix_a[mid:])
    return merge(l_arr, r_arr)


def merge(args):
    left_arr, right_arr = args
    index_left, index_right = 0, 0
    merged_arr = []
    while index_left < len(left_arr) and index_right < len(right_arr):
        if left_arr[index_left] <= right_arr[index_right]:
            merged_arr.append(left_arr[index_left])
            index_left+=1
        else: 
            merged_arr.append(right_arr[index_right])
            index_right+=1
    while index_left < len(left_arr):
        merged_arr.append(left_arr[index_left])
        index_left+=1
    while index_right < len(right_arr):
        merged_arr.append(right_arr[index_right])
        index_right+=1
    return merged_arr
def merge_sort(matrix_a):
    if len(matrix_a) <= 1:  return matrix_a
    mid = len(matrix_a) // 2
    left_arr = merge_sort(matrix_a[:mid])
    right_arr = merge_sort(matrix_a[mid:])
    return merge((left_arr, right_arr))
def parallel_mergesort(matrix_a):
    #chia thanh nhieu mang con vay tien hanh sort song song
    max_cpus = multiprocessing.cpu_count()  #lay ra so luong cpu toi da
    devide_subarrs = np.array_split(matrix_a, max_cpus)
    pool = multiprocessing.Pool(processes=max_cpus)
    with concurrent.futures.ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as pool:
        result = list(pool.map(merge_sort, devide_subarrs))

    remained_arr = None
    sub_arrs = result
    while(1):
        if len(sub_arrs) == 1: break
        remained_arr = sub_arrs.pop() if len(sub_arrs) % 2 == 1 else None 
        couple_arrs = [(sub_arrs[i], sub_arrs[i+1]) for i in range(0, len(sub_arrs), 2)]
        with concurrent.futures.ProcessPoolExecutor(max_workers=len(sub_arrs) // 2) as pool:
            sub_arrs = list(pool.map(merge, couple_arrs))
    return merge((sub_arrs, remained_arr)) if remained_arr != None else sub_arrs[0]
    


def quicksort(args):
    matrix_a, left, right = args
    if left >= right:
        return matrix_a

    # Sử dụng pivot ngẫu nhiên
    pivot_index = random.randint(left, right)
    matrix_a[pivot_index], matrix_a[right] = matrix_a[right], matrix_a[pivot_index]
    pivot = matrix_a[right]

    id_left = left
    id_right = right - 1

    while True:
        while id_left <= id_right and matrix_a[id_left] < pivot:
            id_left += 1
        while id_right >= id_left and matrix_a[id_right] > pivot:
            id_right -= 1
        if id_left > id_right:
            break
        matrix_a[id_left], matrix_a[id_right] = matrix_a[id_right], matrix_a[id_left]
        id_left += 1
        id_right -= 1

    matrix_a[id_left], matrix_a[right] = matrix_a[right], matrix_a[id_left]

    if left < id_left - 1:
        quicksort((matrix_a, left, id_left - 1))
    if id_left + 1 < right:
        quicksort((matrix_a, id_left + 1, right))

    return matrix_a


def devide_arr(args):
    start = time.time()
    matrix_a, pivot1, pivot2 = args
    less_than_pivot1 = []
    between_pivots = []
    greater_than_pivot2 = []

    matrix_a = np.array(matrix_a) 

    less_than_pivot1 = matrix_a[matrix_a < pivot1].tolist()  
    between_pivots = matrix_a[(pivot1 <= matrix_a) & (matrix_a <= pivot2)].tolist()  
    greater_than_pivot2 = matrix_a[matrix_a > pivot2].tolist()  

    return [less_than_pivot1, between_pivots, greater_than_pivot2]

def parallel_quicksort(matrix_a):
    if len(matrix_a) <= 1000000:
        return quicksort((matrix_a, 0, len(matrix_a) - 1))

    max_cpus = multiprocessing.cpu_count()
    sub_arrs = np.array_split(matrix_a, max_cpus)

    start = time.time()
    #chọn ra các phần tử ngẫu nhiên trong process
    selected_sample = []
    for sub_arr in sub_arrs:
        selected_elements = random.sample(list(sub_arr), 2)
        selected_sample = np.concatenate((selected_sample, selected_elements))
        

    sort_sample = quicksort((selected_sample, 0, len(selected_sample) - 1))
    select_pivot1 = sort_sample[(len(sort_sample)//2)-1]
    select_pivot2 = sort_sample[(len(sort_sample)//2)+1]
    with concurrent.futures.ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as pool:
        futures = [pool.submit(devide_arr, (matrix, select_pivot1, select_pivot2)) for matrix in sub_arrs]
        results = []
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
    
    left_arr = []
    right_arr = []
    middle_arr = []
    for arr in results:
        left_arr = np.concatenate((left_arr, arr[0]))
        middle_arr = np.concatenate((middle_arr, arr[1]))
        right_arr = np.concatenate((right_arr, arr[2]))

    with concurrent.futures.ProcessPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(parallel_quicksort, [left_arr, middle_arr, right_arr]))

    return np.concatenate((results[0], results[1], results[2]))


if __name__ == "__main__":
    start = time.time()
    matrix_a = np.random.randint(-105000, 250000, 5000000)
    # matrix = quicksort((matrix_a, 0, len(matrix_a) - 1))
    matrix = parallel_quicksort(matrix_a)
    end = time.time()
    print("Thoi gian: " + str(end - start))
    if sorted(matrix_a) == list(matrix):
        print("2 ma tran bang nhau")
    else: print("2 ma tran khong bang nhau")

