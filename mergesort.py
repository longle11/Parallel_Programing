import concurrent.futures
import numpy as np
import time
import threading
def mergesort_algorithm(arr):
    if len(arr) <= 1:
        return arr
    
    mid = len(arr) // 2
    left_seq = arr[:mid]
    right_seq = arr[mid:]

    left_result = mergesort_algorithm(left_seq)
    right_result = mergesort_algorithm(right_seq)

    result = arr.copy()
    parallel_merge(left_result, 0, right_result, 0, result)

    return result

def binary_search(arr, value):
    if value <= arr[0]:
        return 0
    for index in range(len(arr) - 1):
        if arr[index] <= value and arr[index + 1] > value:
            return index + 1
    if value >= arr[len(arr) - 1]: return len(arr)
def parallel_merge(left_arr, index_left, right_arr, index_right, result):
    if len(left_arr) == 0 and len(right_arr) == 0:
        return
    elif len(left_arr) == 0 and len(right_arr) != 0:
        for index in range(len(right_arr)):
            result[index_left + index_right + index] = right_arr[index]

    elif len(left_arr) != 0 and len(right_arr) == 0:
        for index in range(len(left_arr)):
            result[index_left + index_right + index] = left_arr[index]
    else:
        mid_left = len(left_arr) // 2
        pos_search = binary_search(right_arr, left_arr[mid_left])

        result[mid_left + pos_search + index_left + index_right] = left_arr[mid_left]

        with concurrent.futures.ThreadPoolExecutor() as excutor:
            res_left = excutor.submit(parallel_merge, left_arr[:mid_left], index_left, right_arr[:pos_search], index_right, result)
            res_right = excutor.submit(parallel_merge, left_arr[mid_left + 1:], index_left + 1 + mid_left, right_arr[pos_search:], pos_search + index_right, result)

            res_left.result()
            res_right.result()
if __name__ == "__main__":
    array = np.random.randint(-1000, 1000, 1000)
    start = time.time()
    new_arr = mergesort_algorithm(array)
    end = time.time()
    if(np.array_equal(np.sort(array), new_arr)):
        print("2 mảng bằng nhau")
    else:
        print("2 mảng không bằng nhau")
    print("sort hết " + str(end - start) + " kết quả: " + str(new_arr))
    print("Mảng sort: " + str(np.sort(array)))
