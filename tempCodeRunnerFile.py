import concurrent.futures
import numpy as np
import multiprocessing
import time
import math
import threading 

# hàm tính prefix sum
# def parallel_reduce(A, start, end):
#     if start == end:
#         return A[start]
#     else:
#         mid = (start + end) // 2
#         left_result = parallel_reduce(A, start, mid)
#         right_result = parallel_reduce(A, mid + 1, end)
#         return left_result + right_result
def parallel_reduce(A, start, end):
    sum = 0
    for index in range(start, end + 1):
        sum += A[index]
    return sum
def scan(A, arr_B, s, t, offset):
    if s == t:
        arr_B[s] = offset + A[s]
    else:
        mid = (s + t) // 2
        left_sum = parallel_reduce(A, s, mid)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            left_result = executor.submit(scan, A, arr_B, s, mid, offset)
            right_result = executor.submit(scan, A, arr_B, mid + 1, t, offset + left_sum)

            # Đợi kết quả của từng công việc
            left_result.result()
            right_result.result()
    return arr_B
    
def parallel_prefixSum(matrix_a):  
    arr_B = matrix_a.copy()
    return scan(matrix_a, arr_B, 0, len(matrix_a) - 1, 0)




# prefix sum sử dụng theo cách khác nhưng tương tự cách làm trên
def sum_left_arrs(arrs, start, end):
    sum_arrs = 0
    for i in range(start, end + 1):
        sum_arrs += arrs[i]
    return sum_arrs
def scan1(arrs, res_arrs, start, end, offset):
    if start == end: 
        res_arrs[start] = arrs[start] + offset
    else:
        mid = (start + end) // 2
        sum_left = sum_left_arrs(arrs, start, mid)
        with concurrent.futures.ThreadPoolExecutor() as executor:
            left_result = executor.submit(scan1, arrs, res_arrs, start, mid, offset)
            right_result = executor.submit(scan1, arrs, res_arrs, mid + 1, end, offset + sum_left)
            left_result.result()
            right_result.result()


# hàm prefixsum với 1 thuật toán khác
def sum_couple(args):
    arr_index, arrs, couple_arrs = args
    for i in range(len(arr_index)):
        index = arr_index[i]
        couple_arrs[index] = arrs[2*index] + arrs[2*index + 1]
def assign_value_new_arrs(args):
    old_arrs, old_res_arrs, new_arrs, array_index = args
    for index in range(len(array_index)):
        current_pos = array_index[index]
        if(current_pos != 0):
            if current_pos % 2:
                new_arrs[current_pos] = old_res_arrs[current_pos//2]
            else:
                new_arrs[current_pos] = old_arrs[current_pos] + old_res_arrs[(current_pos//2) - 1]

def prefixSum_smallSize(arrs, sizeOfArrs):
    if sizeOfArrs == 1:
        return arrs
    else:
        sum_couple_arrs = np.full(sizeOfArrs // 2, 0)
        tuple_arrs = []
        if(len(sum_couple_arrs <= multiprocessing.cpu_count())):
            devide_sum_couple = np.array_split(list(range(len(sum_couple_arrs))), multiprocessing.cpu_count())
            for index in range(len(devide_sum_couple)):
                tuple_arrs.append((devide_sum_couple[index], arrs, sum_couple_arrs))
        else:
            tuple_arrs.append((list(range(len(sum_couple_arrs))), arrs, sum_couple_arrs))
        with concurrent.futures.ThreadPoolExecutor() as excutor:
            results = list(excutor.map(sum_couple, tuple_arrs))
        new_res_arrs = prefixSum_smallSize(sum_couple_arrs, len(sum_couple_arrs))
        new_arrs = arrs.copy()
        tuple_arrs = []
        if(len(arrs) <= multiprocessing.cpu_count()):
            devide_arr = np.array_split(list(range(len(arrs))), multiprocessing.cpu_count())
            for index in range(len(devide_arr)):
                tuple_arrs.append((arrs, new_res_arrs, new_arrs, devide_arr[index]))
        else:
            tuple_arrs.append((arrs, new_res_arrs, new_arrs, list(range(len(arrs)))))
        with concurrent.futures.ThreadPoolExecutor() as excutor:
            excutor.map(assign_value_new_arrs, tuple_arrs)
        return new_arrs


if __name__ == "__main__":
    # arrs = np.random.randint(-100, 100, 1000)
    arrs = [1,2,3,4,5,6]
    print("Mang co kich thuoc " + str(len(arrs)) + " có mảng " + str(np.array(arrs)))
    res_arrs = np.full(len(arrs), -999)
    start = time.time()
    arr = parallel_prefixSum(arrs)
    end = time.time()
    if(np.array_equal(arr, np.cumsum(arrs))):
        print("2 mảng bằng nhau mất thời gian " + str(end - start))
    else: 
        print("2 mảng không bằng nhau mất thời gian " + str(end - start))
    print("mảng sau khi in ra " + str(np.array(arr)))