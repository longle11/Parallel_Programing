import concurrent.futures
import numpy as np
import time
import multiprocessing

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

def prefixsum_algorithm(arrs, sizeOfArrs):
    if sizeOfArrs == 1:
        return arrs
    else:
        sum_couple_arrs = np.full(sizeOfArrs // 2, 0)
        start = time.time()
        tuple_arrs = []
        if(len(sum_couple_arrs <= multiprocessing.cpu_count())):
            devide_sum_couple = np.array_split(list(range(len(sum_couple_arrs))), multiprocessing.cpu_count())
            for index in range(len(devide_sum_couple)):
                tuple_arrs.append((devide_sum_couple[index], arrs, sum_couple_arrs))
        else:
            tuple_arrs.append((list(range(len(sum_couple_arrs))), arrs, sum_couple_arrs))
        with concurrent.futures.ThreadPoolExecutor() as excutor:
            results = list(excutor.map(sum_couple, tuple_arrs))
        end = time.time()
        new_res_arrs = prefixsum_algorithm(sum_couple_arrs, len(sum_couple_arrs))
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

def array_after_partition(arrs, arr_result, new_arrs, check, fix_pos):
    store_value = 0
    if arr_result[0] == 1:
        if check: new_arrs[fix_pos] = arrs[0]
        else: new_arrs[0] = arrs[0]
        store_value = 1
    for index in range(1, len(arr_result)):
        if(store_value != arr_result[index]):
            store_value = arr_result[index]
            if check:
                new_arrs[fix_pos + arr_result[index] - 1] = arrs[index]
            else:
                new_arrs[arr_result[index] - 1] = arrs[index]
def binary_search(args):
    pivot = 0
    arr_index, flag_greater_equal_mid, flag_smaller_mid, arrs = args
    for index in arr_index:
        if(arrs[index] >= arrs[pivot]): 
            flag_greater_equal_mid[index] = 1
            flag_smaller_mid[index] = 0
        else: 
            flag_greater_equal_mid[index] = 0
            flag_smaller_mid[index] = 1

def partition_algorithm(arrs):
    flag_greater_equal_mid = arrs.copy()
    flag_smaller_mid = arrs.copy()
    devide_arr = np.array_split(list(range(len(arrs))), multiprocessing.cpu_count())
    tuple_arrs = []
    for index in range(len(devide_arr)):
        tuple_arrs.append((devide_arr[index], flag_greater_equal_mid, flag_smaller_mid, arrs))
    
    with concurrent.futures.ThreadPoolExecutor() as excutor:
        excutor.map(binary_search, tuple_arrs)
    
    tuple_arrs = []
    with concurrent.futures.ThreadPoolExecutor() as excutor:
        # chứa mảng flag có giá trị lớn hơn hoặc bằng giá trị phần tử ở giữa
        result_1 = excutor.submit(prefixsum_algorithm, flag_greater_equal_mid, len(flag_greater_equal_mid))
        # chứa mảng flag có giá trị nhỏ hơn giá trị ở giữa
        result_2 = excutor.submit(prefixsum_algorithm, flag_smaller_mid, len(flag_smaller_mid))

        getResult1 = result_1.result()
        getResult2 = result_2.result()
    fix_pos = getResult2[len(getResult2) - 1]

    new_arrs = arrs.copy()
    with concurrent.futures.ThreadPoolExecutor() as excutor:
        result_1 = excutor.submit(array_after_partition, arrs, getResult1, new_arrs, True, fix_pos)
        result_2 = excutor.submit(array_after_partition, arrs, getResult2, new_arrs, False, fix_pos)
        result_1.result()
        result_2.result()

    for i in range(len(arrs)):
        arrs[i] = new_arrs[i]
    return fix_pos

def quicksort_algorithm(arrs):
    if len(arrs) <= 1:
        return arrs
    index_pivot = partition_algorithm(arrs)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        left_arrs = executor.submit(quicksort_algorithm, arrs[:index_pivot+1])
        right_arrs = executor.submit(quicksort_algorithm, arrs[index_pivot+1:])
        left_res = left_arrs.result()
        right_res = right_arrs.result()

    return left_res + right_res


if __name__ == "__main__":
    # array = np.random.randint(-10000, 10000, 100)
    array=[4,1,2,6]
    start = time.time()
    new_arr = quicksort_algorithm(list(array))
    end = time.time()
    if(np.array_equal(np.sort(array), new_arr)):
        print("2 mảng bằng nhau")
    else:
        print("2 mảng không bằng nhau")
    print("sort hết " + str(end - start) + " kết quả: " + str(new_arr))