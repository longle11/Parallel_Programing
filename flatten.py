import concurrent.futures
import numpy as np


# def sum_couple(i, arrs):
#     return (i, arrs[2*i] + arrs[2*i + 1])
# def assign_value_new_arrs(old_arrs, old_res_arrs, new_arrs, current_pos):
#     if current_pos % 2:
#         new_arrs[current_pos] = old_res_arrs[current_pos//2]
#     else:
#         new_arrs[current_pos] = old_arrs[current_pos] + old_res_arrs[(current_pos//2) - 1]

# def prefixsum_algorithm(arrs, sizeOfArrs):
#     if sizeOfArrs == 1:
#         return arrs
#     else:
#         sum_couple_arrs = np.full(sizeOfArrs // 2, 0)
#         # tính tổng mảng theo từng cặp số -> kích thước giảm 1/2
#         with concurrent.futures.ThreadPoolExecutor() as excutor:
#             results = [excutor.submit(sum_couple, i, arrs) for i in range(sizeOfArrs // 2)]
#             for future in concurrent.futures.as_completed(results):
#                 result = future.result()
#                 sum_couple_arrs[result[0]] = result[1]
#         # trả về kết quả của mảng sau khi tính toán
#         new_res_arrs = prefixsum_algorithm(sum_couple_arrs, len(sum_couple_arrs))
#         new_arrs = arrs.copy()
#         with concurrent.futures.ThreadPoolExecutor() as excutor:
#             results = [excutor.submit(assign_value_new_arrs, arrs, new_res_arrs, new_arrs, i) for i in range(1, sizeOfArrs)]
#             for future in concurrent.futures.as_completed(results):
#                 future.result()
#         return new_arrs

def parallel_reduce(A, start, end):
    if start == end:
        return A[start]
    else:
        mid = int((start + end + 1) / 2)
        left_result = parallel_reduce(A, start, mid - 1)
        right_result = parallel_reduce(A, mid, end)
        return left_result + right_result
def scan(A, arr_B, s, t, offset):
    if s == t:
        arr_B[s] = offset + A[s]
    else:
        mid = (s + t - 1) // 2
        
        with concurrent.futures.ThreadPoolExecutor() as executor:
            left_result = executor.submit(scan, A, arr_B, s, mid, offset)
            left_sum = parallel_reduce(A, s, mid)
            
            right_result = executor.submit(scan, A, arr_B, mid + 1, t, offset + left_sum)

            # Đợi kết quả của từng công việc
            left_result.result()
            right_result.result()
    return arr_B
def parallel_prefix_sum(matrix_a):
    arr_B = matrix_a.copy()
    return scan(matrix_a, arr_B, 0, len(matrix_a) - 1, 0)

def insert_value_arrs(args):
    index, current_arrs, new_arrs = args
    for i in range(len(current_arrs)):
        new_arrs[index + i] = current_arrs[i]

def flatten_algorithm(arrs, sizeOfArrs):
    getSizeOfArrs = [0]
    sumSizeArrs = 0
    for arr in arrs:
        getSizeOfArrs.append(len(arr))
        sumSizeArrs += len(arr)
    getSizeOfArrs.remove(getSizeOfArrs[len(getSizeOfArrs) - 1])
    getSizeOfArrs = np.array(parallel_prefix_sum(getSizeOfArrs))
    tuple_arrs = []
    flatten_arrs = np.full(sumSizeArrs, 0)

    for index in range(len(getSizeOfArrs)):
        tuple_arr = (getSizeOfArrs[index], arrs[index], flatten_arrs)
        tuple_arrs.append(tuple_arr)

    with concurrent.futures.ThreadPoolExecutor() as excutor:
        excutor.map(insert_value_arrs, tuple_arrs)
    
    return flatten_arrs

def random_2d_array_int(rows, columns, low, high):
    return np.random.randint(low, high, size=(rows, columns))


if __name__ == "__main__":
    rows = 10
    columns = 10
    low = -10000
    high = 10000
    random_array = random_2d_array_int(rows, columns, low, high)
    print(random_array)
    array = flatten_algorithm(random_array, len(random_array))
    print("Kết quả sau khi flatten: ")
    print(str(array) + " có kích thước " + str(len(array)))