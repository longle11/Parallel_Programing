import numpy as np
import concurrent.futures
def shuffle_array(array, random_array):
    # print("Mang random " + str(random_array))
    for index in range(len(array)):
        new_pos = random_array[index]
        array[index], array[new_pos] = array[new_pos], array[index]
    # print("Mang sau khi shuffle: " + str(array))

    return array

def isPermutation(args):
    index, index_Random, check_permutation, array, arg_arrays = args
    # print("Thuc nghiem tren index " + str(index) + " co index random " + str(index_Random) + " co mang permutation " + str(check_permutation))

    if check_permutation[index] == 1 or check_permutation[index_Random] == 1:
        # print("2 cap nay khong the hoan vi " + str(index) + "-" + str(index_Random))
        return
    else:
        if index == index_Random: 
            # print("2 cap trung nhau " + str(index))
            check_permutation[index] = 1
        else:
            # print("2 cap co the hoan vi " + str(index) + "-" + str(index_Random))
            check_permutation[index] = 1
            check_permutation[index_Random] = 1

            array[index], array[index_Random] = array[index_Random], array[index]
            # print("---->Sau khi hoan vi " + str(index) + "-" + str(index_Random) + " co mang " + str(array))
        # print("Xoa phan tu thu " + str(index))
        arg_arrays[index] = -1
        check_permutation[index] = 0
        check_permutation[index_Random] = 0
def parallel_shuffle(array, random_array):
    # print("Mang random " + str(random_array))
    check_permutation = np.full(len(array), 0)
    arg_arrays = []
    for index in range(len(array)):
        arg_arrays.append((index, random_array[index], check_permutation, array, arg_arrays))
    # print("******************Mang luc dau " + str(len(arg_arrays)))
    while True:
        with concurrent.futures.ThreadPoolExecutor() as excutor:
            excutor.map(isPermutation, arg_arrays)
        
        check = False
        for index in range(len(arg_arrays)):
            if arg_arrays[index] != -1:
                check = True
                break
        
        if not check:
            break
    # print("******************Mang ket thuc " + str(arg_arrays))

    return array

if __name__ == "__main__":
    array = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']
    random_array = np.random.randint(0, len(array) - 1, len(array))

    if(shuffle_array(array, random_array) == parallel_shuffle(array, random_array)):
        print("2 mang hoan vi bang nhau")
    else: print("2 mang khong bang nhau")
    print("Mang sau khi shuffle " + str(array))
