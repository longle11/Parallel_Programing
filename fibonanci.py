import multiprocessing
import math
import concurrent.futures
import numpy as np

def fibonacci(n):
    fib_values = [0, 1]  

    for i in range(2, n+1):
        fib_values.append(fib_values[i-1] + fib_values[i-2])
    
    return fib_values[n]

def parallel_fibonacci(n):
    if n <= 1:
        return n

    with concurrent.futures.ThreadPoolExecutor() as executor:
        future1 = executor.submit(fibonacci, n-1)
        future2 = executor.submit(fibonacci, n-2)

        A = future1.result()
        B = future2.result()

    return A + B



def matrix_multiply(a, b):
    rows_a = len(a)
    cols_a = len(a[0])
    cols_b = len(b[0])
    
    result = [[0] * cols_b for _ in range(rows_a)]
    
    for i in range(rows_a):
        for j in range(cols_b):
            for k in range(cols_a):
                result[i][j] += a[i][k] * b[k][j]
    
    return result

def matrix_power(matrix, p):
    print("Số mũ " + str(p))
    if p == 0:
        return [[1, 0], [0, 1]]
    elif p == 1:
        return matrix
    elif p % 2 == 0:
        sqrt_matrix = matrix_power(matrix, p // 2)
        return matrix_multiply(sqrt_matrix, sqrt_matrix)
    else:
        sqrt_matrix = matrix_power(matrix, p // 2)
        return matrix_multiply(matrix_multiply(sqrt_matrix, sqrt_matrix), matrix)

def fibonacci_parallel(n):
    if n <= 0:
        return None

    fib_matrix = [[1, 1], [1, 0]]

    # Chia nhỏ việc tính toán lũy thừa ma trận thành nhiều tiến trình
    with multiprocessing.Pool() as pool:
        power = pool.apply(matrix_power, (fib_matrix, n))

    fib_value = power[0][1]
    return fib_value
n = 100
p_fibo = fibonacci_parallel(n)
fibo = fibonacci(n)
print(p_fibo == fibo)
print("Kết quả tính song song: " + str(p_fibo))
print()
print("Kết quả tính tuần tự: " + str(fibo))
