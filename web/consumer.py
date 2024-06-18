def min_max_fun(N, M, C, F):
    dp = [[0] * (M + 1) for _ in range(N + 1)]
    for i in range(1, N + 1):
        for j in range(1, M + 1):
            if C[i - 1] <= j:
                dp[i][j] = max(dp[i - 1][j], F[i - 1] + dp[i - 1][j - C[i - 1]])
            else:
                dp[i][j] = dp[i - 1][j]

    min_amount = max_fun = 0
    for j in range(1, M + 1):
        if dp[N][j] > max_fun:
            max_fun = dp[N][j]
            min_amount = j

    return [min_amount, max_fun]

# Input parsing
N = int(input())
M = int(input())
C = [int(input()) for _ in range(N)]
F = [int(input()) for _ in range(N)]

# Function call
result = min_max_fun(N, M, C, F)
print(result[0], result[1])
