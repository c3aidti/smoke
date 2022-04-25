def run():
    """
    Starts a single instance of StressTestPlainCompute
    """
    import cdecimal

    n = 4000
    cdecimal.getcontext().prec = n + 1
    C = 426880 * cdecimal.Decimal(10005).sqrt()
    K = 6.
    M = 1.
    X = 1
    L = 13591409
    S = L

    for i in range(1, n):
        M = M * (K ** 3 - 16 * K) / ((i + 1) ** 3)
        L += 545140134
        X *= -262537412640768000
        S += cdecimal.Decimal(M * L) / X

        pi = C / S

    return pi