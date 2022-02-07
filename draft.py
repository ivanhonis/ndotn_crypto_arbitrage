import numpy as np


def moving_average(x, w):
    return np.convolve(x, np.ones(w), 'valid') / w


data = [1, 2, 3, 4, 5, 6, 7, 8]
mad = moving_average(data, 3)
print(mad)

print(mad[-1])
