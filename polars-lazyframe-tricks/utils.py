from functools import wraps
from timeit import default_timer as timer


def time_it(func):
    """Decorator to measure execution time of a function"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = timer()
        result = func(*args, **kwargs)
        end = timer()
        print(f"{func.__name__} executed in {end - start:.4f} seconds")
        return result
    return wrapper
