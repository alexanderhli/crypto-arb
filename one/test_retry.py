from tenacity import *

@retry(stop=stop_after_attempt(5), wait=wait_fixed(1))
def test():
    print("Testing")
    raise AssertionError

try:
    test()
except RetryError as e:
    print(e)
except Exception as e:
    print('5')
    print(e)
