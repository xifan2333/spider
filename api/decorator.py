from tenacity import retry, stop_after_attempt, wait_exponential

def request_decorator(func):
    """请求装饰器,用于统一处理请求前的准备工作"""

    def wrapper(self, *args, **kwargs):
        self.update_ua()
        self.update_cookies()
        self.update_proxy()
        return func(self, *args, **kwargs)

    return wrapper


def retry_decorator(func):
    """重试装饰器,用于处理请求失败的情况"""

    def wrapper(self, *args, **kwargs):
        return func(self, *args, **kwargs)

    return wrapper
