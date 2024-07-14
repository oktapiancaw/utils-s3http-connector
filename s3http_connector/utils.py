from functools import wraps


def client_detector(func):
    @wraps(func)
    def wrapped(self, *args, **kwargs):
        if not self.client:
            raise ValueError("Client is not connected")
        return func(self, *args, **kwargs)

    return wrapped
