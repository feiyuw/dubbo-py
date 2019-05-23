class DubboError(RuntimeError):
    def __init__(self, status, msg):
        self.status = status
        self.message = msg
