class RPCException(Exception):
    pass


class RPCResponseError(RPCException):
    pass


class RPCNotConnectedError(RPCException):
    pass
