import msgpack
import asyncio

from .socket import Socket


class Client(Socket):
    """RPC Client"""

    def __init__(self,
                 *,
                 packer: msgpack.Packer = None,
                 unpacker: msgpack.Unpacker = None,
                 loop: asyncio.AbstractEventLoop = None,
                 response_timeout: int = None,
                 ignore_unimplemented: bool = True) -> None:
        Socket.__init__(self,
                        packer=packer,
                        unpacker=unpacker,
                        loop=loop,
                        response_timeout=response_timeout,
                        ignore_unimplemented=ignore_unimplemented)

    async def connect(self, host, port):
        reader, writer = await asyncio.open_connection(host, port)
        self._start(reader, writer)
