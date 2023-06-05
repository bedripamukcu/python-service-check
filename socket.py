import msgpack
import asyncio
import logging
from typing import Any
from collections import OrderedDict

from .error import RPCResponseError, RPCNotConnectedError

logger = logging.getLogger(__name__)

REQUEST = 0
RESPONSE = 1
NOTIFY = 2


def _str(s):
    if isinstance(s, bytes):
        return s.decode('utf-8')
    return str(s)


class Socket(object):
    """RPC Socket"""

    def __init__(self,
                 *,
                 packer: msgpack.Packer = None,
                 unpacker: msgpack.Unpacker = None,
                 loop: asyncio.AbstractEventLoop = None,
                 response_timeout: int = None,
                 ignore_unimplemented: bool = True) -> None:
        self.connected = False
        self._packer = packer if packer is not None else msgpack.Packer(use_bin_type=True)
        self._unpacker = unpacker if unpacker is not None else msgpack.Unpacker(raw=False)
        self._ignore_unimplemented = ignore_unimplemented
        # exposed state
        self.loop = loop if loop is not None else asyncio.get_event_loop()
        self.response_timeout = response_timeout

        # internal  mutable state
        self._next_msgid = 0
        self._pending_requests = {}
        self._events = OrderedDict()

    def _add_event_handler(self, event, f):
        self._events[event] = f

    def _start(self,
               reader: asyncio.StreamReader,
               writer: asyncio.StreamWriter) -> None:
        self._reader = reader
        self._writer = writer
        self.connected = True
        self._receiver_task = self.loop.create_task(self._receiver())

    async def _receiver(self) -> None:
        """Background task to receive objects from the stream

        This allows parallel/overlapping rpc calls
        """
        try:
            unpacker = self._unpacker
            reader = self._reader
            logger.info("starting receiver")
            while True:
                data = await reader.read(n=4096)
                if not data:
                    raise ConnectionError("Connection has been closed")
                unpacker.feed(data)
                for obj in unpacker:
                    await self._on_message(obj)
        except asyncio.CancelledError:
            pass
        except ConnectionError:
            logger.info("Server connection has closed")
        except Exception:
            logger.exception("exception in client receiver")
        finally:
            logging.info("ending receiver")
            self.connected = False

    async def _invoke(self, name: str, params: Any) -> Any:
        if name not in self._events:
            if self._ignore_unimplemented:
                return None
            else:
                raise NotImplementedError(f"{name} is not implemented")
        func = self._events[name]
        if params is not None:
            if isinstance(params, dict):
                result = func(params)
            elif isinstance(params, list):
                result = func(*params)
        else:
            result = func()
        if asyncio.iscoroutine(result):
            result = await result
        return result

    async def _on_message(self, obj) -> None:
        """Handler for the reception of msgpack objects"""
        try:
            if obj[0] == REQUEST:
                _, msgid, name, params = obj
                try:
                    # handler can be a coroutine or a plain function
                    result = await self._invoke(_str(name), params)
                    response = (RESPONSE, msgid, None, result)
                except Exception as e:
                    logger.info("Exception %r in call handler %r", e, _str(name))
                    response = (RESPONSE, msgid, str(e), None)
                self._writer.write(self._packer.pack(response))

            elif obj[0] == NOTIFY:
                _, name, params = obj
                try:
                    result = await self._invoke(_str(name), params)
                except Exception:
                    logger.exception("Exception in notification handler %r", _str(name))

            elif obj[0] == RESPONSE:
                _, msgid, error, result = obj
                _, future = self._pending_requests[msgid]
                if error:
                    future.set_exception(RPCResponseError(error))
                else:
                    future.set_result(result)

            else:
                logger.error("received unknown object type %r", obj)
        except LookupError:
            logger.error("received unknown object %r", obj)

    def _get_next_msgid(self) -> int:
        """return the next msgid to be used"""
        val = self._next_msgid
        self._next_msgid = (self._next_msgid + 1) & 0xFFFFFFFF
        return val

    def close(self) -> None:
        """Remove all pending responses and close the underlying connection"""
        if not self.connected:
            return
        self.connected = False
        self._pending_requests = {}
        self._writer.close()
        self._receiver_task.cancel()

    def on(self, event, f=None):
        def _on(f):
            self._add_event_handler(event, f)
            return f

        if f is None:
            return _on
        else:
            return _on(f)

    async def call(self, name: str, params: Any = None, timeout: float = None) -> Any:
        """Call a remote function

        If timeout is not given the class attribute response_timeout will be used.
        """
        if not self.connected:
            raise RPCNotConnectedError()

        logger.debug(f"call: {name}{params}")
        timeout = timeout if timeout is not None else self.response_timeout

        request = (REQUEST, self._get_next_msgid(), name, params)

        # create a future for the response and make it responsable for its own cleanup
        future_response = self.loop.create_future()
        self._pending_requests[request[1]] = (request, future_response)
        future_response.add_done_callback(lambda fut: self._pending_requests.pop(request[1]))

        self._writer.write(self._packer.pack(request))

        # wait for the future or the timeout to complete
        return await asyncio.wait_for(future_response, timeout=timeout, loop=self.loop)

    def notify(self, name: str, params: Any = None) -> asyncio.Future:
        """Send a one-way notification to the server

        This function returns a future which is resolved when the write buffer is flushed.
        This can be optionally awaited on to ensure the message has been sent.
        """
        if not self.connected:
            raise RPCNotConnectedError()
        logger.debug(f"notify: {name}{params}")
        request = (NOTIFY, name, params)
        self._writer.write(self._packer.pack(request))
        return self._writer.drain()

    def notify_safe(self, name: str, params: Any = None) -> asyncio.Future:
        if not self.connected:
            raise RPCNotConnectedError()
        return self.loop.call_soon_threadsafe(self.notify, name, params)
