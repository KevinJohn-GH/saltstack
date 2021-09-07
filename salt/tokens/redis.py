# -*- coding: utf-8 -*-

"""
Stores eauth tokens in the redis
"""

from __future__ import absolute_import, print_function, unicode_literals

import hashlib
import logging
import os

import salt.payload
from salt.ext import six

try:
    import redis
    from concurrent.futures import ThreadPoolExecutor, wait
    HAS_REDIS = True
except ImportError:
    HAS_REDIS = False

log = logging.getLogger(__name__)


__virtualname__ = "redis"


def __virtual__():
    if not HAS_REDIS:
        return (
            False,
            "Could not use redis for tokens; "
            "redis python client is not installed.",
        )
    return __virtualname__


def _wait_first_succeed(futures, fn):
    ret = None
    while len(futures) > 0:
        done, not_done = wait(futures, return_when='FIRST_COMPLETED')
        for done_future in done:
            exception = done_future.exception()
            if exception:
                if type(exception) == AssertionError:
                    log.debug('[{fn}] Future Exception: {exception}'.format(fn=fn, exception=exception))
                else:
                    log.error('[{fn}] Future Exception: {exception}'.format(fn=fn, exception=exception))
                continue
            else:
                ret = done_future.result()
                return ret
        futures = list(not_done)
    return ret


class RedisToken(object):

    def __init__(self, opts):
        self.redis_instances = opts.get('token_redis_instances')
        self.expire_minutes = opts.get('token_redis_expire_minutes')
        self.socket_timeout = opts.get('token_redis_socket_timeout', 10)
        self.socket_connect_timeout = opts.get('token_redis_socket_connect_timeout', 1)

        self.clients = []
        for instance in self.redis_instances:
            try:
                client = redis.StrictRedis(
                    host=instance['host'],
                    port=instance['port'],
                    db=instance['db'],
                    password=instance.get('password', None),
                    socket_timeout=self.socket_timeout,
                    socket_connect_timeout=self.socket_connect_timeout,
                    socket_keepalive=True)
                self.clients.append(client)
            except redis.exceptions.ConnectionError as err:
                log.warning(
                    "Failed to connect to redis at %s:%s - %s", instance['host'], instance['port'], err
                )

    def _exist(self, client, token):
        result = bool(client.exists(token))
        if result:
            return result
        else:
            kwargs = client.connection_pool.connection_kwargs
            raise AssertionError('Token: {token} not exist in redis://{host}:{port}/{db}'.format(
                token=token, host=kwargs['host'], port=kwargs['port'], db=kwargs['db']))

    def exist(self, token):
        executor = ThreadPoolExecutor(max_workers=10)
        futures = [executor.submit(fn=self._exist, client=client, token=token) for client in self.clients]
        result = _wait_first_succeed(futures=futures, fn=sys._getframe().f_code.co_name)
        return bool(result)

    def _set(self, client, token, data):
        result = client.setex(token, self.expire_minutes * 60, data)

        if not result:
            kwargs = client.connection_pool.connection_kwargs
            raise AssertionError('Token data for key {key} not set into redis://{host}:{port}/{db}'.format(
                key=token, host=kwargs['host'], port=kwargs['port'], db=kwargs['db']))

    def set(self, token, data):
        executor = ThreadPoolExecutor(max_workers=10)
        futures = [executor.submit(fn=self._set, client=client, token=token, data=data) for client in self.clients]
        _wait_first_succeed(futures=futures, fn=sys._getframe().f_code.co_name)

    def _get(self, client, token):
        result = client.get(token)
        if result is None:
            kwargs = client.connection_pool.connection_kwargs
            raise AssertionError('key={key} not exist in redis://{host}:{port}/{db}'.format(
                key=token, host=kwargs['host'], port=kwargs['port'], db=kwargs['db']))
        return result

    def get(self, token):
        executor = ThreadPoolExecutor(max_workers=10)
        futures = [executor.submit(fn=self._get, client=client, token=token) for client in self.clients]
        result = _wait_first_succeed(futures=futures, fn=sys._getframe().f_code.co_name)
        return result

    def _delete(self, client, token):
        client.delete(token)

    def delete(self, token):
        executor = ThreadPoolExecutor(max_workers=10)
        futures = [executor.submit(fn=self._delete, client=client, token=token) for client in self.clients]

    def _keys(self, client):
        return client.keys()

    def keys(self):
        for client in self.clients:
            try:
                ret = self._keys(client=client)
                if ret:
                    return ret
            except Exception as e:
                log.error(str(e))
                continue
        return None



def _redis_client(opts):
    """
    Connect to the redis host and return a StrictRedis client object.
    If connection fails then return None.
    """
    redis_host = opts.get("token_redis_host", "localhost")
    redis_port = opts.get("token_redis_port", 6379)
    redis_db = opts.get("token_redis_db")
    redis_password = opts.get('token_redis_password', None)
    redis_timeout = opts.get('token_redis_timeout')
    try:
        return redis.StrictRedis(
            host=redis_host,
            port=redis_port,
            db=redis_db,
            password=redis_password)
    except redis.exceptions.ConnectionError as err:
        log.warning(
            "Failed to connect to redis at %s:%s - %s", redis_host, redis_port, err
        )
        return None


def mk_token(opts, tdata):
    """
    Mint a new token using the config option hash_type and store tdata with 'token' attribute set
    to the token.
    This module uses the hash of random 512 bytes as a token.

    :param opts: Salt master config options
    :param tdata: Token data to be stored with 'token' attribute of this dict set to the token.
    :returns: tdata with token if successful. Empty dict if failed.
    """

    redis_client = RedisToken(opts)
    if not redis_client.clients:
        return {}
    hash_type = getattr(hashlib, opts.get("hash_type", "md5"))
    tok = six.text_type(hash_type(os.urandom(512)).hexdigest())
    try:
        while redis_client.get(tok) is not None:
            tok = six.text_type(hash_type(os.urandom(512)).hexdigest())
    except Exception as err:  # pylint: disable=broad-except
        log.warning(
            "Authentication failure: cannot get token %s from redis: %s", tok, err
        )
        return {}
    tdata["token"] = tok
    serial = salt.payload.Serial(opts)
    try:
        redis_timeout = opts.get('token_redis_timeout')
        redis_client.set(tok, serial.dumps(tdata))
    except Exception as err:  # pylint: disable=broad-except
        log.warning(
            "Authentication failure: cannot save token %s to redis: %s", tok, err
        )
        return {}
    return tdata


def get_token(opts, tok):
    """
    Fetch the token data from the store.

    :param opts: Salt master config options
    :param tok: Token value to get
    :returns: Token data if successful. Empty dict if failed.
    """
    redis_client = RedisToken(opts)
    if not redis_client.clients:
        return {}
    serial = salt.payload.Serial(opts)
    try:
        tdata = serial.loads(redis_client.get(tok))
        return tdata
    except Exception as err:  # pylint: disable=broad-except
        log.warning(
            "Authentication failure: cannot get token %s from redis: %s", tok, err
        )
        return {}


def rm_token(opts, tok):
    """
    Remove token from the store.

    :param opts: Salt master config options
    :param tok: Token to remove
    :returns: Empty dict if successful. None if failed.
    """
    redis_client = RedisToken(opts)
    if not redis_client.clients:
        return
    try:
        redis_client.delete(tok)
        return {}
    except Exception as err:  # pylint: disable=broad-except
        log.warning("Could not remove token %s: %s", tok, err)


def list_tokens(opts):
    """
    List all tokens in the store.

    :param opts: Salt master config options
    :returns: List of dicts (tokens)
    """
    ret = []
    redis_client = RedisToken(opts)
    if not redis_client.clients:
        return []
    serial = salt.payload.Serial(opts)
    try:
        return [k.decode("utf8") for k in redis_client.keys()]
    except Exception as err:  # pylint: disable=broad-except
        log.warning("Failed to list keys: %s", err)
        return []

