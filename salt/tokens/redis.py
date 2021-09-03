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

class RedisToken(object):

    def __init__(self, opts):
        self.redis_instances = opts.get('token_redis_instances')
        self.expire_minutes = opts.get('token_redis_expire_minutes')
        self.socket_timeout = opts.get('token_redis_socket_timeout', 60)
        self.socket_connect_timeout = opts.get('token_redis_socket_connect_timeout', 5)

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
                self.clients = []
                break


    def _exist(self, client, token):
        return bool(client.exists(token))

    def exist(self, token):
        for client in self.clients:
            try:
                ret = self._exist(client=client, token=token)
                if ret is False:
                    continue
                return ret
            except Exception as e:
                log.error(str(e))
                continue
        return False

    def _set(self, client, token, data):
        client.setex(token, self.expire_minutes * 60, data)

    def set(self, token, data):
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(fn=self._set, client=client, token=token, data=data) for client in self.clients]
            wait(futures)

    def _get(self, client, token):
        return client.get(token)

    def get(self, token):
        for client in self.clients:
            try:
                ret = self._get(client=client, token=token)
                if ret:
                    return ret
            except Exception as e:
                log.error(str(e))
                continue
        return None

    def delete(self, token):
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(fn=client.delete, token=token) for client in self.clients]
            wait(futures)

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

