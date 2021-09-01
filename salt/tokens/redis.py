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
    redis_client = _redis_client(opts)
    if not redis_client:
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
        redis_client.setex(tok, redis_timeout, serial.dumps(tdata))
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
    redis_client = _redis_client(opts)
    if not redis_client:
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
    redis_client = _redis_client(opts)
    if not redis_client:
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
    redis_client = _redis_client(opts)
    if not redis_client:
        return []
    serial = salt.payload.Serial(opts)
    try:
        return [k.decode("utf8") for k in redis_client.keys()]
    except Exception as err:  # pylint: disable=broad-except
        log.warning("Failed to list keys: %s", err)
        return []

