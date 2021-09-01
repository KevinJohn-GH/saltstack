# -*- coding: utf-8 -*-

"""
Stores eauth tokens in the filesystem of the master. Location is configured by the master config option 'token_dir'
"""

from __future__ import absolute_import, print_function, unicode_literals

import hashlib
import logging
import os

import salt.payload
import salt.utils.files
import salt.utils.path
import salt.utils.verify
from salt.ext import six
import redis
log = logging.getLogger(__name__)

__virtualname__ = "localfs"


class RedisToken(object):

    def __init__(self, opts):
        self.host = opts.get('token_redis_host')
        self.port = opts.get('token_redis_port')
        self.db = opts.get('token_redis_db')
        self.password = opts.get('token_redis_password', None)
        self.timeout = opts.get('token_redis_timeout')

        self.client = redis.StrictRedis(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password)

    def exist(self, token):
        return bool(self.client.exists(token))

    def set(self, token, data):
        self.client.setex(token, self.timeout * 60, data)

    def get(self, token):
        return self.client.get(token)

    def delete(self, token):
        self.client.delete(token)

    def list(self):
        return self.client.keys()


class FileToken(object):

    # try:
    #     with salt.utils.files.set_umask(0o177):
    #         with salt.utils.files.fopen(temp_t_path, "w+b") as fp_:
    #             fp_.write(serial.dumps(tdata))
    #     os.rename(temp_t_path, t_path)
    # except (IOError, OSError):
    #     log.warning('Authentication failure: can not write token file "%s".', t_path)
    #     return {}

    def __init__(self, opts):
        self.token_dir = opts['token_dir']

    def exist(self, token):
        t_path = os.path.join(self.token_dir, token)
        return os.path.isfile(t_path)

    def set(self, token, data):
        t_path = os.path.join(self.token_dir, token)
        with salt.utils.files.fopen(t_path, 'w+b') as fp_:
            fp_.write(data)

    def get(self, token):
        t_path = os.path.join(self.token_dir, token)
        if not os.path.isfile(t_path):
            return ''
        with salt.utils.files.fopen(t_path, 'rb') as fp_:
            data = fp_.read()
        return data

    def delete(self, token):
        t_path = os.path.join(self.token_dir, token)
        try:
            os.remove(t_path)
        except (IOError, OSError):
            pass

    def list(self):
        ret = []
        for (dirpath, dirnames, filenames) in salt.utils.path.os_walk(opts["token_dir"]):
            for token in filenames:
                ret.append(token)
        return ret


def mk_token(opts, tdata):
    """
    Mint a new token using the config option hash_type and store tdata with 'token' attribute set
    to the token.
    This module uses the hash of random 512 bytes as a token.

    :param opts: Salt master config options
    :param tdata: Token data to be stored with 'token' attribute of this dict set to the token.
    :returns: tdata with token if successful. Empty dict if failed.
    """

    token_storage_type = opts.get('token_storage_type', 'file')
    token_backend = {
        'file': FileToken(opts),
        'redis': RedisToken(opts)
    }[token_storage_type]

    hash_type = getattr(hashlib, opts.get("hash_type", "md5"))
    tok = six.text_type(hash_type(os.urandom(512)).hexdigest())
    t_path = os.path.join(opts["token_dir"], tok)
    temp_t_path = "{}.tmp".format(t_path)
    # while os.path.isfile(t_path):
    while token_backend.exist(tok):
        tok = six.text_type(hash_type(os.urandom(512)).hexdigest())
        # t_path = os.path.join(opts["token_dir"], tok)
    tdata["token"] = tok
    serial = salt.payload.Serial(opts)
    # try:
    #     with salt.utils.files.set_umask(0o177):
    #         with salt.utils.files.fopen(temp_t_path, "w+b") as fp_:
    #             fp_.write(serial.dumps(tdata))
    #     os.rename(temp_t_path, t_path)
    # except (IOError, OSError):
    #     log.warning('Authentication failure: can not write token file "%s".', t_path)
    #     return {}
    token_backend.set(tok, serial.dumps(tdata))
    return tdata


def get_token(opts, tok):
    """
    Fetch the token data from the store.

    :param opts: Salt master config options
    :param tok: Token value to get
    :returns: Token data if successful. Empty dict if failed.
    """

    token_storage_type = opts.get('token_storage_type', 'file')
    token_backend = {
        'file': FileToken(opts),
        'redis': RedisToken(opts)
    }[token_storage_type]

    t_path = os.path.join(opts["token_dir"], tok)
    if not salt.utils.verify.clean_path(opts["token_dir"], t_path):
        return {}
    # if not os.path.isfile(t_path):
    if not token_backend.exist(tok):
        return {}
    serial = salt.payload.Serial(opts)
    # try:
    #     with salt.utils.files.fopen(t_path, "rb") as fp_:
    #         tdata = serial.loads(fp_.read())
    #         return tdata
    # except (IOError, OSError):
    #     log.warning('Authentication failure: can not read token file "%s".', t_path)
    #     return {}
    tdata = serial.loads(token_backend.get(tok))
    return tdata


def rm_token(opts, tok):
    """
    Remove token from the store.

    :param opts: Salt master config options
    :param tok: Token to remove
    :returns: Empty dict if successful. None if failed.
    """

    token_storage_type = opts.get('token_storage_type', 'file')
    token_backend = {
        'file': FileToken(opts),
        'redis': RedisToken(opts)
    }[token_storage_type]

    # t_path = os.path.join(opts["token_dir"], tok)
    # try:
    #     os.remove(t_path)
    #     return {}
    # except (IOError, OSError):
    #     log.warning("Could not remove token %s", tok)
    token_backend.delete(tok)
    return {}


def list_tokens(opts):
    """
    List all tokens in the store.

    :param opts: Salt master config options
    :returns: List of dicts (tokens)
    """

    token_storage_type = opts.get('token_storage_type', 'file')
    token_backend = {
        'file': FileToken(opts),
        'redis': RedisToken(opts)
    }[token_storage_type]

    # ret = []
    # for (dirpath, dirnames, filenames) in salt.utils.path.os_walk(opts["token_dir"]):
    #     for token in filenames:
    #         ret.append(token)
    # return ret

    return token_backend.list()
