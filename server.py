import sys
import json
import http
import hashlib
import bottle
import struct
import traceback
import html

neighbours = set()
database = {}


NBS_DHSH = 1
NBS_DATA = 2
NBS_MASK = 3

CHUNKS = 8

def store(data):
    pass


def register_peer(data):
    data = data.split(':')
    host = data[0]
    port = int(data[1])
    peer = (host, port)
    neighbours.add(peer)
    return peer


def decode_chunk(data):
    offset = 0
    dhsh = None
    value = None
    metadata = None
    ret = json.loads(data.decode('ascii'))
    return ret['dhsh'], ret['mask'], ret['data'].encode('ascii')


def repair_data(length, *argv):
    ret = [None] * CHUNKS
    for mask, data in argv:
        clength = ( len(data) + CHUNKS - 1 ) // CHUNKS
        for i in range(CHUNKS):
            if (ret[i] is None) and ((1 << i) & 0xff):
                ret[i] = data[i * clength:i * clength + clength]
    return ''.join(ret).encode('ascii')


def damage_data(data, mask):
    ret = b''
    length = len(data)
    clength = ( len(data) + CHUNKS - 1 ) // CHUNKS
    for byte in range(CHUNKS):
        if (1 << byte) & mask:
            ret += data[byte * clength:byte * clength + clength]
        else:
            ret += b'0' * clength
    return ret[0:length]


def iter_chunks(data, dhsh):
    for byte in range(8):
        ret = {'dhsh': dhsh,
               'mask': 0xff ^ (1 << byte),
               'data': damage_data(data, 0xff ^ (1 << byte)).decode('ascii')}
        ret = json.dumps(ret)
        yield ret


@bottle.get('/db/')
def http_db():
    return bottle.template('{{db}}', db=database)


@bottle.post('/db/one/')
def http_db_one():
    key = bottle.request.body.getvalue().decode('ascii')
    ret = database.get(key, None)
    if ret is None:
        return bottle.template('')
    else:
        return bottle.template('{{data}}', data=json.dumps([ret[0], ret[1].decode('ascii')]))


@bottle.post('/db/get/')
def http_db_get():
    key = bottle.request.body.getvalue().decode('ascii')
    ret = database.get(key, None)
    if ret is None:
        return bottle.template('')
    else:
        if ret[0] == 0:
            return bottle.template('{{data}}', data=ret[1])
        else:
            rets = []
            length = 0
            for peer in neighbours:
                try:
                    cx = http.client.HTTPConnection(*peer)
                    try:
                        cx.request('POST', '/db/one/', body=key)
                        ret = cx.getresponse()
                        assert ret.status == 200
                        ret = html.unescape(ret.read().decode('ascii'))
                        ret = json.loads(ret)
                        rets.append(ret)
                        length = len(ret[1])
                    finally:
                        cx.close()
                except:
                    traceback.print_exc()
            return bottle.template('{{data}}', data=repair_data(length, *rets))


@bottle.post('/store/chunk/')
def http_store_chunk():
    data = bottle.request.body.getvalue()
    dhsh, metadata, data = decode_chunk(data)
    database[dhsh] = (metadata, data)
    return bottle.template('{{chunk}}', chunk=data)


@bottle.post('/store/data/')
def http_store():
    data = bottle.request.body.getvalue()
    dhsh = hashlib.md5(data).hexdigest()
    database[dhsh] = (0, data)
    chunks = iter_chunks(data, dhsh)
    for peer in neighbours:
        try:
            chunk = next(chunks)
        except:
            traceback.print_exc()
            break
        try:
            cx = http.client.HTTPConnection(*peer)
            try:
                cx.request('POST', '/store/chunk/', body=chunk)
                ret = cx.getresponse()
                assert ret.status == 200
            finally:
                cx.close()
        except:
            traceback.print_exc()
    return bottle.template('{{digest}}', digest=dhsh)


@bottle.post('/config/neighbours/test')
def http_neighbour_test():
    data = bottle.request.body.getvalue().decode('utf-8')
    if data:
        peer = register_peer(data)
    return bottle.template('{{nbx}}', nbx=neighbours)


@bottle.post('/config/neighbours/add')
def http_neighbour_add():
    data = bottle.request.body.getvalue().decode('utf-8')
    peer = register_peer(data)
    # ping peer
    try:
        cx = http.client.HTTPConnection(*peer)
        try:
            self = ':'.join(sys.argv[1:3])
            cx.request('POST', '/config/neighbours/test', body=self.encode('utf-8'))
            ret = cx.getresponse()
            assert ret.status == 200
        finally:
            cx.close()
    except:
        traceback.print_exc()
        neighbours.remove(peer)
    return bottle.template('{{nbx}}', nbx=neighbours)


bottle.run(host=sys.argv[1], port=int(sys.argv[2]))
