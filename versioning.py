#!/usr/bin/env python

VERSION = '99.99.99'
NAME = 'WriteBehind'

import time
import redis
import json


conn = None
_debug=True

KEY = '_key'

REDIS_HOST = 'redis-12052.ohad.demo.redislabs.com'
REDIS_PORT = 12052
REDIS_PASSWORD = None
ORIGIN_FIELD='origin'

SLEEP_TIME=1

config = {
    'customer:id': {
        'prefix_key':'customer',
        'history_prefix_key':'customer-',
        'ttl': 20,
        'index_name': 'customers',
        'timestamp_field': 'timestamp'
    },
    'organization:id': {
        'prefix_key':'organization:',
        'history_prefix_key':'organization-',
        'ttl': 20,
        'index_name': 'organizations',
        'timestamp_field': 'timestamp'
    },
}


# config = {
#     'person2:id': {
#         TABLE_KEY: 'person1',
#         'first_name': 'first',
#         'last_name': 'last',
#         'age': 'age',
#     },
#     'car:license': {
#         'color': 'color',
#     },
# }

#----------------------------------------------------------------------------------------------

def Log(msg, prefix='RedisGears - '):
    msg = prefix + msg
    try:
        execute('debug', 'log', msg)
    except Exception:
        print(msg)

def Debug(msg, prefix='RedisGears - '):
    if not _debug:
        return
    msg = prefix + msg
    try:
        execute('debug', 'log', msg)
    except Exception:
        print(msg)

#----------------------------------------------------------------------------------------------

def Connect():
    global conn

    try:
        conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASSWORD, decode_responses=True)
        return conn
    except Exception as e:
        Debug('Failed connecting to redis on host %s, error=%s' %(REDIS_HOST, str(e)))


def GetStreamName(config):
    return '_%sstream-{%s}' % (config['history_prefix_key'], hashtag())

def DeleteFromSearch(config):
    def RemoveFromSearch(r):
        WriteBehindLog('In RemoveFromSearch: ' + r['key'])
        execute('FT.DEL', config['index_name'], r['key'])
        # Debug('In RemoveFromSearch: ' + r['key'])
        # data = []
        # data.append([KEY, r['key'].split(':')[1]])
        # if 'value' in r.keys():z
        #     for k, v in r['value'].items():
        #         execute('FT.DEL', config['index_name'], k)
        # Debug('In CreateStreamInserter')
    return RemoveFromSearch

def CreateStreamInserter(config):
    def AddToStream(r):
        # Debug('In AddToStream: ' + r['key'])
        data = []
        data.append([KEY, r['key'].split(':')[1]])
        if 'value' in r.keys():
            # keys = r['value'].keys()
            for k, v in r['value'].items():
                # if kInHash.startswith('_'):
                #     continue
                # if kInHash not in keys:
                #     msg = 'Could not find %s in hash %s' % (kInHash, r['key'])
                #     Log(msg)
                #     raise Exception(msg)
                data.append([k, v])
        execute('xadd', GetStreamName(config), '*', *sum(data, []))
    # Debug('In CreateStreamInserter')
    return AddToStream

def CreateSearchDataWriter(config):
    def WriteToSearch(r):
        # Debug('In WriteToSearch')

        global conn

        if len(r) == 0:
            Log('Warning, got an empty batch')
            return
        # for x in r:
        #     x.pop('streamId', None)## pop the stream id out of the record, we do not need it.
        while True:
            # Debug('WriteToSearch: in loop')
            errorOccured = False

            try:
                if not conn:
                    conn = Connect()
            except Exception as e:
                conn = None # next time we will reconnect to the database
                Log('Failed connecting to Redis, will retry in %d second. error="%s"' % (SLEEP_TIME, str(e)))
                time.sleep(SLEEP_TIME)
                continue # lets retry

            try:
                for x in r:
                    changedKey = x.pop(KEY, None)
                    streamId = x.pop('streamId', None)
                    timestampField = config['timestamp_field']
                    x[ORIGIN_FIELD] = r"%s\:%s" % (config['prefix_key'], changedKey)

                    # create the hash to store the data and add TTL
                    if timestampField:
                        timestamp = x[timestampField]
                        timestampedKey = '{%s%s}:%s' % (config['history_prefix_key'], changedKey, timestamp)
                    else:
                        timestamp = streamId.split('-')[0]
                        timestampedKey = '{%s%s}:%s' % (config['history_prefix_key'], changedKey, timestamp)
                        x['time_stamp'] = timestamp

                    conn.hmset(timestampedKey, x)
                    if config['ttl'] > 0:
                        conn.expire(timestampedKey, config['ttl'])

                    # index the data both current and history
                    Log('!!!!Indexing, data=%s.' % x)
                    dict_list=[]
                    for i,j in x.items():
                        dict_list.append (i)
                        dict_list.append (j)
                    conn.execute_command('FT.ADD', config['index_name'], '%s:%s' % (config['history_prefix_key'], changedKey), 1.0, 'REPLACE', 'FIELDS', *dict_list) 
                    conn.execute_command('FT.ADD', config['index_name'], timestampedKey, 1.0, 'REPLACE', 'FIELDS', *dict_list) 
            except Exception as e:
                Log('Got exception when writing to Search, key="%s", error="%s".' % (changedKey, str(e)))
                raise e
                errorOccured = True

            if errorOccured:
                conn = None # next time we will reconnect to the database
                Log('Error occured while writing to search, will retry in %d second.' % SLEEP_TIME)
                time.sleep(SLEEP_TIME)
                continue # lets retry
            return # we finished successfully, lets break the retry loop

    # Debug('In CreateSearchDataWriter')
    return WriteToSearch

def CheckIfHash(r):
    if 'value' not in r.keys() or isinstance(r['value'], dict) :
        return True
    Log('Got a none hash value, key="%s" value="%s"' % (str(r['key']), str(r['value'] if 'value' in r.keys() else 'None')))
    return False

def WriteBehindLog(msg, prefix='RedisGears - '):
    msg = prefix + msg
    Log('notice', msg)    

def WriteBehindDebug(msg, prefix='RedisGears - '):
    msg = prefix + msg
    Log('debug', msg)

def RegistrationArrToDict(registration, depth):
    if depth >= 2:
        return registration
    if type(registration) is not list:
        return registration
    d = {}
    for i in range(0, len(registration), 2):
        d[registration[i]] = RegistrationArrToDict(registration[i + 1], depth + 1)
    return d

def IsVersionLess(v):
    if VERSION == '99.99.99':
        return True # 99.99.99 is greater then all versions
    major, minor, patch = VERSION.split('.')
    v_major, v_minot, v_patch = v.split('.')

    if int(major) > int(v_major):
        return True
    elif int(major) < int(v_major):
        return False

    if int(minor) > int(v_major):
        return True
    elif int(minor) > int(v_major):
        return False

    if int(patch) > int(v_patch):
        return True
    elif int(patch) > int(v_patch):
        return False

    return False

def UnregisterOldVersions():
    WriteBehindLog('Unregistering old versions of %s' % NAME)
    registrations = execute('rg.dumpregistrations')
    for registration in registrations:
        registrationDict = RegistrationArrToDict(registration, 0)
        descStr = registrationDict['desc']
        try:
            desc = json.loads(descStr)
        except Exception as e:
            continue
        if 'name' in desc.keys() and desc['name'] == NAME:
            if 'version' not in desc.keys():
                execute('rg.unregister', registrationDict['id'])
                WriteBehindLog('Unregistered %s' % registrationDict['id'])
            version = desc['version']
            if IsVersionLess(version):
                execute('rg.unregister', registrationDict['id'])
                WriteBehindLog('Unregistered %s' % registrationDict['id'])
            else:
                raise Exception('Found a version which is greater or equals current version, aborting.')

def RegisterExecutions():
    for k, v in config.items():
        regs0 = execute('rg.dumpregistrations')

        regex = k.split(':')[0]

        ## create the execution to write each changed key to stream
        descJson = {
            'name':NAME,
            'version':VERSION,
            'desc':'add each changed key with prefix %s:* to Stream' % regex,
        }
        GB('KeysReader', desc=json.dumps(descJson)).\
        filter(lambda x: x['key'] != GetStreamName(v)).\
        filter(CheckIfHash).\
        foreach(CreateStreamInserter(v)).\
        register(mode='sync', regex='%s:*' % regex, eventTypes=['hset', 'hmset'])

        ## create the execution to delete from search on delete key event
        descJson = {
            'name':NAME,
            'version':VERSION,
            'desc':'remove indexed document on hash delete key with prefix {%s-* ' % regex,
        }
        GB('KeysReader', desc=json.dumps(descJson)).\
        filter(lambda x: x['key'] != GetStreamName(v)).\
        filter(CheckIfHash).\
        foreach(DeleteFromSearch(v)).\
        register(mode='sync', regex='{%s-*' % regex, eventTypes=['del', 'expired'])

        ## create the execution to write each key from stream to DB
        descJson = {
            'name':NAME,
            'version':VERSION,
            'desc':'read from stream and write and save to Redis for versioning',
        }
        GB('StreamReader', desc=json.dumps(descJson)).\
        aggregate([], lambda a, r: a + [r], lambda a, r: a + r).\
        foreach(CreateSearchDataWriter(v)).\
        count().\
        register(regex='_%sstream-*' % (v['history_prefix_key']), mode="async_local", batch=100, duration=1000)

    # Debug('-' * 80)
    # regs = execute('rg.dumpregistrations')
    # Debug('regs: ' + str(regs))
    # Debug('-' * 80)

#----------------------------------------------------------------------------------------------

Debug('-' * 80)
Log('Starting gear')

Debug('-' * 80)

UnregisterOldVersions()
RegisterExecutions()

Debug('-' * 80)
