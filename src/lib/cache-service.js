import { debugModule } from '@mdrooker/rna-logger'
import { caching, multiCaching } from 'cache-manager';
import redisStore from "cache-manager-ioredis-yet";
import Redis from "ioredis";
import { default as subscriptionInit } from './subscription.js'

import { validateConfig } from './utils.js';
const debug = debugModule('rna:cache-manager:cache');
let _MEMORY_CACHE_TIME;
let _MEMORY_CACHE_SIZE;
let _options;
let _redisConfigOptions;
let _cachePurgeChannel;
let _rstore;
let _cacheKeyPrefix;
let _rateLimitPrefix
let _keyPrefix;
let _redisConfig;
let _redisCache;
let _memoryCache;
let _client;
let _staleCache;
let _rateLimiter;
let _publisher;
let _subscriber;
let _multiCache;
let _metrics;
const parseConfig = ({ config }) => {
    if (config && validateConfig(config)) {
        _redisConfig = config.CACHE.REDIS;
        _redisConfigOptions = {
            host: _redisConfig.URL,
            port: _redisConfig.PORT,
            password: _redisConfig.PASSWORD ? _redisConfig.PASSWORD : null,
            auth_pass: _redisConfig.PASSWORD,
            compress: true,
            connectTimeout: 20000,
            maxRetriesPerRequest: 10
        }
        _MEMORY_CACHE_TIME = config.CACHE.MEMORY.CACHE_TIME;
        _MEMORY_CACHE_SIZE = config.CACHE.MEMORY.CACHE_SIZE;
        _keyPrefix = `app:${config.NAME.SYSTEM}:${config.NAME.PRODUCT}:${config.ENVIRONMENT}:`;
        _cacheKeyPrefix = `${_keyPrefix}cache`;
        _cachePurgeChannel = `${_keyPrefix}purge`;
        _rateLimitPrefix = `${_keyPrefix}ratelimit`;
        return true
    } else {
        return false
    }
}

function isCacheableValue(value) {
    if (_.isObject(value)) {
        return true;
    }
    else {
        return false;
    }
};


class CacheService {
    constructor() {
        this.cache = {};
    }
    get cachePurgeChannel() {
        if (_cachePurgeChannel) {
            return _cachePurgeChannel
        }
    }
    getCachePurgeChannel() {
        if (_options) {
            return _cachePurgeChannel
        } else {
            throw new Error('Cache not initialized')
        }
    }
    get publisher() {
        if (_publisher) {
            return _publisher
        }
        else {
            if (_options) {
                _publisher = this.getPublisher();
            }
        }
    }
    getPublisher() {
        if (_publisher) {
            return _publisher
        } else {
            debug('Publisher not initialized..Retrying');
            if (!_options) {
                debug('Publisher not initialized.');
            } else {
                _publisher = new Redis(_options);
                if (!_publisher) {
                    throw new Error('Publisher not initialized')
                } else {
                    return _publisher
                }

            }
        }
    }
    getSubscriber() {
        if (_subscriber) {
            return _subscriber
        } else {
            debug('Subscriber not initialized..Retrying');
            _subscriber = new Redis(_options);
            if (!_subscriber) {
                throw new Error('Publisher not initialized')
            } else {
                return _subscriber
            }
        }
    }
    get subscriber() {
        if (_subscriber) {
            return _subscriber
        }
        else {
            if (_options) {
                _subscriber = this.getPublisher();
            }
        }
    }
    get rateLimiter() {
        if (_rateLimiter) {
            return _rateLimiter
        }
        else {
            if (_options) {
                _rateLimiter = this.getRateLimiter();
            }
        }

    }
    getRateLimiter() {
        if (!_rateLimiter) {
            return _rateLimiter
        } else {
            _rateLimiter = new Redis({ ..._options, keyPrefix: rateLimitPrefix });
            debug('Rate Limiter not initialized..retrying');
            if (!_rateLimiter) {
                throw new Error('Rate Limiter not initialized')
            } else {
                return _rateLimiter
            }
        }
    }

    get staleCache() {
        if (_staleCache) {
            return _staleCache
        }
        else {
            if (_options) {
                _staleCache = this.getStaleCache();
            }
        }
    }
    getStaleCache() {
        if (!_staleCache) {
            return _staleCache
        } else {
            debug('Stale Cache not initialized..Retrying');
            caching('memory', { store: 'memory', max: MEMORY_CACHE_SIZE, ttl: MEMORY_CACHE_TIME }).then((cache) => {
                _staleCache = cache
                if (!_staleCache) {
                    throw new Error('Stale Cache not initialized')
                } else {
                    return _staleCache
                }
            });
        }
    }
    get redisCache() {
        if (_redisCache) {
            return _redisCache
        }
        else {
            if (_options) {
                _redisCache = this.getRedisCache();
            }
        }

    }
    getRedisCache() {
        if (!_redisCache) {
            return _redisCache
        } else {
            debug('Redis Cache not initialized..Retrying');
            caching(_rstore,
                {
                    isCacheableValue: isCacheableValue
                }).then((cache) => {
                    _redisCache = cache
                    if (!_redisCache) {
                        throw new Error('Redis Cache not initialized')
                    } else {
                        return _redisCache
                    }
                })
        }
    }

    get memoryCache() {
        if (_memoryCache) {
            return _memoryCache
        }
        else {
            if (_options) {
                _memoryCache = this.getMemoryCache();
            }
        }
    }
    getMemoryCache() {
        if (!_memoryCache) {
            return _memoryCache
        } else {
            debug('Memory Cache not initialized..Retrying');
            caching('memory', { store: 'memory', max: MEMORY_CACHE_SIZE, ttl: MEMORY_CACHE_TIME }).then((cache) => {
                _memoryCache = cache
                if (!_memoryCache) {
                    throw new Error('Memory Cache not initialized')
                } else {
                    return _memoryCache
                }
            });
        }
    }

    getMultiCache() {
        debugger
        if (_multiCache) {
            return _multiCache
        } else {
            if (_options) {
                _multiCache = multiCaching([this.getMemoryCache(), this.getRedisCache()]);
            }

        }
    }
    get multiCache() {
        if (_multiCache) {
            return _multiCache
        }
        else {
            if (_options) {
                _multiCache = this.getMultiCache();
            }
        }
    }
    async wrap({ cache, key, metricName, retreiver, metrics, ttl }) {
        let fromSource = false;
        if (metrics) {
            metrics.counter({ name: metricName });
        }
        let cacheKey = `${_cacheKeyPrefix}:${key}`;
        var opts = {
            cacluateTTL: function (ttl) {
                if (ttl === 0) return 0;
                if (ttl) {
                    if (ttl <= 1000 && ttl > 0) {
                        return ttl * 1000;
                    } else {
                        return ttl;
                    }
                }
                else {
                    return 1000;
                }
            }
        };
        let staleCacheEntry = await _staleCache.get(cacheKey);
        if (ttl === 0) {
            if (retreiver) {
                try {
                    let returnPayload = await retreiver();
                    return returnPayload;
                }
                catch (error) {
                    console.error(error);
                    if (staleCacheEntry) {
                        if (metrics) {
                            //Do something to let us know its serving stale
                            metrics.counter({ name: `cachemanager:${metricName}.stale` });
                            _staleCache.set(`${cacheKey}:hit`, staleCache.get(`${cacheKey}:hit`) + 1);
                        }
                        return staleCacheEntry;
                    }
                }
            }
        } else {

            let returnPayload = cache.wrap(cacheKey, async function () {
                fromSource = true
                debug(`${cacheKey} - Getting from Source`);
                const t2 = Date.now();
                if (retreiver) {
                    try {
                        returnPayload = await retreiver();
                    }
                    catch (error) {
                        console.error(error);
                        if (staleCacheEntry) {
                            if (metrics) {
                                //Do something to let us know its serving stale
                                metrics.counter({ name: `cachemanager.${metricName}.stale` });
                                _staleCache.set(`${cacheKey}:hit`, staleCache.get(`${cacheKey}:hit`) + 1);
                            }
                            return staleCacheEntry;
                        }
                    }
                }
                let getElapsed = Date.now() - t2;
                if (metrics) {
                    metrics.histogram({ name: `cachemanager.${metricName}.elapsed`, value: getElapsed });
                    metrics.counter({ name: `cachemanager.${metricName}.retreiver` });
                }
                _staleCache.set(cacheKey, returnPayload);
                return returnPayload;
            }, opts.cacluateTTL(ttl)).then(payload => {

                if (fromSource) {
                    return payload;
                } else {
                    debug(`${cacheKey} - Getting from Cache`);
                    if (metrics) {
                        metrics.counter({ name: `cachemanager.${metricName}.cache` });
                    }
                    if (!staleCacheEntry) {
                        _staleCache.set(cacheKey, payload);
                    }
                    return payload;
                }
            });
            return returnPayload;
        }
    }
    async purge({ key, isMaster }) {
        let cacheKey = `${_keyPrefix}:${key}`;
        let keysToBePurged;
        try {
            debug(`Searching for Keys to purge ${cacheKey}`);
            keysToBePurged = await _redisCache.keys(cacheKey);
        }
        catch (error) {
            console.error(error);
        }
        debug(`Keys to purge ${keysToBePurged}`);
        keysToBePurged.forEach(async (key) => {
            debug(`Purging Key from the the local Cache ${key} to Channel ${cachePurgeChannel}`);
            memoryCache.del(key); //if you delete it from memory...its just gonna go back to redis
            if (isMaster) {
                redisCache.del(key);
            }
            publisher.publish(cachePurgeChannel, key);
        });


    }
    async listCurrentCacheKeys() {
        let cacheKey = `${_cacheKeyPrefix}*`;
        let currentKeys = [];
        return new Promise((resolve, reject) => {
            try {
                debug(`Looking for ${cacheKey}`);
                let stream = _publisher.scanStream({
                    match: `${cacheKey}`,
                    // returns approximately 100 elements per call
                    count: 500,
                });
                stream.on('data', function (keys) {
                    // `keys` is an array of strings representing key names
                    debug(`Found keys of ${keys.length} size`);
                    if (keys.length) {
                        keys.forEach(function (key) {
                            currentKeys.push(key);
                        });
                    }
                });
                stream.on('end', function () {
                    debug(`Done Finding Keys for ${cacheKey}`);
                    resolve(currentKeys);
                });
            }
            catch (error) {
                console.log(error);
            }
        });


    }
    async purge({ key, isMaster }) {
        let cacheKey = `${_keyPrefix}:${key}`;
        let keysToBePurged;
        try {
            debug(`Searching for Keys to purge ${cacheKey}`);
            keysToBePurged = await _redisCache.keys(cacheKey);
        }
        catch (error) {
            console.error(error);
        }
        debug(`Keys to purge ${keysToBePurged}`);
        keysToBePurged.forEach(async (key) => {
            debug(`Purging Key from the the local Cache ${key} to Channel ${_cachePurgeChannel}`);
            _memoryCache.del(key); //if you delete it from memory...its just gonna go back to redis
            if (isMaster) {
                _redisCache.del(key);
            }
            _publisher.publish(cachePurgeChannel, key);
        });

    }
    async fastPurge({ key, isMaster }) {
        let cacheKey = `${_cacheKeyPrefix}:*${key}*`;
        return new Promise((resolve, reject) => {
            try {
                debug(`Looking for ${cacheKey} to Purge`);
                let stream = _publisher.scanStream({
                    match: `${cacheKey}`,
                    // returns approximately 500 elements per call
                    count: 500,
                });

                stream.on('data', function (keys) {
                    // `keys` is an array of strings representing key names
                    debug(`Found keys of ${keys.length} size for Purge`);
                    if (keys.length) {
                        keys.forEach(function (key) {
                            debug(`Purging Key ${key} from Memory`);
                            _memoryCache.del(key); //if you delete it from memory...its just gonna go back to redis
                            if (isMaster) {
                                _redisCache.del(key);
                            }
                            debug(`Purging Key ${key} from Published Channel ${_cachePurgeChannel}`);
                            _publisher.publish(_cachePurgeChannel, key);
                        });
                    }
                });
                stream.on('end', function () {
                    debug('Done Purging Keys');
                    resolve();
                });
            }
            catch (error) {
                debugger
                console.log(error);
            }
        });


    }
    async init({ config, metrics }) {
        if ((parseConfig({ config }))) {
            _rstore = await redisStore.redisStore({
                host: _redisConfig.URL,
                port: _redisConfig.PORT,
                password: _redisConfig.PASSWORD ? _redisConfig.PASSWORD : null,
                auth_pass: _redisConfig.PASSWORD,
                compress: true,
                connectTimeout: 20000,
                maxRetriesPerRequest: 10
            })

            _redisCache = await caching(_rstore,
                {
                    isCacheableValue: isCacheableValue
                }),
                _memoryCache = await caching('memory', { store: 'memory', max: _MEMORY_CACHE_SIZE, ttl: _MEMORY_CACHE_TIME }),
                _staleCache = await caching('memory', { store: 'memory', max: _MEMORY_CACHE_SIZE, ttl: _MEMORY_CACHE_TIME }),
                _options = {
                    connectionName: 'redis',
                    host: _redisConfig.URL,
                    port: _redisConfig.PORT,
                    password: _redisConfig.PASSWORD ? _redisConfig.PASSWORD : null,
                    connectTimeout: 20000,
                    compress: true,
                    maxRetriesPerRequest: 10
                };
            _client = new Redis({ ..._options, keyPrefix: _keyPrefix });
            _rateLimiter = new Redis({ ..._options, keyPrefix: _rateLimitPrefix });
            _publisher = new Redis(_options);
            _subscriber = new Redis(_options);
            _multiCache = multiCaching([_memoryCache, _redisCache]);
            debug(`Using InMemory Cache of ${_MEMORY_CACHE_TIME} seconds`);
            debug(`Using Redis Cache of ${_redisConfig.CACHE_TIME} seconds`);

            subscriptionInit({ config: _redisConfigOptions, publisher: getPublisher(), subscriber: getSubscriber() })
        } else {
            throw new Error('Invalid Config');
        }
        if (metrics) {
            _metrics = metrics;
        }
        return true;
    }

}

let cacheService = new CacheService();

export const { getRateLimiter,
    getPublisher,
    getSubscriber,
    getStaleCache,
    getCachePurgeChannel,

    init,
    wrap,
    listCurrentCacheKeys,
    fastPurge,
    purge,
    cachePurgeChannel,

    multiCache,
    staleCache,
    publisher,
    memoryCache,
    rateLimiter,
    redisCache,
    subscriber }
    = cacheService
export default cacheService;