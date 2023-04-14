import { debugModule } from '@mdrooker/rna-logger'
import { caching, multiCaching } from 'cache-manager';
import _ from "lodash";
import redisStore from "cache-manager-ioredis-yet";
import Redis from "ioredis";
import { default as subscriptionInit } from '../../src/lib/subscription.js'
import { validateConfig } from '../../src/lib/utils.js';


const debug = debugModule('rna:cache-manager');

let _config;
let MEMORY_CACHE_TIME;
let MEMORY_CACHE_SIZE;
let redisConfigOptions;
let cachePurgeChannel;
let cacheKeyPrefix;
let rateLimitPrefix
let keyPrefix;
let _options;
let redisConfig;
let _redisCache;
let _memoryCache;
let client;
let _staleCache;
let _rateLimiter;
let _publisher;
let _subscriber;

let multiCache;
function isCacheableValue(value) {
    if (_.isObject(value)) {
        return true;
    }
    else {
        return false;
    }
};
async function listCurrentCacheKeys() {
    let cacheKey = `${cacheKeyPrefix}*`;
    let currentKeys = [];
    return new Promise((resolve, reject) => {
        try {
            debug(`Looking for ${cacheKey}`);
            let stream = publisher.scanStream({
                match: `${cacheKey}`,
                // returns approximately 100 elements per call
                count: 500,
            });
            stream.on('data', function (keys) {
                // `keys` is an array of strings representing key names
                debug(`Found keys of  ${keys.length} size`);
                if (keys.length) {
                    keys.forEach(function (key) {
                        currentKeys.push(key);
                    });
                }
            });
            stream.on('end', function () {

                resolve(currentKeys);
            });
        }
        catch (error) {
            console.log(error);
        }
    });
}
async function fastPurge({ key, isMaster }) {
    let cacheKey = `${cacheKeyPrefix}:*${key}*`;
    return new Promise((resolve, reject) => {
        try {
            debug(`Looking for ${cacheKey}`);
            let stream = publisher.scanStream({
                match: `${cacheKey}`,
                // returns approximately 500 elements per call
                count: 500,
            });
            stream.on('data', function (keys) {
                // `keys` is an array of strings representing key names
                appDebug(`Found keys of  ${keys.length} size`);
                if (keys.length) {
                    keys.forEach(function (key) {
                        appDebug(`Purging Key ${key}`);
                        memoryCache.del(key); //if you delete it from memory...its just gonna go back to redis
                        if (isMaster) {
                            redisCache.del(key);
                        }
                        publisher.publish(cachePurgeChannel, key);
                    });
                }
            });
            stream.on('end', function () {
                resolve();
            });
        }
        catch (error) {
            console.log(error);
        }
    });
}
async function wrap({ cache, key, metricName, retreiver, metrics, ttl }) {
    if (metrics) {
        metrics.counter({ name: metricName });
    }
    debugger
    let cacheKey = `${cacheKeyPrefix}:${key}`;
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
    let staleCacheEntry = await staleCache.get(cacheKey);
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
                        metrics.counter({ name: `resolve.${metricName}.stale` });
                        staleCache.set(`${cacheKey}:hit`, staleCache.get(`${cacheKey}:hit`) + 1);
                    }
                    return staleCacheEntry;
                }
            }
        }
    } else {
        let returnPayload = cache.wrap(cacheKey, async function () {
            debug(`${metricName} - from source`);
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
                            metrics.counter({ name: `resolve.${metricName}.stale` });
                            staleCache.set(`${cacheKey}:hit`, staleCache.get(`${cacheKey}:hit`) + 1);
                        }
                        return staleCacheEntry;
                    }
                }
            }
            let getElapsed = Date.now() - t2;
            if (metrics) {
                metrics.histogram({ name: `resolve.${metricName}.elapsed`, value: getElapsed });
                metrics.counter({ name: `resolve.${metricName}.database` });
            }
            staleCache.set(cacheKey, returnPayload);

            return returnPayload;
        }, opts.cacluateTTL(ttl)).then(payload => {
            console.log("Getting from cache")
            debug(`${metricName} - Getting from Cache`);
            if (metrics) {
                metrics.counter({ name: `resolve.${metricName}.cache` });
            }
            if (!staleCacheEntry) {
                staleCache.set(cacheKey, payload);
            }

            return payload;
        });
        return returnPayload;
    }
}
async function purge({ key, isMaster }) {
    let cacheKey = `${keyPrefix}:${key}`;
    let keysToBePurged;
    try {
        debug(`Searching for Keys to purge ${cacheKey}`);
        keysToBePurged = await redisCache.keys(cacheKey);
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

function getStaleCache() {
    if (!_staleCache) {
        return _staleCache
    } else {
        throw new Error('Stale Cache not initialized');
    }
}
function getRateLimiter() {
    if (!_rateLimit) {
        return _rateLimit
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
function getSubscriber() {
    if (!_subscriber) {
        return _subscriber
    } else {
        debug('Subscriber not initialized..retrying');
        _subscriber = new Redis(_options);
        if (!_subscriber) {
            throw new Error('Subscriber not initialized')
        }
        else {
            return _subscriber
        }
    }
}
function getPublisher() {
    if (_publisher) {
        return _publisher
    } else {
        debug('Publisher not initialized..Retrying');
        _publisher = new Redis(_options);
        if (!_publisher) {
            throw new Error('Publisher not initialized')
        } else {
            return _publisher

        }
    }
}
const parseConfig = ({ config }) => {
    if (config && validateConfig(config)) {
        redisConfig = config.CACHE.REDIS;
        redisConfigOptions = {
            host: redisConfig.URL,
            port: redisConfig.PORT,
            password: redisConfig.PASSWORD ? redisConfig.PASSWORD : null,
            auth_pass: redisConfig.PASSWORD,
            compress: true,
            connectTimeout: 20000,
            maxRetriesPerRequest: 10
        }
        MEMORY_CACHE_TIME = config.CACHE.MEMORY.CACHE_TIME;
        MEMORY_CACHE_SIZE = config.CACHE.MEMORY.CACHE_SIZE;
        keyPrefix = `app:${config.NAME.SYSTEM}:${config.NAME.PRODUCT}:${config.ENVIRONMENT}:`;
        cacheKeyPrefix = `${keyPrefix}:cache`;
        cachePurgeChannel = `${keyPrefix}:purge`;
        rateLimitPrefix = `${keyPrefix}:ratelimit`;
        return true
    } else {
        return false
    }

}
let init = async function ({ config }) {
    debugger

    if (parseConfig({ config })) {
        let rstore = await redisStore.redisStore({
            host: redisConfig.URL,
            port: redisConfig.PORT,
            password: redisConfig.PASSWORD ? redisConfig.PASSWORD : null,
            auth_pass: redisConfig.PASSWORD,
            compress: true,
            connectTimeout: 20000,
            maxRetriesPerRequest: 10
        })
        _redisCache = await caching(rstore,
            {
                isCacheableValue: isCacheableValue
            }),
            _memoryCache = await caching('memory', { store: 'memory', max: MEMORY_CACHE_SIZE, ttl: MEMORY_CACHE_TIME }),
            _staleCache = await caching('memory', { store: 'memory', max: MEMORY_CACHE_SIZE, ttl: MEMORY_CACHE_TIME }),
            _options = {
                connectionName: 'redis',
                host: redisConfig.URL,
                port: redisConfig.PORT,
                password: redisConfig.PASSWORD ? redisConfig.PASSWORD : null,
                connectTimeout: 20000,
                compress: true,
                maxRetriesPerRequest: 10
            };
        client = new Redis({ ..._options, keyPrefix: keyPrefix });
        _rateLimiter = new Redis({ ..._options, keyPrefix: rateLimitPrefix });
        _publisher = new Redis(_options);
        _subscriber = new Redis(_options);
        multiCache = multiCaching([_memoryCache, _redisCache]);
        debug(`Using InMemory Cache of ${MEMORY_CACHE_TIME} seconds`);
        debug(`Using Redis Cache of ${redisConfig.CACHE_TIME} seconds`);
        subscriptionInit({ config: redisConfigOptions, publisher: _publisher })
    } else {
        throw new Error('Invalid Config')
    }
}



export default init
export { purge, fastPurge, listCurrentCacheKeys, wrap }
export { getPublisher, getSubscriber, getRateLimiter, getStaleCache }
export { multiCache, _memoryCache as memoryCache, client }
