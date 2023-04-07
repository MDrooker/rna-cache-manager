import cacheManager, { listCurrentCacheKeys, fastPurge, wrap, subscribeToRedis } from '../src/index.js'
import config from './config/config.json'  assert { type: "json" };
const runner = async function () {
    let local = config.local
    await cacheManager.init({ config: local })
    let multiCache = cacheManager.multiCache;
    let cachePurgeChannel = cacheManager.cachePurgeChannel
    let data = await wrap({
        cache: multiCache,
        key: "helloworld",
        retreiver: async () => {
            return {
                name: "Matthew"
            }
        },
        ttl: 600
    });

    await subscribeToRedis({
        channel: cachePurgeChannel,
        handler: ({ channel, message }) => {
            console.log("CHANNEL", channel)
            console.log("MESSAGE", message)
        }
    })

    setInterval(async () => {
        let data = await wrap({
            cache: multiCache,
            key: "helloworld",
            retreiver: async () => {
                return {
                    name: "Matthew"
                }
            },
            ttl: 60
        });
        console.log(data)
    }, 1000);


    setInterval(async () => {
        await fastPurge({ key: "*", isMaster: true })
    }, 5000);
}

await runner()

console.log('HERE')