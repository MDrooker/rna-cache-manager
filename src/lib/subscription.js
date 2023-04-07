import { debugModule } from '@mdrooker/rna-logger'
import Redis from "ioredis";
import { validateConfig } from './utils.js';

const debug = debugModule('rna:cache-manager:subscription');
let redisConfigOptions = null;
let subscriberMap = new Map();
let _publisher = null

async function subscribeToRedis({ channel, handler }) {
    let channelSubscriber = new Redis(redisConfigOptions);
    channelSubscriber.subscribe(channel, function (err, count) {
        if (err) {
            console.error(err);
        }
    });
    channelSubscriber.on("message", (channel, message) => {
        let channelHandler = subscriberMap.get(channel);
        channelHandler({ channel, message });
    });
    subscriberMap.set(channel, handler);
    debug(`Subscribing to Channel ${channel}.  Current Subscriber Count ${subscriberMap.size}`);
}
async function publishToRedis({ channel, message }) {
    let count = await _publisher.publish(channel, JSON.stringify(message));
    debug(`Published ${JSON.stringify(message)} to ${count} clients on ${channel}`);
}

let init = async function ({ config, publisher }) {
    if (config && validateConfig({ config })) {
        redisConfigOptions = config;
        _publisher = publisher;
        return;
    } else {
        throw new Error('Invalid Config');
    }
}
export default init
export { subscribeToRedis, publishToRedis }