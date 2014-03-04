var redis = require("redis"),
    client = redis.createClient();

client.hgetall('xxx', redis.print);