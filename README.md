# Streaming over Redis database Or S3 object storage as message queue (http mode is experimental)
This is a really simple VPN that uses Redis or AWS S3 Object storage as message bus and routes your traffic through your VPN server with it.
It uses AES-256-GCM encryption too now.
Also I added http mode too, which you can use a cdn/api destination for tunneling, it requires more testing.

# Installation
Put outside directory in your vps and run these commands
```
npm install
```
Then put your redis database string in config, use rediss database strings for enabling TLS encryptions
After that
```
npm run dev
```

Sample config.json that should be in current directory you run the proxy client
```
{
    "connstring": "redis url",
    "tls": "if you have tls enabled put your redis destination host here",
    "symmetricKey": "generate a 32 byte symmetric key and turn to hex",
    "mode": "s3 or redis",
    "secretKey": "if you use s3 you need your secret key",
    "accessKey": "you need access key for s3",
    "endpointUrl": "your s3 object storage url here",
    "zone": "the zone",
    "bucket": "and your bucket",
    "pathstyle": true, // Just if you want add the bucket name like a url path at the end
    "deleteManual": true, // If your endpoint dosent support DeleteObjects command you need this
    "minimalClient": true, // If you want a client dont restrict http calls and manage open sockets and etc or your provider dosent support new sdks you need this 
    "pingAllT": false, // some redis endpoints get timeout, i kept all redis connections alive however it might not help
    "prefix": "0", // this prefix should be in config of vpn server
    "hostname": "e.g cdn, your api destination"
}
```

config.json sample for vpn server
```
{
    "connstring": "redis url",
    "tls": "if you have tls enabled put your redis destination host here",
    "symmetricKey": "generate a 32 byte symmetric key and turn to hex",
    "mode": "s3 or redis",
    "secretKey": "if you use s3 you need your secret key",
    "accessKey": "you need access key for s3",
    "endpointUrl": "your s3 object storage url here",
    "zone": "the zone",
    "bucket": "and your bucket",
    "pathstyle": true, // Just if you want add the bucket name like a url path at the end
    "deleteManual": true, // If your endpoint dosent support DeleteObjects command you need this
    "minimalClient": true, // If you want a client dont restrict http calls and manage open sockets and etc or your provider dosent support new sdks you need this,
    "pingAllT": false, // some redis endpoints get timeout, i kept all redis connections alive however it might not help
    "prefixes": [
        "0",
        "1"
    ],
    "concurrency": 2500 // how much a prefix can make callback (socket)
}
```


# Important
Well this is for personal use, if IT ISNT you should make a front api that monitors user traffic, and putting backpressure on it
The api you use should put specific limit (backpressure, can be changed by speed of message bus in vpn server (v1) and client proxy (v2), and RTT)
speed is getting calculated each minute and batches use 0 as their start, the api can be even a v2ray server that talks to client proxy with unix sockets, or the client proxy itself
can become the api, there can be any amount of redis db, s3 storage, vpn server, client proxy, the api route users with credintals known in another storage, and forwards them to these
what i choose unix sockets with api for production (just need to be sockets within the local server, unix socket phrase its just determining we should not have latency in our local machine)
because the code base becomes scalable and can be used for both personal use and production use, this is just a roadmap for who willing to benefit from this which i dont support them
and i should be the guy who benefits everything, so why i share my fucking cool tools? because im idiot

Ok i built the api, it dosen't have backpressure because i don't need this front api but the core idea is clear [https://github.com/Shayegan8/secap](https://github.com/Shayegan8/secap)
Well you might (none) wonder why i change the bandwidth with client not server side
well lets say we do it reverse just like usual, well server sends the chunks it reads and client sends ack after that, in this period we consider it as RTT, well meanwhile we are
listening for ack, we are also listening for bandwidth so we still read with old bandwidth
in our scenario, we say client says what bandwidth it wants, you see? both have same process of getting something, my scenario gets bandwidth number and connectionID from client
in usual way it just gets ack from client, both have approximately same RTT, so it dosen't matter what scenario we use

# On mobile
Well I use typescript 5.7 version in termux with **SocksTun** and it works well

QA:
- Is it fast?: well it depends on your **infrastructure**, if your server has a limit that cuts off chunks n by n, if your server has a slow network interface speed, if your redis or s3 responds slowly, all of these are factors, if you want something really fast, well your message buses should be fast, and so as your internet and so as your servers
- But redis and s3 have latency and blablabla: Well everything has latency idiot, the whole backend of every website has latency by everything they use what you mean anyway
- Do you use this repository for your daily?: Yes all the day and i have good **infrastructure** which all of them are really really cheap

MAYBE i add a mechanism that you can use both s3 and redis, like sending your chunks with redis database and getting them with s3 and backwards, maybe i add udp ip spoof tunneling
