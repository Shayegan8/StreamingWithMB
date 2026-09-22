# Streaming over Redis database Or S3 object storage as message queue
This is a really simple VPN that uses Redis or AWS S3 Object storage as message bus and routes your traffic through your VPN server with it.
It uses AES-256-GCM encryption too now. 

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

Sample config
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
    "ackS3": false,
    "bucket": "and your bucket",
    "pathstyle": true, // Just if you want add the bucket name like a url path at the end
    "deleteManual": true, // If your endpoint dosent support DeleteObjects command you need this
    "minimalClient": true, // If you want a client dont restrict http calls and manage open sockets and etc or your provider dosent support new sdks you need this 
}
```

Well this is for personal use, if IT ISNT you should make a front api that monitors user traffic, and putting backpressure on it

TODO:
- better ack logic for s3 mode, i'll put it in a same packet i send without sending another packet

Same operation applies for the proxy :)

oh btw the port is 1080

MAYBE i add a mechanism that you can use both s3 and redis, like sending your chunks with redis database and getting them with s3 and backwards, maybe i add udp ip spoof tunneling
