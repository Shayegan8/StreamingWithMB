import net from 'net'
import crypto from 'crypto'
import { exit } from 'process'
import { Redis } from 'ioredis'
import PQueue from 'p-queue'
import config from "../config.json" with {type: 'json'}
import { S3Client as S3Light } from '@bradenmacdonald/s3-lite-client'
import { DeleteObjectCommand, DeleteObjectsCommand, GetObjectCommand, ListObjectsV2Command, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import https from 'https'

const mode = config.mode

const sclient = new S3Light({
    endPoint: config.endpointUrl,
    region: config.zone,
    accessKey: config.accessKey,
    bucket: config.bucket,
    pathStyle: config.pathstyle,
    secretKey: config.secretKey
})

const justForDelete = new S3Client({
    region: config.zone,
    endpoint: config.endpointUrl,
    credentials: {
        accessKeyId: config.accessKey,
        secretAccessKey: config.secretKey,
    },
    requestHandler: {
        httpsAgent: new https.Agent({
            keepAlive: true,
            keepAliveMsecs: 30_000,
            maxSockets: 512,
            maxFreeSockets: 256,
            timeout: 60_000,
        }),
        connectionTimeout: 5_000,
        socketTimeout: 60_000
    },
    retryMode: 'adaptive',
    maxAttempts: 8,
    forcePathStyle: config.pathstyle
})

const s3g = new S3Client({
    region: config.zone,
    endpoint: config.endpointUrl,
    credentials: {
        accessKeyId: config.accessKey,
        secretAccessKey: config.secretKey,
    },
    requestHandler: {
        httpsAgent: new https.Agent({
            keepAlive: true,
            keepAliveMsecs: 30_000,
            maxSockets: 512,
            maxFreeSockets: 256,
            timeout: 60_000,
        }),
        connectionTimeout: 5_000,
        socketTimeout: 60_000
    },
    retryMode: 'adaptive',
    maxAttempts: 8,
    forcePathStyle: config.pathstyle
})

logger("Path style: " + config.pathstyle)

let toDelete = new Set<string>()
let toDeletePQueue = new PQueue({ concurrency: 100 })
if (mode == "s3")
    setInterval(async () => {
        const toDeleteCopy = toDelete
        if (toDeleteCopy.size != 0) {
            for (const connectionID of toDelete) {
                try {
                    if (config.minimalClient) {
                        const ls = await Array.fromAsync(sclient.listObjects({ prefix: `appserver,${connectionID}/` }), (entry) => entry.key)
                        if (!ls.length) {
                            logger(`Nothing to delete`, "info")
                            continue
                        }
                        await Promise.all(ls.map(async key => await toDeletePQueue.add(async () => await sclient.deleteObject(key))))
                    } else {
                        const data2 = await justForDelete.send(new ListObjectsV2Command({
                            Bucket: bucketName,
                            Prefix: `appserver,${connectionID}/`,
                        }))

                        const sagjerk2: { Key: string }[] = []
                        if (data2.Contents && data2.Contents.length != 0) {
                            for (const element of data2.Contents)
                                sagjerk2.push({ Key: element.Key! })
                            if (!config.deleteManual) {
                                try {
                                    await justForDelete.send(
                                        new DeleteObjectsCommand({
                                            Bucket: bucketName,
                                            Delete: {
                                                Objects: sagjerk2,
                                            },
                                        })
                                    )
                                } catch (e) {
                                    logger("Failed to delete with DeleteObjectsCommand trying with DeleteObject", "info")
                                    try {
                                        await Promise.all(sagjerk2.map(async (key) => {
                                            await justForDelete.send(new DeleteObjectCommand({
                                                Bucket: bucketName,
                                                Key: key.Key
                                            }))
                                        }))
                                    } catch (e) {
                                        logger("Ok this failed too? why?")
                                        logger(e as any)
                                    }
                                }
                            } else {
                                try {
                                    await Promise.all(sagjerk2.map(async (key) => {
                                        await justForDelete.send(new DeleteObjectCommand({
                                            Bucket: bucketName,
                                            Key: key.Key
                                        }))
                                    }))
                                } catch (e) {
                                    logger("Ok this failed too? why?")
                                    logger(e as any)
                                }
                            }
                        }
                    }
                } catch (e) {
                    logger(`failed to delete ${connectionID}: ${e}`, "error")
                }
            }
            toDelete.clear()
        }
    }, 10000)

const bucketName = config.bucket

let conn: Redis | null
if (mode != 's3')
    if (config.tls == "")
        conn = new Redis(config.connstring, {
            maxRetriesPerRequest: null,
            keepAlive: 10000,
        })
    else
        conn = new Redis(config.connstring, {
            maxRetriesPerRequest: null,
            keepAlive: 10000,
            tls: { servername: config.tls }
        })

let ack: Redis | null
if (mode != 's3')
    if (config.tls == "")
        ack = new Redis(config.connstring, {
            maxRetriesPerRequest: null,
            keepAlive: 10000,
        })
    else
        ack = new Redis(config.connstring, {
            maxRetriesPerRequest: null,
            keepAlive: 10000,
            tls: { servername: config.tls }
        })

if (mode != 's3')
    try {
        await conn!.ping()
        await ack!.ping()
    } catch (e) {
        logger(`conn ping error: ${e}`, "error")
    }


function logger(param: string, type?: string) {
    const date = new Date(Date.now())
    console.log(type == "info" ? `[\x1b[33mINFO\x1b[0m] [\x1b[32m${mode}\x1b[0m] ${date.getHours()}:${date.getMinutes()}:${date.getSeconds()} ${param}`
        : (type == "error" ? `[\x1b[31mERR\x1b[0m] [\x1b[32m${mode}\x1b[0m] ${date.getHours()}:${date.getMinutes()}:${date.getSeconds()} ${param}` : param))
}

const symmetricKey = Buffer.from(config.symmetricKey, "hex")

const popperBuffer = async (key: string, connectionID: string, abrt: AbortController) => {
    let delay = 20
    for (let i = 0; i < 200; i++) {
        try {
            if (abrt.signal.aborted) {
                toDelete.add(connectionID)
                break
            }
            let bod: Uint8Array<ArrayBufferLike>
            if (config.minimalClient) {
                const data = await sclient.getObject(key)
                bod = new Uint8Array(await data.arrayBuffer())
            } else {
                const data = await s3g.send(new GetObjectCommand({ Bucket: bucketName, Key: key }))
                bod = await data.Body!.transformToByteArray()
            }
            logger(`Batch received for ${key}`, "info")
            const extractIv = bod.subarray(0, 12)
            const tag = bod.subarray(12, 28)
            const encryptedChunk = bod.subarray(28)
            const decipher = crypto.createDecipheriv("aes-256-gcm", symmetricKey, extractIv)
            decipher.setAuthTag(tag)
            const decryptedChunk = Buffer.concat([decipher.update(encryptedChunk), decipher.final()])
            const realMsg = decryptedChunk.subarray(10)
            if (!Buffer.from('end', 'binary').compare(realMsg)) {
                toDelete.add(connectionID)
                abrt.abort()
                break
            }
            return bod
        } catch (e) {
            await new Promise(r => setTimeout(r, 500))
            delay = Math.min(delay * 2, 500)
        }
    }
}

const server = net.createServer((socket) => {
    socket.on('error', (err) => {
        logger(`Client error: ${err.message}`, "error")
    })
    socket.once('data', (data1: Buffer) => {
        if (data1[0] != 0x05)
            return socket.end()
        const packet_l = data1.length
        if (packet_l >= 3) { // VER
            const reply = Buffer.alloc(2)
            reply[0] = 0x05 // VER
            reply[1] = 0x00 // NO AUTHENTICATION REQUIRED ONLY
            socket.write(reply)
        }
        socket.once('data', async (data: Buffer) => {
            if (data[2] == 0x00 && data.length >= 10) {
                // DST.ADDR, DST.PORT
                let DSTADDR: string
                let DSTPORT
                let portOffset
                switch (data[3]) { // ATYP
                    case 0x01: // IPv4
                        DSTADDR = [data[4], data[5], data[6], data[7]].join('.')
                        portOffset = 8
                        break
                    case 0x03: // DOMAINNAME
                        const numberOfBytes = data.readUInt8(4)
                        DSTADDR = data.subarray(5, 5 + numberOfBytes).toString()
                        portOffset = 5 + numberOfBytes
                        break
                    case 0x04: // IPv6
                        let IPv6: string[] = []
                        for (let i = 4; i < 20; i += 2)
                            IPv6.push(data.readUInt16BE(i).toString(16))
                        DSTADDR = IPv6.join(':')
                        portOffset = 20
                        break
                    default:
                        return
                }
                DSTPORT = data.readUInt16BE(portOffset)
                const ATYP = data[3]
                logger(`ATYP: ${Buffer.from(ATYP.toString()).toString('binary')}, Address: ${DSTADDR}:${DSTPORT}`, "info")
                logger(`CMD: ${Buffer.from(data[1]!.toString()).toString('binary')}`, "info")
                switch (data[1]) { // cmd
                    case 0x01: // CONNECT
                        let pqueue = new PQueue({ concurrency: 1 })
                        let buff: Buffer[] = []
                        let timejerk: NodeJS.Timeout
                        let rtt = 0


                        let max = 2 * 1024 * 1024
                        let inSeq = "0"
                        const pqueueMax = new PQueue({ concurrency: 1 })
                        let sent = false
                        let inatervo: NodeJS.Timeout | null
                        socket.on('data', (data: Buffer) => {
                            if (timejerk)
                                clearTimeout(timejerk)
                            pqueue.add(() => {
                                buff.push(data)
                            })
                            timejerk = setTimeout(async () => {
                                pqueue.add(async () => {
                                    logger(`Pushing batch to proxy,${connectionID}`, "info")
                                    let msg: Buffer<ArrayBuffer> | null
                                    let newVersion: Buffer
                                    if (mode != "s3")
                                        msg = Buffer.concat(buff)
                                    else {
                                        const concatious = Buffer.concat(buff)
                                        const preMsg = Buffer.alloc(10 + concatious.length)
                                        const jerk = crypto.randomBytes(10)
                                        newVersion = jerk
                                        jerk.copy(preMsg, 0, 0, 10)
                                        concatious.copy(preMsg, 10, 0)
                                        msg = preMsg
                                    }
                                    const iv = crypto.randomBytes(12)
                                    const cipher = crypto.createCipheriv("aes-256-gcm", symmetricKey, iv)
                                    const encryptedMsg = Buffer.concat([cipher.update(msg), cipher.final()])
                                    const tag = cipher.getAuthTag()
                                    if (conn) {
                                        await conn.lpush(`proxy,${connectionID}`, Buffer.concat([iv, tag, encryptedMsg]))
                                    } else {
                                        rtt = Date.now()
                                        if (config.minimalClient) {
                                            await sclient.putObject(`proxy,${connectionID}/${inSeq}`, Buffer.concat([iv, tag, encryptedMsg])).catch((reason) => {
                                                logger(`Problem with pushing batch after informing ${reason}`, "error")
                                            })
                                        } else {
                                            await s3g.send(new PutObjectCommand({
                                                Bucket: bucketName,
                                                Key: `proxy,${connectionID}/${inSeq}`,
                                                ACL: 'private',
                                                Body: Buffer.concat([iv, tag, encryptedMsg]),
                                            })).catch((reason) => {
                                                logger(`Problem with pushing batch after informing ${reason}`, "error")
                                            })
                                        }
                                        logger(`It took ${Date.now() - rtt}ms for pushing batch`, "info")
                                        inSeq = newVersion!.toString('hex')
                                    }
                                    buff = []
                                    sent = true
                                    if (config.ackS3 || mode != "s3")
                                        pqueueMax.add(async () => {
                                            const msgACK = Buffer.from(`${max}`, 'binary')
                                            const ivACK = crypto.randomBytes(12)
                                            const cipherACK = crypto.createCipheriv("aes-256-gcm", symmetricKey, ivACK)
                                            const encryptedMsgACK = Buffer.concat([cipherACK.update(msgACK), cipherACK.final()])
                                            const tagACK = cipherACK.getAuthTag()
                                            if (conn) {
                                                rtt = Date.now()
                                                await conn.lpush(`ack,${connectionID}`, Buffer.concat([ivACK, tagACK, encryptedMsgACK]))
                                            } else {
                                                if (config.minimalClient) {
                                                    await sclient.putObject(`ack,${connectionID}`, Buffer.concat([ivACK, tagACK, encryptedMsgACK])).catch((reason) => {
                                                        logger(`Problem with pushing batch after informing ${reason}`, "error")
                                                    })
                                                } else {
                                                    await s3g.send(new PutObjectCommand({
                                                        Bucket: bucketName,
                                                        Key: `ack,${connectionID}`,
                                                        ACL: 'private',
                                                        Body: Buffer.concat([ivACK, tagACK, encryptedMsgACK]),
                                                    })).catch((reason) => {
                                                        logger(`Problem with pushing batch after informing ${reason}`, "error")
                                                    })
                                                }
                                            }
                                        })
                                })
                            }, 100)
                        })
                        let connectionID = crypto.randomUUID()
                        const server_reply = Buffer.alloc(10)
                        server_reply[0] = 0x05 // VER
                        server_reply[1] = 0x00 // REP
                        server_reply[2] = 0x00 // RSV
                        server_reply[3] = 0x01
                        // dummy bound address, its tough to receive this
                        server_reply[4] = 0
                        server_reply[5] = 0
                        server_reply[6] = 0
                        server_reply[7] = 0
                        // dummy port
                        server_reply[8] = 0
                        server_reply[9] = 0
                        socket.write(server_reply)
                        logger(`CONNECT done for ${connectionID} with ${DSTADDR} destination`, "info")

                        logger("Informing for " + connectionID, "info")
                        const msg = Buffer.from(`${DSTADDR},${DSTPORT},${connectionID},${ATYP}`, 'binary')
                        const iv = crypto.randomBytes(12)
                        const cipher = crypto.createCipheriv("aes-256-gcm", symmetricKey, iv)
                        const encryptedMsg = Buffer.concat([cipher.update(msg), cipher.final()])
                        const tag = cipher.getAuthTag()
                        if (conn)
                            await conn.lpush(`inform`, Buffer.concat([iv, tag, encryptedMsg]))
                        else {
                            const key = crypto.randomBytes(10).toString('hex')
                            if (config.minimalClient) {
                                await sclient.putObject(`informs/${key}`, Buffer.concat([iv, tag, encryptedMsg])).catch((reason) => {
                                    logger(`Problem with pushing inform ${reason}`, "error")
                                })
                            } else {
                                await s3g.send(new PutObjectCommand({
                                    Bucket: bucketName,
                                    Key: `informs/${key}`,
                                    ACL: 'private',
                                    Body: Buffer.concat([iv, tag, encryptedMsg]),
                                })).catch((reason) => {
                                    logger(`Problem with pushing batch after informing ${reason}`, "error")
                                })
                            }
                        }
                        if (config.ackS3 || mode != "s3") {
                            inatervo = setInterval(async () => {
                                if (!sent) {
                                    sent = true
                                    rtt = Date.now()
                                    pqueueMax.add(async () => {
                                        const msgACK = Buffer.from(`${max}`, 'binary')
                                        const ivACK = crypto.randomBytes(12)
                                        const cipherACK = crypto.createCipheriv("aes-256-gcm", symmetricKey, ivACK)
                                        const encryptedMsgACK = Buffer.concat([cipherACK.update(msgACK), cipherACK.final()])
                                        const tagACK = cipherACK.getAuthTag()
                                        if (conn)
                                            await conn.lpush(`ack,${connectionID}`, Buffer.concat([ivACK, tagACK, encryptedMsgACK]))
                                        else {
                                            if (config.minimalClient) {
                                                await sclient.putObject(`ack,${connectionID}`, Buffer.concat([ivACK, tagACK, encryptedMsgACK])).catch((reason) => {
                                                    logger(`Problem with pushing inform ${reason}`, "error")
                                                })
                                            } else {
                                                await s3g.send(new PutObjectCommand({
                                                    Bucket: bucketName,
                                                    Key: `ack,${connectionID}`,
                                                    ACL: 'private',
                                                    Body: Buffer.concat([ivACK, tagACK, encryptedMsgACK]),
                                                })).catch((reason) => {
                                                    logger(`Problem with pushing ack after informing ${reason}`, "error")
                                                })
                                            }
                                        }
                                    })
                                }
                            }, 100)
                        }
                        let blconn: Redis | null
                        const ctl = new AbortController()
                        if (mode != "s3")
                            if (config.tls == "")
                                blconn = new Redis(config.connstring, {
                                    maxRetriesPerRequest: null,
                                    keepAlive: 10000,
                                })
                            else
                                blconn = new Redis(config.connstring, {
                                    maxRetriesPerRequest: null,
                                    keepAlive: 10000,
                                    tls: { servername: config.tls }
                                })
                        let pinger: NodeJS.Timeout | null
                        if (mode != "s3")
                            pinger = setInterval(async () => {
                                try {
                                    await blconn!.ping()
                                } catch (e) {
                                    logger("pinger: " + e, "info")
                                    clearInterval(pinger!)
                                    clearInterval(inatervo!)
                                    clearImmediate(imedo)
                                    conn!.del(`ack,${connectionID}`)
                                    conn!.del(`appserver,${connectionID}`)
                                    await conn!.del(`proxy,${connectionID}`)
                                    socket.end()
                                }
                            }, 10000)

                        socket.once('error', (e) => {
                            logger(`Client error: ${e}`, "error")
                            if (pinger)
                                clearInterval(pinger)
                            if (inatervo)
                                clearInterval(inatervo)
                            clearImmediate(imedo)
                            if (blconn)
                                blconn.quit().catch(() => { })
                        })

                        socket.on('end', async () => {
                            await pqueue.add(async () => {
                                logger(`Sending half close signal to proxy,${connectionID}`, "info")
                                const msg = Buffer.from('end', 'binary')
                                const iv = crypto.randomBytes(12)
                                const cipher = crypto.createCipheriv("aes-256-gcm", symmetricKey, iv)
                                const encryptedMsg = Buffer.concat([cipher.update(msg), cipher.final()])
                                const tag = cipher.getAuthTag()
                                if (conn) {
                                    conn.del(`ack,${connectionID}`)
                                    conn.del(`appserver,${connectionID}`)
                                    await conn.lpush(`proxy,${connectionID}`, Buffer.concat([iv, tag, encryptedMsg]))
                                } else {
                                    try {
                                        if (config.minimalClient) {
                                            if (config.ackS3)
                                                await sclient.deleteObject(`ack,${connectionID}`)
                                            await sclient.putObject(`proxy,${connectionID}/${inSeq}`, Buffer.concat([iv, tag, encryptedMsg]))
                                        } else {
                                            if (config.ackS3)
                                                await s3g.send(
                                                    new DeleteObjectCommand({
                                                        Bucket: bucketName,
                                                        Key: `ack,${connectionID}`,
                                                    })
                                                )
                                            await s3g.send(new PutObjectCommand({
                                                Bucket: bucketName,
                                                Key: `proxy,${connectionID}/${inSeq}`,
                                                ACL: 'private',
                                                Body: Buffer.concat([iv, tag, encryptedMsg]),
                                            }))
                                        }
                                    } catch (e) {
                                        logger("Problem with s3g " + e, "error")
                                    }
                                    if (pinger)
                                        clearInterval(pinger)
                                    if (inatervo)
                                        clearInterval(inatervo)
                                    clearImmediate(imedo)
                                    ctl.abort()
                                    if (blconn)
                                        blconn.quit().catch(() => { })
                                }
                            })
                        })

                        if (mode != "s3")
                            blconn!.on('error', () => {
                                logger("blconn error event: " + connectionID, "error")
                                clearInterval(pinger!)
                                clearInterval(inatervo!)
                                clearImmediate(imedo)
                                conn!.del(`ack,${connectionID}`)
                                conn!.del(`appserver,${connectionID}`)
                                conn!.del(`proxy,${connectionID}`)
                                blconn!.disconnect(false)
                                socket.end()
                            })

                        let outSeq = "0"
                        const imedo = setImmediate(async () => {
                            while (!ctl.signal.aborted) {
                                try {
                                    let response: ArrayBuffer | Uint8Array<ArrayBufferLike> | undefined
                                    if (blconn)
                                        response = (await blconn.brpopBuffer(`appserver,${connectionID}`, 20))?.[1]
                                    else {
                                        response = await popperBuffer(`appserver,${connectionID}/${outSeq}`, connectionID, ctl)
                                    }
                                    if (!response) {
                                        logger(`server chunk for ${connectionID} is null`, "info")
                                        if (pinger)
                                            clearInterval(pinger)
                                        if (inatervo)
                                            clearInterval(inatervo)
                                        clearImmediate(imedo)
                                        if (blconn)
                                            blconn.quit().catch(() => { })
                                        const msg = Buffer.from('end', 'binary')
                                        const iv = crypto.randomBytes(12)
                                        const cipher = crypto.createCipheriv("aes-256-gcm", symmetricKey, iv)
                                        const encryptedMsg = Buffer.concat([cipher.update(msg), cipher.final()])
                                        const tag = cipher.getAuthTag()
                                        if (conn) {
                                            conn.del(`ack,${connectionID}`)
                                            conn.del(`appserver,${connectionID}`)
                                            await conn.lpush(`proxy,${connectionID}`, Buffer.concat([iv, tag, encryptedMsg]))
                                        } else {
                                            try {
                                                if (config.minimalClient) {
                                                    if (config.ackS3)
                                                        await sclient.deleteObject(`ack,${connectionID}`)
                                                    await sclient.putObject(`proxy,${connectionID}/${inSeq}`, Buffer.concat([iv, tag, encryptedMsg]))
                                                } else {
                                                    if (config.ackS3)
                                                        await s3g.send(
                                                            new DeleteObjectCommand({
                                                                Bucket: bucketName,
                                                                Key: `ack,${connectionID}`,
                                                            })
                                                        )

                                                    await s3g.send(new PutObjectCommand({
                                                        Bucket: bucketName,
                                                        Key: `proxy,${connectionID}/${inSeq}`,
                                                        ACL: 'private',
                                                        Body: Buffer.concat([iv, tag, encryptedMsg]),
                                                    }))
                                                }
                                            } catch (e) {
                                                logger("Problem with s3g " + e, "error")
                                            }
                                        }
                                        toDelete.add(connectionID)
                                        ctl.abort()
                                        socket.end()
                                        break
                                    }
                                    sent = false
                                    if (config.ackS3 || mode != "s3")
                                        pqueueMax.add(() => {
                                            const meseaured = Date.now() - rtt
                                            if (meseaured > 10000)
                                                max = Math.max((max / 2), 256 * 1024)
                                            else
                                                if (max < (1024 * 1024 * 2))
                                                    max += (500 * 1024)
                                            if (max > (2 * 1024 * 1024))
                                                max = Math.max((max / 2), 256 * 1024)
                                            logger(`RTT ${connectionID}:${String(Math.fround(meseaured / (1000))).slice(0, 5)}s for received packet with length of ${Math.fround(response!.length / (1024 * 1024))}mb`, "info")
                                        })

                                    logger(`RTT ${connectionID}:${String(Math.fround((Date.now() - rtt) / (1000))).slice(0, 5)}s for received packet with length of ${Math.fround(response!.length / (1024 * 1024))}mb`, "info")

                                    const extractIv = response!.subarray(0, 12)
                                    const tag = response!.subarray(12, 28)
                                    const encryptedChunk = response!.subarray(28)
                                    const decipher = crypto.createDecipheriv("aes-256-gcm", symmetricKey, extractIv)
                                    decipher.setAuthTag(tag)
                                    let decryptedChunk = Buffer.concat([decipher.update(encryptedChunk), decipher.final()])
                                    let version: string | null
                                    if (mode == "s3") {
                                        version = decryptedChunk.subarray(0, 10).toString('hex')
                                        outSeq = version
                                        const realMsg = decryptedChunk.subarray(10)
                                        decryptedChunk = realMsg
                                    }
                                    if (!Buffer.from('end', 'binary').compare(decryptedChunk)) {
                                        logger(`server chunk for ${connectionID} is null`, "info")
                                        if (pinger)
                                            clearInterval(pinger)
                                        if (inatervo)
                                            clearInterval(inatervo)
                                        clearImmediate(imedo)
                                        if (blconn)
                                            blconn.quit().catch(() => { })
                                        socket.end()
                                        const msg = Buffer.from('end', 'binary')
                                        const iv = crypto.randomBytes(12)
                                        const cipher = crypto.createCipheriv("aes-256-gcm", symmetricKey, iv)
                                        const encryptedMsg = Buffer.concat([cipher.update(msg), cipher.final()])
                                        const tag = cipher.getAuthTag()
                                        if (conn) {
                                            conn.del(`ack,${connectionID}`)
                                            conn.del(`appserver,${connectionID}`)
                                            await conn.lpush(`proxy,${connectionID}`, Buffer.concat([iv, tag, encryptedMsg]))
                                        } else {
                                            try {
                                                if (config.minimalClient) {
                                                    if (config.ackS3)
                                                        await sclient.deleteObject(`ack,${connectionID}`)
                                                    await sclient.putObject(`proxy,${connectionID}/${inSeq}`, Buffer.concat([iv, tag, encryptedMsg]))
                                                } else {
                                                    if (config.ackS3)
                                                        await s3g.send(
                                                            new DeleteObjectCommand({
                                                                Bucket: bucketName,
                                                                Key: `ack,${connectionID}`,
                                                            })
                                                        )
                                                    await s3g.send(new PutObjectCommand({
                                                        Bucket: bucketName,
                                                        Key: `proxy,${connectionID}/${inSeq}`,
                                                        ACL: 'private',
                                                        Body: Buffer.concat([iv, tag, encryptedMsg]),
                                                    }))

                                                }
                                            } catch (e) {
                                                logger("Problem with s3g " + e, "error")
                                            }
                                        }
                                        socket.end()
                                        break
                                    }
                                    socket?.write(decryptedChunk)
                                } catch (error) {
                                    if (mode != "s3") {
                                        clearInterval(pinger!)
                                        conn!.del(`ack,${connectionID}`)
                                        conn!.del(`proxy,${connectionID}`)
                                        await conn!.del(`appserver,${connectionID}`)
                                    } else {
                                        const msg = Buffer.from('end', 'binary')
                                        const iv = crypto.randomBytes(12)
                                        const cipher = crypto.createCipheriv("aes-256-gcm", symmetricKey, iv)
                                        const encryptedMsg = Buffer.concat([cipher.update(msg), cipher.final()])
                                        const tag = cipher.getAuthTag()
                                        if (conn) {
                                            conn.del(`ack,${connectionID}`)
                                            conn.del(`appserver,${connectionID}`)
                                            await conn.lpush(`proxy,${connectionID}`, Buffer.concat([iv, tag, encryptedMsg]))
                                        } else {
                                            try {
                                                if (config.minimalClient) {
                                                    if (config.ackS3)
                                                        await sclient.deleteObject(`ack,${connectionID}`)
                                                    await sclient.putObject(`proxy,${connectionID}/${inSeq}`, Buffer.concat([iv, tag, encryptedMsg]))
                                                } else {
                                                    if (config.ackS3)
                                                        await s3g.send(
                                                            new DeleteObjectCommand({
                                                                Bucket: bucketName,
                                                                Key: `ack,${connectionID}`,
                                                            })
                                                        )
                                                    await s3g.send(new PutObjectCommand({
                                                        Bucket: bucketName,
                                                        Key: `proxy,${connectionID}/${inSeq}`,
                                                        ACL: 'private',
                                                        Body: Buffer.concat([iv, tag, encryptedMsg]),
                                                    }))
                                                }
                                            } catch (e) {
                                                logger("Problem with s3g " + e, "error")
                                            }
                                        }
                                    }
                                    if (inatervo)
                                        clearInterval(inatervo)
                                    clearImmediate(imedo)
                                    socket.end()
                                    break
                                }
                            }
                        })
                        break
                    default:
                        break
                }
            }
        })
    })
})

if (mode != "s3")
    setInterval(async () => {
        try {
            await conn!.ping()
            await ack!.ping()
        } catch (e) {
            logger("gPinger: " + e, "info")
        }
    }, 10000)

process.on('uncaughtException', (error) => {
    logger(`Uncaught exception ${error}`, "error")
})

const finishCallback = async () => {
    logger("Stopping the server", "info")
    server.close()
    if (conn)
        await conn.flushdb()
    else {
        try {
            if (config.minimalClient) {
                const ls = await Array.fromAsync(sclient.listObjects({ prefix: "" }), (entry) => entry.key)
                await Promise.all(ls.map(async (key) => {
                    await sclient.deleteObject(key)
                }))
            } else {
                const data = await justForDelete.send(
                    new ListObjectsV2Command({
                        Bucket: bucketName,
                    })
                )
                let sagjerk: { Key: string }[] = []
                if (data.Contents && data.Contents.length != 0) {
                    for (const element of data.Contents)
                        sagjerk.push({ Key: element.Key! })
                    if (!config.deleteManual) {
                        try {
                            logger("OK SO HERE???")
                            await justForDelete.send(
                                new DeleteObjectsCommand({
                                    Bucket: bucketName,
                                    Delete: {
                                        Objects: sagjerk,
                                    },
                                })
                            )
                            logger("REALLLLY?")
                        } catch (e) {
                            logger("Failed to delete with DeleteObjectsCommand trying with DeleteObject", "info")
                            try {
                                await Promise.all(sagjerk.map(async (key) => {
                                    await justForDelete.send(new DeleteObjectCommand({
                                        Bucket: bucketName,
                                        Key: key.Key
                                    }))
                                }))
                            } catch (e) {
                                logger("Ok this failed too? why?")
                                logger(e as any)
                            }
                        }
                    } else {
                        try {
                            await Promise.all(sagjerk.map(async (key) => {
                                await justForDelete.send(new DeleteObjectCommand({
                                    Bucket: bucketName,
                                    Key: key.Key
                                }))
                            }))
                        } catch (e) {
                            logger("Ok this failed too? why?")
                            logger(e as any)
                        }
                    }
                }
            }
        } catch (reason) {
            logger(reason as any)
            logger(`Problem with getting all chunks or deleting them ${reason}`, "error")
        }
    }
    logger("Removing chunks completed", "info")
    exit(0)
}

process.on('SIGTERM', async () => {
    await finishCallback()
})

process.on('SIGINT', async () => {
    await finishCallback()
})

logger("Listening on 1080", "info")
server.listen(1080)

server.on('error', (error) => {
    logger(`Problem with connection with application server ${error.message}`, "error")
})
