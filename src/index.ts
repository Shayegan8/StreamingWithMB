import net from 'net'
import crypto from 'crypto'
import { exit } from 'process'
import { Redis } from 'ioredis'
import PQueue, { PriorityQueue, type QueueAddOptions } from 'p-queue'
import { DeleteObjectCommand, DeleteObjectsCommand, GetObjectCommand, ListObjectsCommand, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import config from "../config.json" with {type: 'json'}

const mode = config.mode

const s3 = new S3Client({
    region: config.zone,
    endpoint: config.endpointUrl,
    credentials: {
        accessKeyId: config.accessKey,
        secretAccessKey: config.secretKey,
    },
    requestHandler: {
        httpsAgent: { maxSockets: 10000 },
    }
});

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
        logger("conn ping error: " + e, "error")
    }

const s3PQueue = new PQueue({ concurrency: 1 })

function logger(param: string, type?: string) {
    const date = new Date(Date.now())
    console.log(type == "info" ? `[\x1b[33mINFO\x1b[0m] [\x1b[32m${mode}\x1b[0m] ${date.getHours()}:${date.getMinutes()}:${date.getSeconds()} ${param}`
        : (type == "error" ? `[\x1b[31mERR\x1b[0m] [\x1b[32m${mode}\x1b[0m] ${date.getHours()}:${date.getMinutes()}:${date.getSeconds()} ${param}` : param))
}

const connlist = new Map<string, any>()

const symmetricKey = Buffer.from(config.symmetricKey, "hex")

const popperBuffer = async (key: string) => {
    try {
        const data = await s3.send(new GetObjectCommand({ Bucket: bucketName, Key: key }))
        await s3.send(
            new DeleteObjectCommand({
                Bucket: bucketName,
                Key: key,
            })
        )
        logger("Seems like we actually got the buffer")
        return await data.Body!.transformToByteArray()
    } catch (e) {
        await new Promise<void>((resolve) => setTimeout(resolve, 100))
        return await popperBuffer(key)
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
                        let connectionID = crypto.randomUUID()
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
                            logger("Seems like we are sending inform WITH THIS HASH " + key)
                            await s3PQueue.add(async () => {
                                await s3.send(new PutObjectCommand({
                                    Bucket: bucketName,
                                    Key: `informs/${key}`,
                                    ACL: 'private',
                                    Body: Buffer.concat([iv, tag, encryptedMsg]),
                                })).catch((reason) => {
                                    logger(`Problem with pushing inform ${reason}`, "error")
                                })
                            })
                            logger("Seems like sending inform got passed")
                        }
                        let pqueue = new PQueue({ concurrency: 1 })
                        let buff: Buffer[] = []
                        socket.on('data', (data: Buffer) => {
                            pqueue.add(() => {
                                buff.push(data)
                                length += data.length
                                connlist.set(connectionID, {})
                            })
                        })

                        let length = 0
                        let rtt = 0
                        let max = 2 * 1024 * 1024
                        let seq = "0"
                        const pqueueMax = new PQueue({ concurrency: 1 })
                        const interv = setInterval(async () => {
                            pqueue.add(async () => {
                                if (length != buff.length)
                                    length = buff.length
                                else {
                                    if (buff.length != 0) {
                                        length = 0
                                        connlist.set(connectionID, true)
                                        logger(`Pushing batch to proxy,${connectionID}`, "info")
                                        const msg = Buffer.concat(buff)
                                        const iv = crypto.randomBytes(12)
                                        const cipher = crypto.createCipheriv("aes-256-gcm", symmetricKey, iv)
                                        const encryptedMsg = Buffer.concat([cipher.update(msg), cipher.final()])
                                        const tag = cipher.getAuthTag()
                                        if (conn)
                                            await conn.lpush(`proxy,${connectionID}`, Buffer.concat([iv, tag, encryptedMsg]))
                                        else {
                                            logger("Ok here! " + `proxy,${connectionID}/${seq}`)
                                            await s3.send(new PutObjectCommand({
                                                Bucket: bucketName,
                                                Key: `proxy,${connectionID}/${seq}`,
                                                ACL: 'private',
                                                Body: Buffer.concat([iv, tag, encryptedMsg]),
                                            })).catch((reason) => {
                                                logger(`Problem with pushing batch after informing ${reason}`, "error")
                                            })
                                            logger("Done?")
                                        }
                                        buff = []
                                        sent = true
                                        pqueueMax.add(async () => {
                                            const msgACK = Buffer.from(`${max}`, 'binary')
                                            const ivACK = crypto.randomBytes(12)
                                            const cipherACK = crypto.createCipheriv("aes-256-gcm", symmetricKey, ivACK)
                                            const encryptedMsgACK = Buffer.concat([cipherACK.update(msgACK), cipherACK.final()])
                                            const tagACK = cipherACK.getAuthTag()
                                            if (conn)
                                                await conn.lpush(`ack,${connectionID}`, Buffer.concat([ivACK, tagACK, encryptedMsgACK]))
                                            else {
                                                logger("Ok we are sending rtt")
                                                await s3.send(new PutObjectCommand({
                                                    Bucket: bucketName,
                                                    Key: `ack,${connectionID}`,
                                                    ACL: 'private',
                                                    Body: Buffer.concat([ivACK, tagACK, encryptedMsgACK]),
                                                })).catch((reason) => {
                                                    logger(`Problem with pushing ack after informing ${reason}`, "error")
                                                })
                                                logger("Seems like sending rtt got passed? lol")
                                            }
                                            rtt = Date.now()
                                        })
                                    }
                                }
                            })
                        }, 100)
                        let sent = false
                        const inatervo = setInterval(async () => {
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
                                        logger("Ok we are sending rtt in interval")
                                        await s3.send(new PutObjectCommand({
                                            Bucket: bucketName,
                                            Key: `ack,${connectionID}`,
                                            ACL: 'private',
                                            Body: Buffer.concat([ivACK, tagACK, encryptedMsgACK]),
                                        })).catch((reason) => {
                                            logger(`Problem with pushing ack after informing ${reason}`, "error")
                                        })
                                        logger("This passed again?")
                                    }
                                })
                            }
                        }, 100)

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
                        let blconn: Redis | null
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
                                    clearInterval(interv)
                                    clearInterval(inatervo)
                                    clearImmediate(imedo)
                                    conn!.del(`ack,${connectionID}`)
                                    conn!.del(`appserver,${connectionID}`)
                                    await conn!.del(`proxy,${connectionID}`)
                                    socket.end()
                                    connlist.delete(connectionID)
                                }
                            }, 10000)

                        socket.once('error', (e) => {
                            logger(`Client error: ${e}`, "error")
                            if (pinger)
                                clearInterval(pinger)
                            clearInterval(inatervo)
                            clearInterval(interv)
                            clearImmediate(imedo)
                            if (blconn)
                                blconn.quit().catch(() => { })
                            connlist.delete(connectionID)
                        })

                        socket.on('end', async () => {
                            logger(`Sending half close signal to proxy,${connectionID}`, "info")
                            if (conn) {
                                conn.del(`ack,${connectionID}`)
                                conn.del(`appserver,${connectionID}`)
                                await conn!.del(`proxy,${connectionID}`)
                            } else {
                                logger("Somehow we managed to delete a shit?")
                                s3.send(
                                    new DeleteObjectCommand({
                                        Bucket: bucketName,
                                        Key: `ack,${connectionID}`,
                                    })
                                ).catch((reason) => {
                                    logger(`Problem with pushing ack in end ${reason}`, "error")
                                })
                                logger("And its passed?")
                                await s3.send(
                                    new DeleteObjectCommand({
                                        Bucket: bucketName,
                                        Key: `appserver,${connectionID}`,
                                    })
                                ).catch((reason) => {
                                    logger(`Problem with pushing ack in end ${reason}`, "error")
                                })
                                logger("So as this one?")
                            }
                            if (pinger)
                                clearInterval(pinger)
                            clearInterval(inatervo)
                            clearImmediate(imedo)
                            clearInterval(interv)
                            if (blconn)
                                blconn.quit().catch(() => { })
                            connlist.delete(connectionID)
                        })

                        if (mode != "s3")
                            blconn!.on('error', () => {
                                logger("blconn error event: " + connectionID, "error")
                                clearInterval(interv)
                                clearInterval(pinger!)
                                clearInterval(inatervo)
                                clearImmediate(imedo)
                                conn!.del(`ack,${connectionID}`)
                                conn!.del(`appserver,${connectionID}`)
                                conn!.del(`proxy,${connectionID}`)
                                connlist.delete(connectionID)
                                blconn!.disconnect(false)
                                socket.end()
                            })

                        const imedo = setImmediate(async () => {
                            while (true) {
                                try {
                                    let response: Uint8Array<ArrayBufferLike> | undefined
                                    if (blconn)
                                        response = (await blconn.brpopBuffer(`appserver,${connectionID}`, 0))?.[1]
                                    else {
                                        logger(`appserver,${connectionID}/${seq}`)
                                        response = await popperBuffer(`appserver,${connectionID}/${seq}`)
                                    }
                                    sent = false
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

                                    const extractIv = response!.subarray(0, 12)
                                    const tag = response!.subarray(12, 28)
                                    const encryptedChunk = response!.subarray(28)
                                    const decipher = crypto.createDecipheriv("aes-256-gcm", symmetricKey, extractIv)
                                    decipher.setAuthTag(tag)
                                    let decryptedChunk = Buffer.concat([decipher.update(encryptedChunk), decipher.final()])
                                    let version: string | null
                                    if (mode == "s3") {
                                        version = decryptedChunk.subarray(0, 10).toString('hex')
                                        logger("THIS IS THE VERSSSSIOOON " + version)
                                        seq = version
                                        const realMsg = decryptedChunk.subarray(10)
                                        decryptedChunk = realMsg
                                    }
                                    if (!Buffer.from('end', 'binary').compare(decryptedChunk)) {
                                        logger(`server chunk for ${connectionID} is null`, "info")
                                        if (pinger)
                                            clearInterval(pinger)
                                        clearInterval(inatervo)
                                        clearInterval(interv)
                                        clearImmediate(imedo)
                                        if (blconn)
                                            blconn.quit().catch(() => { })
                                        socket.end()
                                        connlist.delete(connectionID)
                                        if (conn) {
                                            conn.del(`ack,${connectionID}`)
                                            conn.del(`appserver,${connectionID}`)
                                            await conn!.del(`proxy,${connectionID}`)
                                        } else {
                                            s3.send(
                                                new DeleteObjectCommand({
                                                    Bucket: bucketName,
                                                    Key: `ack,${connectionID}`,
                                                })
                                            ).catch((reason) => {
                                                logger(`Problem with pushing ack in end ${reason}`, "error")
                                            })
                                            await s3.send(
                                                new DeleteObjectCommand({
                                                    Bucket: bucketName,
                                                    Key: `appserver,${connectionID}`,
                                                })
                                            ).catch((reason) => {
                                                logger(`Problem with pushing ack in end ${reason}`, "error")
                                            })
                                            logger("Passed????")
                                        }
                                        socket.end()
                                        break
                                    }
                                    socket?.write(decryptedChunk)
                                } catch (error) {
                                    if (mode != "s3")
                                        clearInterval(pinger!)
                                    clearInterval(inatervo)
                                    clearImmediate(imedo)
                                    clearInterval(interv)
                                    connlist.delete(connectionID)
                                    conn!.del(`ack,${connectionID}`)
                                    conn!.del(`proxy,${connectionID}`)
                                    await conn!.del(`appserver,${connectionID}`)
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

process.on('SIGTERM', async () => {
    logger("Stopping the server", "info")
    server.close()
    if (conn)
        await conn.flushdb()
    else {
        try {
            const data = await s3.send(
                new ListObjectsCommand({
                    Bucket: bucketName,
                })
            )
            let sagjerk: { Key: string }[] = []
            for (const element of data.Contents!)
                sagjerk.push({ Key: element.Key! })
            await s3.send(
                new DeleteObjectsCommand({
                    Bucket: bucketName,
                    Delete: {
                        Objects: sagjerk,
                    },
                })
            )
        } catch (reason) {
            logger(`Problem with getting all chunks or deleting them ${reason}`, "error")
        }
    }

    logger("Removing chunks completed", "info")
    exit(0)
})

process.on('SIGINT', async () => {
    logger("Stopping the server", "info")
    server.close()
    if (conn)
        await conn.flushdb()
    else {
        try {
            const data = await s3.send(
                new ListObjectsCommand({
                    Bucket: bucketName,
                })
            )
            let sagjerk: { Key: string }[] = []
            for (const element of data.Contents!)
                sagjerk.push({ Key: element.Key! })
            await s3.send(
                new DeleteObjectsCommand({
                    Bucket: bucketName,
                    Delete: {
                        Objects: sagjerk,
                    },
                })
            )
        } catch (reason) {
            logger(`Problem with getting all chunks or deleting them ${reason}`, "error")
        }
    }
    logger("Removing chunks completed", "info")
    exit(0)
})

logger("Listening on 1080", "info")
server.listen(1080)

server.on('error', (error) => {
    logger(`Problem with connection with application server ${error.message}`, "error")
})
