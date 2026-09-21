import net, { Socket } from 'net'
import dns from 'dns/promises'
import { Redis } from 'ioredis'
import { exit } from 'process'
import PQueue from 'p-queue'
import crypto from 'node:crypto'
import { DeleteObjectsCommand, GetObjectCommand, ListObjectsCommand, ListObjectsV2Command, PutObjectCommand, S3, S3Client } from '@aws-sdk/client-s3'
import https from 'https'

import config from "../config.json" with {type: 'json'}

const mode = config.mode

let g = 0

let s3list: S3Client[] = []
if (mode == "s3")
    for (let index = 0; index < 10; index++)
        s3list.push(new S3Client({
            region: config.zone,
            endpoint: config.endpointUrl,
            credentials: {
                accessKeyId: config.accessKey,
                secretAccessKey: config.secretKey,
            },
            requestHandler: {
                httpsAgent: new https.Agent({
                    keepAlive: true,
                    keepAliveMsecs: 5000,
                    maxSockets: 128,
                    maxFreeSockets: 32,
                    timeout: 30000,
                })
            }
        }))

let s3DeleteList: S3Client[] = []
if (mode == "s3")
    for (let index = 0; index < 10; index++)
        s3DeleteList.push(new S3Client({
            region: config.zone,
            endpoint: config.endpointUrl,
            credentials: {
                accessKeyId: config.accessKey,
                secretAccessKey: config.secretKey,
            },
            requestHandler: {
                httpsAgent: new https.Agent({
                    keepAlive: true,
                    keepAliveMsecs: 5000,
                    maxSockets: 128,
                    maxFreeSockets: 32,
                    timeout: 30000,
                })
            }
        }))


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

let blconn: Redis | null
if (mode != 's3')
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

if (mode != "s3")
    try {
        await conn!.ping()
        await blconn!.ping()
    } catch (e) {
    }


//DNS RESOLVE, for now its not optimized but works atleast
const workingDNSes = new Map<string, { ip: string, requiredTime: number }>() // Map<address, ip>
let fastestDNSes = new Map<string, { ip: string, requiredTime: number }>() //Map<address, fastest ip>

const symmetricKey = Buffer.from(config.symmetricKey, "hex")

async function testConnection(address: string, ip: string, port: number): Promise<boolean> {
    return await new Promise<boolean>((resolve) => {
        const startTime = Date.now()
        const connection = net.createConnection(port!, ip)
        const timeo = setTimeout(() => {
            connection.destroy()
            resolve(false)
        }, 3000)
        connection.on('connect', () => {
            clearTimeout(timeo)
            workingDNSes.set(address, { ip: ip, requiredTime: Date.now() - startTime })
            resolve(true)
        })
        connection.on('error', () => {
            clearTimeout(timeo)
            connection.destroy()
            resolve(false)
        })
    })
}

async function getFastestIP(address: string, port: number): Promise<string | null> {
    if (fastestDNSes.has(address))
        return fastestDNSes.get(address)!.ip
    try {
        const ipv4s = (await dns.resolve(address)).filter(x => x.includes('.'))
        if (ipv4s.length == 0)
            return null
        for (const ipv4 of ipv4s) {
            await testConnection(address, ipv4, port)
        }
        fastestDNSes = new Map([...workingDNSes.entries()].sort((a, b) => a[1].requiredTime - b[1].requiredTime))
        return fastestDNSes.get(address)?.ip || null
    } catch (err) {
        return null
    }
}

function logger(param: string, type?: string) {
    const date = new Date(Date.now())
    console.log(type == "info" ? `[\x1b[33mINFO\x1b[0m] [\x1b[32m${mode}\x1b[0m] ${date.getHours()}:${date.getMinutes()}:${date.getSeconds()} ${param}`
        : (type == "error" ? `[\x1b[31mERR\x1b[0m] [\x1b[32m${mode}\x1b[0m] ${date.getHours()}:${date.getMinutes()}:${date.getSeconds()} ${param}` : param))
}

async function popperBuffer2(key: string, connectionID: string, s3Client: S3Client) {
    let dangoz = Date.now()
    for (let i = 0; i < 200; i++) {
        try {
            if (sockets.get(connectionID)?.abort) {
                logger("Freeing memory")
                sockets.delete(connectionID)
                try {
                    const data2 = await s3Client.send(new ListObjectsV2Command({
                        Bucket: bucketName,
                        Prefix: `proxy,${connectionID}/`,
                    }))

                    const sagjerk2: { Key: string }[] = []
                    if (data2.Contents && data2.Contents.length != 0) {
                        for (const element of data2.Contents)
                            sagjerk2.push({ Key: element.Key! })
                        await s3Client.send(
                            new DeleteObjectsCommand({
                                Bucket: bucketName,
                                Delete: {
                                    Objects: sagjerk2,
                                },
                            })
                        )
                    }
                    s3Client.destroy()
                    break
                } catch (e) {
                    s3Client.destroy()
                    logger("Problem with fucking appserver chunks " + e, "error")
                    break
                }
            }
            logger("Im getting this mother fucker so bad")
            const data = await s3Client.send(new GetObjectCommand({ Bucket: bucketName, Key: key }))
            logger(`It took me ${Date.now() - dangoz}ms to actually receive this`)
            logger("Fucked?")
            return await data.Body!.transformToByteArray()
        } catch {
            await new Promise(r => setTimeout(r, 500))
        }
    }
}


const callback = (payload: Uint8Array<ArrayBufferLike>) => {
    const extractIv = payload.subarray(0, 12)
    const tag = payload.subarray(12, 28)
    const encryptedChunk = payload.subarray(28)
    const decipher = crypto.createDecipheriv("aes-256-gcm", symmetricKey, extractIv)
    decipher.setAuthTag(tag)
    const decryptedChunk = Buffer.concat([decipher.update(encryptedChunk), decipher.final()])
    const things = decryptedChunk.toString('utf8').split(',')!
    const dstaddr = things[0]!
    const dstport = parseInt(things[1]!)
    const connectionID = things[2]!
    const atyp = things[3]!
    setImmediate(async () => {
        try {
            let blconn1: Redis | null
            let s31: S3Client | null
            if (mode != "s3")
                if (config.tls == "")
                    blconn1 = new Redis(config.connstring, {
                        maxRetriesPerRequest: null,
                        keepAlive: 10000,
                    })
                else
                    blconn1 = new Redis(config.connstring, {
                        maxRetriesPerRequest: null,
                        keepAlive: 10000,
                        tls: { servername: config.tls }
                    })
            else {
                s31 = new S3Client({
                    region: config.zone,
                    endpoint: config.endpointUrl,
                    credentials: {
                        accessKeyId: config.accessKey,
                        secretAccessKey: config.secretKey,
                    },
                    requestHandler: {
                        httpsAgent: new https.Agent({
                            keepAlive: true,
                            keepAliveMsecs: 30000,
                            maxSockets: 128,
                            maxFreeSockets: 32,
                            timeout: 30000,
                        })
                    }
                })
            }
            let ackconn: Redis | null
            if (mode != "s3")
                if (config.tls == "")
                    ackconn = new Redis(config.connstring, {
                        maxRetriesPerRequest: null,
                        keepAlive: 10000,
                    })
                else
                    ackconn = new Redis(config.connstring, {
                        maxRetriesPerRequest: null,
                        keepAlive: 10000,
                        tls: { servername: config.tls }
                    })

            if (mode != "s3")
                try {
                    await blconn1!.ping()
                    await ackconn!.ping()
                } catch (e) {
                    return
                }

            let pinger: NodeJS.Timeout | null
            if (mode != "s3")
                pinger = setInterval(async () => {
                    try {
                        await blconn1!.ping()
                        await ackconn!.ping()
                    } catch (e) {
                        clearInterval(pinger!)
                        blconn1!.quit().catch(() => { })
                        ackconn!.quit().catch(() => { })
                        sockets.delete(connectionID)
                    }
                }, 10000)

            let ack = 2 * 1024 * 1024
            let inSeq = "0"
            let outSeq = "0"
            let seqChanged = true
            while (mode == "s3" ? await new Promise<Boolean>((resolve) => {
                const ass = setInterval(() => {
                    if (seqChanged) {
                        clearInterval(ass)
                        resolve(true)
                    }
                }, 100)
            }) : true) {
                seqChanged = false
                let request: Uint8Array<ArrayBufferLike> | undefined
                if (mode != "s3")
                    request = (await blconn1!.brpopBuffer(`proxy,${connectionID}`, 20))?.[1]
                else {
                    logger(`proxy,${connectionID}/${inSeq}`)
                    request = await popperBuffer2(`proxy,${connectionID}/${inSeq}`, connectionID, s31!)
                }
                logger("IS this because of that proxy, shit?")
                if (!request) {
                    logger("Request issue")
                    sockets.get(connectionID)?.socket?.end()
                    sockets.delete(connectionID)
                    if (mode != "s3") {
                        clearInterval(pinger!)
                        await blconn1!.del(`appserver,${connectionID}`)
                        blconn1!.quit().catch(() => { })
                        ackconn!.quit().catch(() => { })
                    } else {
                        try {
                            const data2 = await s31!.send(new ListObjectsV2Command({
                                Bucket: bucketName,
                                Prefix: `proxy,${connectionID}/`,
                            }))

                            const sagjerk2: { Key: string }[] = []
                            if (data2.Contents && data2.Contents.length != 0) {
                                for (const element of data2.Contents)
                                    sagjerk2.push({ Key: element.Key! })
                                await s31!.send(
                                    new DeleteObjectsCommand({
                                        Bucket: bucketName,
                                        Delete: {
                                            Objects: sagjerk2,
                                        },
                                    })
                                )
                            }
                            s31!.destroy()
                        } catch (e) {
                            s31!.destroy()
                            logger("Problem with fucking appserver chunks " + e, "error")
                        }
                    }
                    break
                }
                logger("After match, the version being used " + inSeq)
                const extractIv = request!.subarray(0, 12)
                const tag = request!.subarray(12, 28)
                const encryptedChunk = request!.subarray(28)
                const decipher = crypto.createDecipheriv("aes-256-gcm", symmetricKey, extractIv)
                decipher.setAuthTag(tag)
                const decryptedChunk = Buffer.concat([decipher.update(encryptedChunk), decipher.final()])
                let realMsg: Buffer<ArrayBuffer>
                if (mode == "s3") {
                    const newVersion = decryptedChunk.subarray(0, 10)
                    inSeq = newVersion.toString('hex')
                    realMsg = decryptedChunk.subarray(10)
                }
                logger("The version now we want after " + inSeq)
                if (!Buffer.from('end', 'binary').compare(decryptedChunk)) {
                    logger("Freeing memory from client")
                    sockets.get(connectionID)?.socket?.end()
                    sockets.delete(connectionID)
                    if (mode != "s3") {
                        clearInterval(pinger!)
                        await blconn1!.del(`proxy,${connectionID}`)
                        blconn1!.quit().catch(() => { })
                        ackconn!.quit().catch(() => { })
                    } else {
                        try {
                            const data2 = await s31!.send(new ListObjectsV2Command({
                                Bucket: bucketName,
                                Prefix: `proxy,${connectionID}/`,
                            }))

                            const sagjerk2: { Key: string }[] = []
                            if (data2.Contents && data2.Contents.length != 0) {
                                for (const element of data2.Contents)
                                    sagjerk2.push({ Key: element.Key! })
                                await s31!.send(
                                    new DeleteObjectsCommand({
                                        Bucket: bucketName,
                                        Delete: {
                                            Objects: sagjerk2,
                                        },
                                    })
                                )
                            }
                            s31!.destroy()
                        } catch (e) {
                            s31!.destroy()
                            logger("Problem with fucking appserver chunks " + e, "error")
                        }
                    }
                    break
                }

                let buffered: Uint8Array<ArrayBufferLike> | undefined
                if (mode != "s3") {
                    buffered = (await ackconn!.brpopBuffer(`ack,${connectionID}`, 0))?.[1]
                } else if (config.ackS3) {
                    buffered = await popperBuffer2(`ack,${connectionID}`, connectionID, s31!)
                }
                if (!buffered && config.ackS3) {
                    logger("Buffered issue")
                    sockets.get(connectionID)?.socket?.end()
                    sockets.delete(connectionID)
                    if (mode != "s3") {
                        clearInterval(pinger!)
                        await blconn1!.del(`ack,${connectionID}`)
                        blconn1!.quit().catch(() => { })
                        ackconn!.quit().catch(() => { })
                    } else {
                        try {
                            const data2 = await s31!.send(new ListObjectsV2Command({
                                Bucket: bucketName,
                                Prefix: `proxy,${connectionID}/`,
                            }))

                            const sagjerk2: { Key: string }[] = []
                            if (data2.Contents && data2.Contents.length != 0) {
                                for (const element of data2.Contents)
                                    sagjerk2.push({ Key: element.Key! })
                                await s31!.send(
                                    new DeleteObjectsCommand({
                                        Bucket: bucketName,
                                        Delete: {
                                            Objects: sagjerk2,
                                        },
                                    })
                                )
                            }
                            s31!.destroy()
                        } catch (e) {
                            s31!.destroy()
                            logger("Problem with fucking appserver chunks " + e, "error")
                        }
                    }
                    break
                }

                if (config.ackS3 || mode != "s3") {
                    const extractIvACK = buffered!.subarray(0, 12)
                    const tagACK = buffered!.subarray(12, 28)
                    const encryptedChunkACK = buffered!.subarray(28)
                    const decipherACK = crypto.createDecipheriv("aes-256-gcm", symmetricKey, extractIvACK)
                    decipherACK.setAuthTag(tagACK)
                    const decryptedChunkACK = Buffer.concat([decipherACK.update(encryptedChunkACK), decipherACK.final()])
                    ack = parseInt(decryptedChunkACK.toString('utf8'))
                }
                if (!sockets.has(connectionID)) {
                    let fastestWorkingIP: string | null
                    if (atyp === "3")
                        fastestWorkingIP = await getFastestIP(dstaddr, dstport)
                    else
                        fastestWorkingIP = dstaddr
                    if (!fastestWorkingIP) {
                        let msg: Buffer<ArrayBuffer> | null
                        if (mode != "s3")
                            msg = Buffer.from('end', 'binary')
                        else {
                            const preMsg = Buffer.alloc(13)
                            const jerk = crypto.randomBytes(10)
                            jerk.copy(preMsg, 0, 0, 10)
                            preMsg.write('end', 10)
                            msg = preMsg
                        }
                        const iv = crypto.randomBytes(12)
                        const cipher = crypto.createCipheriv("aes-256-gcm", symmetricKey, iv)
                        const encryptedMsg = Buffer.concat([cipher.update(msg), cipher.final()])
                        const tag = cipher.getAuthTag()
                        if (mode != "s3") {
                            await conn!.lpush(`appserver,${connectionID}`, Buffer.concat([iv, tag, encryptedMsg]))
                        } else {
                            logger("Ok before of this portion!")
                            try {
                                await s31!.send(new PutObjectCommand({
                                    Bucket: bucketName,
                                    Key: `appserver,${connectionID}/${outSeq}`,
                                    ACL: 'private',
                                    Body: Buffer.concat([iv, tag, encryptedMsg]),
                                })).catch((reason) => {
                                    logger(`Problem with pushing appserver end ${reason}`, "error")
                                })
                                const data2 = await s31!.send(new ListObjectsV2Command({
                                    Bucket: bucketName,
                                    Prefix: `proxy,${connectionID}/`,
                                }))

                                const sagjerk2: { Key: string }[] = []
                                if (data2.Contents && data2.Contents.length != 0) {
                                    for (const element of data2.Contents)
                                        sagjerk2.push({ Key: element.Key! })
                                    await s31!.send(
                                        new DeleteObjectsCommand({
                                            Bucket: bucketName,
                                            Delete: {
                                                Objects: sagjerk2,
                                            },
                                        })
                                    )
                                }
                                s31!.destroy()
                                logger("Ok it seems i really change the version now!")
                                logger("The version is now " + outSeq)
                            } catch (e) {
                                s31!.destroy()
                                logger("Problemw ith adwdadwa " + e, "error")
                            }
                        }
                        if (mode != "s3") {
                            clearInterval(pinger!)
                            blconn1!.quit().catch(() => { })
                            ackconn!.quit().catch(() => { })
                        }
                        sockets.delete(connectionID)
                        break
                    }
                    const appServer = net.createConnection(dstport, fastestWorkingIP)
                    sockets.set(connectionID, { socket: appServer, abort: false })
                    const res = await new Promise<Boolean>((resolve) => {
                        const connectionTimeout = setTimeout(() => {
                            if (appServer)
                                appServer.destroy()
                            resolve(false)
                        }, 25000)
                        appServer.once('connect', async () => {
                            clearTimeout(connectionTimeout)
                            resolve(true)
                        })
                        appServer.once('error', () => {
                            clearTimeout(connectionTimeout)
                            if (appServer)
                                appServer.destroy()
                            resolve(false)
                        })
                    })
                    if (!res) {
                        sockets.delete(connectionID)
                        if (mode != "s3") {
                            clearInterval(pinger!)
                            blconn1!.quit().catch(() => { })
                            ackconn!.quit().catch(() => { })
                        }
                        break
                    }

                    sockets.get(connectionID)?.socket?.write(mode == "s3" ? realMsg! : decryptedChunk)
                    let buffass: Buffer[] = []
                    let timeout: NodeJS.Timeout
                    let pqueue = new PQueue({ concurrency: 1 })
                    let length = 0
                    appServer.on('data', (data: Buffer) => {
                        logger("data arrived")
                        pqueue.add(() => {
                            logger("We are in the queue")
                            if (timeout)
                                clearTimeout(timeout)
                            length += data.length
                            buffass.push(data)
                            pqueue.add(async () => {
                                if (length > ack) { // bigger than 2mb
                                    let msg: Buffer<ArrayBuffer> | null
                                    let newVersion: Buffer
                                    if (mode != "s3")
                                        msg = Buffer.concat(buffass)
                                    else {
                                        const concatious = Buffer.concat(buffass)
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
                                    if (mode != "s3") {
                                        logger("YOU MEAN IM PUSHING TO THIS ASSHOLE?")
                                        await conn!.lpush(`appserver,${connectionID}`, Buffer.concat([iv, tag, encryptedMsg]))
                                    } else {
                                        logger("Ok we send appserver chunk")
                                        await s31!.send(new PutObjectCommand({
                                            Bucket: bucketName,
                                            Key: `appserver,${connectionID}/${outSeq}`,
                                            ACL: 'private',
                                            Body: Buffer.concat([iv, tag, encryptedMsg]),
                                        })).catch((reason) => {
                                            logger(`Problem with pushing appserver batch ${reason}`, "error")
                                        })
                                        outSeq = newVersion!.toString('hex')
                                        seqChanged = true
                                        logger("And it passed?")
                                    }
                                    buffass = []
                                    length = 0
                                }
                            })
                            timeout = setTimeout(() => {
                                if (!length)
                                    return
                                pqueue.add(async () => {
                                    let msg: Buffer<ArrayBuffer> | null
                                    let newVersion: Buffer
                                    if (mode != "s3")
                                        msg = Buffer.concat(buffass)
                                    else {
                                        const concatious = Buffer.concat(buffass)
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
                                    if (mode != "s3") {
                                        logger("YOU MEAN IM PUSHING TO THIS ASSHOLE?")
                                        await conn!.lpush(`appserver,${connectionID}`, Buffer.concat([iv, tag, encryptedMsg]))
                                    } else {
                                        logger("Sending appserver chunk in timeout")
                                        await s31!.send(new PutObjectCommand({
                                            Bucket: bucketName,
                                            Key: `appserver,${connectionID}/${outSeq}`,
                                            ACL: 'private',
                                            Body: Buffer.concat([iv, tag, encryptedMsg]),
                                        })).catch((reason) => {
                                            logger(`Problem with pushing appserver batch ${reason}`, "error")
                                        })
                                        outSeq = newVersion!.toString('hex')
                                        seqChanged = true
                                        logger("It passed so means the fucking version is now this " + outSeq)
                                    }
                                    buffass = []
                                    length = 0
                                })
                            }, 100)
                        })
                    })
                    // notify the proxy appserver dont sends data anymore (half close)
                    appServer.on('end', async () => {
                        if (mode != "s3") {
                            clearInterval(pinger!)
                            blconn1!.quit().catch(() => { })
                            ackconn!.quit().catch(() => { })
                            sockets.delete(connectionID)
                        } else {
                            seqChanged = true
                            sockets.set(connectionID, { socket: undefined, abort: true })
                        }
                    })
                } else
                    sockets.get(connectionID)?.socket?.write(mode == "s3" ? realMsg! : decryptedChunk)
            }
        } catch (e) {
        }
    })
}

const sockets = new Map<string, { socket: Socket | undefined, abort: boolean }>()
setImmediate(async () => {
    while (true) {
        if (blconn)
            callback((await blconn.brpopBuffer(`inform`, 0))?.[1]!)
        else {
            try {
                // and i wait here for client that he deleted directory, with this way we can send all packets from client
                if (s3list.length == g)
                    g = 0
                const data = await s3list[g]!.send(new ListObjectsV2Command({
                    Bucket: bucketName,
                    Prefix: "informs/",
                }))

                let sagjerk: { Key: string }[] = []

                if (data.Contents && data.Contents.length != 0)
                    for (const element of data.Contents) {
                        logger("Name of that " + element.Key)
                        sagjerk.push({ Key: element.Key! })
                        const daljerk = await s3list[g]!.send(new GetObjectCommand({
                            Bucket: bucketName, Key: element.Key
                        }))

                        logger("OK so now this means we really have the shit out of it")
                        callback(await daljerk.Body!.transformToByteArray())
                    }
                await s3DeleteList[g++]!.send(
                    new DeleteObjectsCommand({
                        Bucket: bucketName,
                        Delete: {
                            Objects: sagjerk,
                        },
                    })
                )
                // I send the client that you should remove the directory
            } catch (e) {

            }
        }
    }
})

if (mode != "s3")
    setInterval(async () => {
        try {
            await conn!.ping()
            await blconn!.ping()
        } catch (e) {
        }
    }, 10000)

process.on('uncaughtException', (error) => {
    logger(`${error.cause}:${error.message}:${error.name}`, "error")
})

process.on('SIGTERM', async () => {
    if (conn)
        await conn.flushdb()
    else {
        try {
            if (s3list.length == g)
                g = 0
            const data = await s3list[g]!.send(
                new ListObjectsCommand({
                    Bucket: bucketName,
                })
            )
            let sagjerk: { Key: string }[] = []
            if (data.Contents && data.Contents.length != 0) {
                for (const element of data.Contents)
                    sagjerk.push({ Key: element.Key! })
                await s3DeleteList[g++]!.send(
                    new DeleteObjectsCommand({
                        Bucket: bucketName,
                        Delete: {
                            Objects: sagjerk,
                        },
                    })
                )
            }
        } catch (reason) {
            logger(`Problem with getting all chunks or deleting them ${reason}`, "error")
        }
    }
    exit(0)
})

process.on('SIGINT', async () => {
    if (conn)
        await conn.flushdb()
    else {
        try {
            if (s3list.length == g)
                g = 0
            const data = await s3list[g]!.send(
                new ListObjectsCommand({
                    Bucket: bucketName,
                })
            )
            let sagjerk: { Key: string }[] = []
            if (data.Contents && data.Contents.length != 0) {
                for (const element of data.Contents)
                    sagjerk.push({ Key: element.Key! })
                await s3DeleteList[g++]!.send(
                    new DeleteObjectsCommand({
                        Bucket: bucketName,
                        Delete: {
                            Objects: sagjerk,
                        },
                    })
                )
            }
        } catch (reason) {
            logger(`Problem with getting all chunks or deleting them ${reason}`, "error")
        }
    }
    exit(0)
})
