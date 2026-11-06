import net, { Socket } from 'net'
import dns from 'dns/promises'
import { Redis } from 'ioredis'
import { exit } from 'process'
import PQueue, { PriorityQueue, type QueueAddOptions } from 'p-queue'
import crypto from 'node:crypto'
import { S3Client as S3Light } from '@bradenmacdonald/s3-lite-client'
import { DeleteObjectCommand, DeleteObjectsCommand, GetObjectCommand, ListObjectsV2Command, PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import https from 'https'
import http from 'http'

import config from "../config.json" with {type: 'json'}

const mode = config.mode
const symmetricKey = Buffer.from(config.symmetricKey, "hex")
const sockets = new Map<string, { socket: Socket | undefined, bandwidth?: number }>()

function logger(param: string, type?: string) {
    const date = new Date(Date.now())
    console.log(type == "info" ? `[\x1b[33mINFO\x1b[0m] [\x1b[32m${mode}\x1b[0m] ${date.getHours()}:${date.getMinutes()}:${date.getSeconds()} ${param}`
        : (type == "error" ? `[\x1b[31mERR\x1b[0m] [\x1b[32m${mode}\x1b[0m] ${date.getHours()}:${date.getMinutes()}:${date.getSeconds()} ${param}` : param))
}

//DNS RESOLVE, for now its not optimized but works atleast
const workingDNSes = new Map<string, { ip: string, requiredTime: number }>() // Map<address, ip>
let fastestDNSes = new Map<string, string>() //Map<address, fastest ip>

async function getFastestIP(address: string, port: number): Promise<string | undefined> {
    if (fastestDNSes.has(address))
        return fastestDNSes.get(address)
    try {
        const ipv4s = (await dns.resolve(address)).filter(x => x.includes('.'))
        if (ipv4s.length == 0)
            return undefined

        await new Promise<boolean>(async (resolve) => {
            await Promise.all(ipv4s.map(async (ipv4) => {
                const startTime = Date.now()
                const connection = net.createConnection(port!, ipv4)
                const timeo = setTimeout(() => {
                    connection.destroy()
                    resolve(false)
                }, 3000)
                connection.on('connect', () => {
                    clearTimeout(timeo)
                    workingDNSes.set(address, { ip: ipv4, requiredTime: Date.now() - startTime })
                    resolve(true)
                })
                connection.on('error', () => {
                    clearTimeout(timeo)
                    connection.destroy()
                    resolve(false)
                })
            }))
        })
        fastestDNSes.set(address, workingDNSes.get(address)?.ip!)
        return fastestDNSes.get(address)
    } catch (err) {
        return undefined
    }
}

if (mode == "http") {
    /*
        client sends 2 kind of data
        one is saying what bandwidth it wants for specific connection
        another is just request
        both are encrypted with aes256
    */
    const server = http.createServer((req, resp) => {
        req.on('data', async (bod: Buffer) => {
            try {
                if (!req.url) {
                    resp.writeHead(500)
                    resp.end()
                    return
                }
                if (req.url.length > 100) {
                    resp.writeHead(500)
                    resp.end()
                    return
                }
                if (req.url?.includes("end")) { // if it was end of stream willed by client
                    const connectionID = req.url?.split('/')[1]?.split('&')[0] // /uuid&end
                    if (!connectionID) {
                        resp.writeHead(500)
                        resp.end()
                        return
                    }
                    if (!sockets.has(connectionID)) {
                        resp.writeHead(500)
                        resp.end()
                        return
                    }
                    sockets.get(connectionID)?.socket?.removeAllListeners()
                    sockets.get(connectionID)?.socket?.end()
                    sockets.delete(connectionID)
                } else if (req.url == "/inform") {
                    const extractIv = bod.subarray(0, 12)
                    const tag = bod.subarray(12, 28)
                    const encryptedChunk = bod.subarray(28)
                    const decipher = crypto.createDecipheriv("aes-256-gcm", symmetricKey, extractIv)
                    decipher.setAuthTag(tag)
                    const decryptedChunk = Buffer.concat([decipher.update(encryptedChunk), decipher.final()])
                    const things = decryptedChunk.toString('utf8').split(',')!
                    const dstaddr = things[0]!
                    const dstport = parseInt(things[1]!)
                    const connectionID = things[2]!
                    const atyp = things[3]!
                    if (!sockets.has(connectionID)) {
                        let fastestWorkingIP: string | undefined
                        if (atyp === "3")
                            fastestWorkingIP = await getFastestIP(dstaddr, dstport)
                        else
                            fastestWorkingIP = dstaddr
                        if (!fastestWorkingIP) {
                            resp.writeHead(500)
                            resp.end()
                            sockets.delete(connectionID)
                            return
                        }
                        const appServer = net.createConnection(dstport, fastestWorkingIP)
                        appServer.on('end', async () => {
                            const msg = Buffer.from('end', 'binary')
                            const iv = crypto.randomBytes(12)
                            const cipher = crypto.createCipheriv("aes-256-gcm", symmetricKey, iv)
                            const encryptedMsg = Buffer.concat([cipher.update(msg), cipher.final()])
                            const tag = cipher.getAuthTag()
                            resp.writeHead(500)
                            sockets.delete(connectionID)
                            resp.end(Buffer.concat([iv, tag, encryptedMsg]))
                        })

                        sockets.set(connectionID, { socket: appServer, bandwidth: 512 * 1024 })
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
                            resp.writeHead(500)
                            resp.end()
                            sockets.delete(connectionID)
                            return
                        }
                    }
                } else { // real request, /uuid&bandwidth
                    const shits = req.url?.split('/')
                    if (!shits || !shits[1]) {
                        resp.writeHead(500)
                        resp.end()
                        return
                    }
                    const shitss = shits[1].split('&')
                    const connectionID = shitss[0]
                    const bandwidth = shitss[1]
                    if (!connectionID || !bandwidth) {
                        resp.writeHead(500)
                        resp.end()
                        return
                    }
                    if (!sockets.has(connectionID)) {
                        resp.writeHead(500)
                        resp.end()
                        return
                    }
                    let buffass: Buffer[] = []
                    let timeout: NodeJS.Timeout
                    let pqueue = new PQueue({ concurrency: 1 })
                    let length = 0
                    let finished = false
                    for await (const data of sockets.get(connectionID)!.socket!) {
                        logger("data arrived")
                        if (finished)
                            break
                        pqueue.add(() => {
                            logger("We are in the queue")
                            if (timeout)
                                clearTimeout(timeout)
                            length += data.length
                            buffass.push(data)
                            pqueue.add(async () => {
                                if (length > sockets.get(connectionID)?.bandwidth!) { // bigger than 2mb
                                    let msg = Buffer.concat(buffass)
                                    const iv = crypto.randomBytes(12)
                                    const cipher = crypto.createCipheriv("aes-256-gcm", symmetricKey, iv)
                                    const encryptedMsg = Buffer.concat([cipher.update(msg), cipher.final()])
                                    const tag = cipher.getAuthTag()
                                    resp.writeHead(200)
                                    resp.end(Buffer.concat([iv, tag, encryptedMsg]))
                                    buffass = []
                                    length = 0
                                    finished = true
                                }
                            })
                            timeout = setTimeout(() => {
                                if (!length)
                                    return
                                pqueue.add(async () => {
                                    let msg = Buffer.concat(buffass)
                                    const iv = crypto.randomBytes(12)
                                    const cipher = crypto.createCipheriv("aes-256-gcm", symmetricKey, iv)
                                    const encryptedMsg = Buffer.concat([cipher.update(msg), cipher.final()])
                                    const tag = cipher.getAuthTag()
                                    resp.writeHead(200)
                                    resp.end(Buffer.concat([iv, tag, encryptedMsg]))
                                    buffass = []
                                    length = 0
                                    finished = true
                                })
                            }, 100)
                        })
                    }
                }
            } catch (e) {
                resp.writeHead(500)
                resp.end()
                req.removeAllListeners()
            }
        })
    })
    server.listen(80, "0.0.0.0", () => {
        logger("Listening on 80", "info")
    })
} else {
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
                timeout: 60_000,
                maxSockets: config.concurrency
            }),
            connectionTimeout: 60_000,
            socketTimeout: 60_000
        },
        retryMode: 'adaptive',
        maxAttempts: 8,
        forcePathStyle: config.pathstyle
    })

    const s3 = new S3Client({
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
                timeout: 60_000,
                maxSockets: config.concurrency
            }),
            connectionTimeout: 60_000,
            socketTimeout: 60_000
        },
        retryMode: 'adaptive',
        maxAttempts: 8,
        forcePathStyle: config.pathstyle
    })

    const s32 = new S3Client({
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
                maxSockets: config.concurrency,
                timeout: 60_000,
            }),
            connectionTimeout: 60_000,
            socketTimeout: 60_000
        },
        retryMode: 'adaptive',
        maxAttempts: 8,
        forcePathStyle: config.pathstyle
    })

    let toDelete = new Set<string>()
    let toDeletePQueue = new PQueue({ concurrency: 100 })
    const intervalQueue = new PQueue({ concurrency: 1 })
    if (mode == "s3")
        setInterval(async () => {
            intervalQueue.add(async () => {
                const toDeleteCopy = [...toDelete]
                if (toDeleteCopy.length != 0) {
                    for (const connectionID of toDelete) {
                        try {
                            if (config.minimalClient) {
                                const ls = await Array.fromAsync(sclient.listObjects({ prefix: `proxy,${connectionID}/` }), (entry) => entry.key)
                                if (!ls.length) {
                                    logger(`Nothing to delete`, "info")
                                    continue
                                }
                                await Promise.all(ls.map(async key => await toDeletePQueue.add(async () => await sclient.deleteObject(key))))
                            } else {
                                const data2 = await justForDelete.send(new ListObjectsV2Command({
                                    Bucket: bucketName,
                                    Prefix: `proxy,${connectionID}/`,
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
            })
        }, 10000)

    const bucketName = config.bucket
    const interACKPool = new PQueue({ concurrency: 1000 })

    async function interAck(bandwidth: { dumber: number }, aborti: AbortController, connectionID: string, inSeq: string) {
        while (mode == "s3" ? !(aborti.signal.aborted) : true) {
            const now = Date.now()
            const bandkir = await popperBuffer2(`proxy,${connectionID}/${inSeq}`, connectionID, aborti)
            bandwidth.dumber = parseInt(Buffer.from(bandkir!).toString('utf8'))
            const nower = Date.now()
            await new Promise((resolve) => {
                if ((nower - now) < 3000)
                    setTimeout(resolve, nower - now)
            })
        }
    }

    let conn: Redis | null
    if (mode != 's3') {
        conn = new Redis(config.connstring, {
            maxRetriesPerRequest: null,
            keepAlive: 10000,
            tls: config.tls == "" ? {} : { servername: config.tls }
        })
        conn!.on('end', () => {
            logger("Lemme see thi happens or not conn")
            conn!.disconnect()
            conn = new Redis(config.connstring, {
                maxRetriesPerRequest: null,
                keepAlive: 10000,
                tls: config.tls == "" ? {} : { servername: config.tls }
            })
        })
    }
    if (mode != "s3" && config.pingAllT)
        try {
            await conn!.ping()
        } catch (e) {
        }

    async function popperBuffer2(key: string, connectionID: string, ctl: AbortController) {
        let dangoz = Date.now()
        let delay = 10
        for (let i = 0; i < 200; i++) {
            try {
                if (ctl.signal.aborted) {
                    logger("Freeing memory")
                    sockets.delete(connectionID)
                    toDelete.add(connectionID)
                    break
                }
                logger("Im getting this mother fucker so bad " + key)
                let bod: Uint8Array<ArrayBufferLike>
                if (config.minimalClient) {
                    const data = await sclient.getObject(key)
                    bod = new Uint8Array(await data.arrayBuffer())
                } else {
                    const data = await s3.send(new GetObjectCommand({ Bucket: bucketName, Key: key }))
                    bod = await data.Body!.transformToByteArray()
                }
                const extractIv = bod.subarray(0, 12)
                const tag = bod.subarray(12, 28)
                const encryptedChunk = bod.subarray(28)
                const decipher = crypto.createDecipheriv("aes-256-gcm", symmetricKey, extractIv)
                decipher.setAuthTag(tag)
                const decryptedChunk = Buffer.concat([decipher.update(encryptedChunk), decipher.final()])
                const realMsg = decryptedChunk.subarray(10)
                if (!Buffer.from('end', 'binary').compare(realMsg)) {
                    sockets.delete(connectionID)
                    toDelete.add(connectionID)
                    ctl.abort()
                    break
                }
                logger(`It took me ${Date.now() - dangoz}ms to actually receive this`)
                logger("Fucked?")
                return bod
            } catch (e) {
                await new Promise(r => setTimeout(r, delay))
                delay = Math.min(delay * 2, 500)
            }
        }
    }


    const callback = (payload: Uint8Array<ArrayBufferLike>) => {
        setImmediate(async () => {
            logger("Subarjerk")
            const extractIv = payload.subarray(0, 12)
            const tag = payload.subarray(12, 28)
            const encryptedChunk = payload.subarray(28)
            logger("Subarjerk after")
            const decipher = crypto.createDecipheriv("aes-256-gcm", symmetricKey, extractIv)
            decipher.setAuthTag(tag)
            const decryptedChunk = Buffer.concat([decipher.update(encryptedChunk), decipher.final()])
            const things = decryptedChunk.toString('utf8').split(',')!
            const dstaddr = things[0]!
            const dstport = parseInt(things[1]!)
            const connectionID = things[2]!
            const atyp = things[3]!
            try {
                let blconn1: Redis | null
                if (mode != "s3") {
                    blconn1 = new Redis(config.connstring, {
                        maxRetriesPerRequest: null,
                        keepAlive: 10000,
                        tls: config.tls == "" ? {} : { servername: config.tls }
                    })
                    blconn1.on('end', () => {
                        logger("Lemme see thi happens or not blconn1")
                        blconn1!.disconnect()
                        blconn1 = new Redis(config.connstring, {
                            maxRetriesPerRequest: null,
                            keepAlive: 10000,
                            tls: config.tls == "" ? {} : { servername: config.tls }
                        })
                    })
                }
                let ackconn: Redis | null
                if (mode != "s3") {
                    ackconn = new Redis(config.connstring, {
                        maxRetriesPerRequest: null,
                        keepAlive: 10000,
                        tls: config.tls == "" ? {} : { servername: config.tls }
                    })
                    ackconn.on('end', () => {
                        logger("Lemme see thi happens or not ackconn")
                        ackconn!.disconnect()
                        ackconn = new Redis(config.connstring, {
                            maxRetriesPerRequest: null,
                            keepAlive: 10000,
                            tls: config.tls == "" ? {} : { servername: config.tls }
                        })
                    })
                }

                if (mode != "s3" && config.pingAllT)
                    try {
                        await blconn1!.ping()
                        await ackconn!.ping()
                    } catch (e) {
                        return
                    }

                let pinger: NodeJS.Timeout | null
                if (mode != "s3" && config.pingAllT)
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

                let bandwidth = { dumber: 512 * 1024 }
                let inSeq = "0"
                let outSeq = "0"
                const aborti = new AbortController()
                while (mode == "s3" ? !(aborti.signal.aborted) : true) {
                    let request: Uint8Array<ArrayBufferLike> | undefined
                    if (mode != "s3") {
                        request = (await blconn1!.brpopBuffer(`proxy,${connectionID}`, 0))?.[1]
                    } else {
                        logger(`proxy,${connectionID}/${inSeq}`)
                        request = await popperBuffer2(`proxy,${connectionID}/${inSeq}`, connectionID, aborti)
                        interACKPool.add(async () => {
                            interAck(bandwidth, aborti, connectionID, inSeq)
                        })
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
                            toDelete.add(connectionID)
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
                    logger("The version now we want after " + inSeq)
                    if (!Buffer.from('end', 'binary').compare(decryptedChunk)) {
                        logger("Freeing memory from client")
                        sockets.get(connectionID)?.socket?.end()
                        sockets.delete(connectionID)
                        aborti.abort()
                        if (mode != "s3") {
                            clearInterval(pinger!)
                            await blconn1!.del(`proxy,${connectionID}`)
                            blconn1!.quit().catch(() => { })
                            ackconn!.quit().catch(() => { })
                        } else {
                            toDelete.add(connectionID)
                        }
                        break
                    }
                    let realMsg: Buffer<ArrayBuffer>
                    if (mode == "s3") {
                        logger("Ok this is")
                        const newVersion = decryptedChunk.subarray(0, 10)
                        inSeq = newVersion.toString('hex')
                        realMsg = decryptedChunk.subarray(10)
                    }

                    let buffered: Uint8Array<ArrayBufferLike> | undefined
                    if (mode != "s3") {
                        buffered = (await ackconn!.brpopBuffer(`ack,${connectionID}`, 0))?.[1]
                        if (!buffered) {
                            logger("Buffered issue")
                            sockets.get(connectionID)?.socket?.end()
                            sockets.delete(connectionID)
                            clearInterval(pinger!)
                            await blconn1!.del(`ack,${connectionID}`)
                            blconn1!.quit().catch(() => { })
                            ackconn!.quit().catch(() => { })
                            break
                        }
                        const extractIvACK = buffered!.subarray(0, 12)
                        const tagACK = buffered!.subarray(12, 28)
                        const encryptedChunkACK = buffered!.subarray(28)
                        const decipherACK = crypto.createDecipheriv("aes-256-gcm", symmetricKey, extractIvACK)
                        decipherACK.setAuthTag(tagACK)
                        const decryptedChunkACK = Buffer.concat([decipherACK.update(encryptedChunkACK), decipherACK.final()])
                        bandwidth.dumber = parseInt(decryptedChunkACK.toString('utf8'))

                    }

                    if (!sockets.has(connectionID)) {
                        let fastestWorkingIP: string | undefined
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
                                    if (config.minimalClient) {
                                        await sclient.putObject(`appserver,${connectionID}/${outSeq}`, Buffer.concat([iv, tag, encryptedMsg]))
                                    } else {
                                        await s3.send(new PutObjectCommand({
                                            Bucket: bucketName,
                                            Key: `appserver,${connectionID}/${outSeq}`,
                                            ACL: 'private',
                                            Body: Buffer.concat([iv, tag, encryptedMsg]),
                                        }))
                                    }
                                    logger("The version is now " + outSeq)
                                } catch (reason) {
                                    logger(`Problem with pushing appserver end ${reason}`, "error")
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
                        appServer.on('end', async () => {
                            logger("Server aborti")
                            if (mode != "s3") {
                                clearInterval(pinger!)
                                blconn1!.quit().catch(() => { })
                                ackconn!.quit().catch(() => { })
                                sockets.delete(connectionID)
                            } else {
                                aborti.abort()
                            }
                        })

                        sockets.set(connectionID, { socket: appServer })
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
                                    if (length > bandwidth.dumber) { // bigger than 2mb
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
                                            try {
                                                if (config.minimalClient) {
                                                    await sclient.putObject(`appserver,${connectionID}/${outSeq}`, Buffer.concat([iv, tag, encryptedMsg]))
                                                } else {
                                                    await s3.send(new PutObjectCommand({
                                                        Bucket: bucketName,
                                                        Key: `appserver,${connectionID}/${outSeq}`,
                                                        ACL: 'private',
                                                        Body: Buffer.concat([iv, tag, encryptedMsg]),
                                                    }))
                                                }
                                            } catch (reason) {
                                                logger(`Problem with pushing appserver batch ${reason}`, "error")
                                            }
                                            outSeq = newVersion!.toString('hex')
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
                                            try {
                                                if (config.minimalClient) {
                                                    await sclient.putObject(`appserver,${connectionID}/${outSeq}`, Buffer.concat([iv, tag, encryptedMsg]))
                                                } else {
                                                    await s3.send(new PutObjectCommand({
                                                        Bucket: bucketName,
                                                        Key: `appserver,${connectionID}/${outSeq}`,
                                                        ACL: 'private',
                                                        Body: Buffer.concat([iv, tag, encryptedMsg]),
                                                    }))
                                                }
                                            } catch (reason) {
                                                logger(`Problem with pushing appserver batch ${reason}`, "error")
                                            }
                                            outSeq = newVersion!.toString('hex')
                                            logger("It passed so means the fucking version is now this " + outSeq)
                                        }
                                        buffass = []
                                        length = 0
                                    })
                                }, 100)
                            })
                        })
                        // notify the proxy appserver dont sends data anymore (half close)
                    } else
                        sockets.get(connectionID)?.socket?.write(mode == "s3" ? realMsg! : decryptedChunk)
                }
            } catch (e) {
            }
        })
    }

    const allowedPrefixes = config.prefixes
    let usedInforms: string[] = []
    const performingQueues: PQueue<PriorityQueue, QueueAddOptions>[] = []
    for (let index = 0; index < allowedPrefixes.length; index++)
        performingQueues.push(new PQueue({ concurrency: config.concurrency }))
    const promisePool = new PQueue({ concurrency: 1000 })
    setImmediate(async () => {
        if (mode != "s3") {
            for (let i = 0; i < allowedPrefixes.length; i++) {
                setImmediate(async () => {
                    let blconn = new Redis(config.connstring, {
                        maxRetriesPerRequest: null,
                        keepAlive: 10000,
                        tls: config.tls == "" ? {} : { servername: config.tls }
                    })
                    blconn!.on('end', () => {
                        logger("Lemme see thi happens or not conn")
                        blconn!.disconnect()
                        blconn = new Redis(config.connstring, {
                            maxRetriesPerRequest: null,
                            keepAlive: 10000,
                            tls: config.tls == "" ? {} : { servername: config.tls }
                        })
                    })

                    if (mode != "s3" && config.pingAllT)
                        setInterval(async () => {
                            try {
                                await blconn.ping()
                            } catch (e) {
                            }
                        }, 10000)
                    try {
                        while (true) {
                            logger("SOMEHOW?")
                            const payload = await blconn.brpopBuffer(`inform${allowedPrefixes[i]}`, 0)
                            callback(payload?.[1]!)
                        }
                    } catch (e) {
                        await new Promise(r => setTimeout(r, 500))
                        logger("Bad shit redis " + e, "error")
                    }
                })
            }
        } else {
            logger("I reach there")
            await Promise.all(allowedPrefixes.map(async (prefix) => {
                while (true) {
                    try {
                        if (config.minimalClient) {
                            const ls = new Map((await Array.fromAsync(sclient.listObjects({ prefix: prefix + "/informs/" }), (entry) => entry.key)).map(k => [k, null]))

                            if (!ls.size) {
                                await new Promise(r => setTimeout(r, 500))
                                continue
                            }

                            const newLs = ls
                            const containList: string[] = []
                            for (const [element] of newLs) {
                                if (usedInforms.includes(element)) {
                                    containList.push(element)
                                    ls.delete(element)
                                }
                            }
                            promisePool.add(async () => {
                                logger("Deleting someshitaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
                                await Promise.all(containList.map(async (key) => {
                                    await sclient.deleteObject(key)
                                }))
                                logger("Deleeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeted?")
                            })
                            performingQueues[allowedPrefixes.indexOf(prefix)]!.add(async () => {
                                await Promise.all(ls.entries().map(async ([key]) => {
                                    try {
                                        logger("OK HERE!" + key)
                                        const daljerk = await sclient.getObject(key)
                                        logger("OK so now this means we really have the shit out of it")
                                        callback(new Uint8Array(await daljerk.arrayBuffer()))
                                    } catch (e) {
                                        logger("Bad batch " + e)
                                    }
                                })).catch((e) => {
                                    logger("Problem in inner loop " + e, "error")
                                })
                            }).catch((e) => {
                                logger("Problem in loop " + e, "error")
                            })
                            for (const [inform] of ls)
                                usedInforms.push(inform)

                        } else {
                            const data = await s32.send(new ListObjectsV2Command({
                                Bucket: bucketName,
                                Prefix: prefix + "/informs/",
                            }))

                            if (!data.Contents || !data.Contents.length) {
                                await new Promise(r => setTimeout(r, 500))
                                continue
                            }
                            let sagjerk = new Map(data.Contents!.map((each) => each.Key!).map(k => [k, null]))

                            const newLs = sagjerk
                            const containList: string[] = []
                            for (const [element] of newLs) {
                                if (usedInforms.includes(element)) {
                                    containList.push(element)
                                    sagjerk.delete(element)
                                }
                            }
                            promisePool.add(async () => {
                                logger("OOOOOOOOOOOOOOOK I WANNNNNNNNNNAAAAA DELETE " + containList.length)
                                if (!config.deleteManual) {
                                    try {
                                        await justForDelete.send(
                                            new DeleteObjectsCommand({
                                                Bucket: bucketName,
                                                Delete: {
                                                    Objects: containList.map(Key => ({ Key })),
                                                },
                                            })
                                        )
                                    } catch (e) {
                                        logger("Failed to delete with DeleteObjectsCommand trying with DeleteObject", "info")
                                        try {
                                            await Promise.all(containList.map(async (key) => {
                                                await justForDelete.send(new DeleteObjectCommand({
                                                    Bucket: bucketName,
                                                    Key: key
                                                }))
                                            }))
                                        } catch (e) {
                                            logger("Ok this failed too? why?")
                                            logger(e as any)
                                        }
                                    }
                                } else {
                                    try {
                                        await Promise.all(containList.map(async (key) => {
                                            await justForDelete.send(new DeleteObjectCommand({
                                                Bucket: bucketName,
                                                Key: key
                                            }))
                                        }))
                                    } catch (e) {
                                        logger("Ok this failed too? why?")
                                        logger(e as any)
                                    }
                                }
                                logger("FINIIIIIIIIIISHED?")
                            })

                            try {
                                performingQueues[allowedPrefixes.indexOf(prefix)]!.add(async () => {
                                    logger("OOOOOOOOOOOOOOOK I WANNNNNNNNNNAAAAA DELETE " + containList.length)
                                    await Promise.all(sagjerk.entries().map(async ([key]) => {
                                        try {
                                            logger("OK HERE! " + key)
                                            const daljerk = await s32.send(new GetObjectCommand({
                                                Bucket: bucketName, Key: key
                                            }))
                                            logger("OK so now this means we really have the shit out of it")
                                            callback(await daljerk.Body!.transformToByteArray())
                                        } catch (e) {
                                            logger("Bad batch " + e)
                                        }
                                    })).catch((e) => {
                                        logger("Problem in inner loop " + e, "error")
                                    })
                                }).catch((e) => {
                                    logger("Problem in loop " + e, "error")
                                })
                            } catch (e) {
                                await new Promise(r => setTimeout(r, 500))
                                logger("Bad delete " + e)
                            }

                            for (const [inform] of sagjerk)
                                usedInforms.push(inform)

                            setImmediate(async () => {
                                if (usedInforms.length >= 10000) {
                                    logger("Im deeeeeeeeeeeeeeeeeeeeeeeleeeeeeeeeeeeeetinggggggggggggggggg")
                                    usedInforms = []
                                }
                            })
                        }
                    } catch (e) {
                        await new Promise(r => setTimeout(r, 500))
                        logger("Bad shit s3 " + e, "error")
                    }
                }
            }))
        }
    })

    if (mode != "s3" && config.pingAllT)
        setInterval(async () => {
            try {
                await conn!.ping()
            } catch (e) {
            }
        }, 10000)

    process.on('uncaughtException', (error) => {
        logger(`BIG ISSUE ${error.cause}:${error.message}:${error.name}`, "error")
    })

    const finishCallback = async () => {
        if (conn)
            await conn.flushdb()
        else {
            try {
                if (config.minimalClient) {
                    const ls = await Array.fromAsync(sclient.listObjects({ prefix: "" }), (entry) => entry.key)
                    for (const element of ls) {
                        logger("Im deleting this " + element)
                    }
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
                        for (const element of data.Contents) {
                            logger("hooooooooooooooooooooooooooooooooooooooooooooooooooooooo")
                            sagjerk.push({ Key: element.Key! })
                        }
                        logger("dayoooooooooooooooooooooooooooooooooooooooooooooooooooooooos\n" + sagjerk.length)
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
        exit(0)
    }

    process.on('SIGTERM', async () => {
        await finishCallback()
    })

    process.on('SIGINT', async () => {
        await finishCallback()
    })
}