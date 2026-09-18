import net, { Socket } from 'net'
import dns from 'dns/promises'
import { Redis } from 'ioredis'
import { exit } from 'process'
import PQueue from 'p-queue'
import crypto from 'node:crypto'

import config from '../config.json' with { type: 'json' }

const conn = new Redis(config.connstring, {
    maxRetriesPerRequest: null,
    tls: { servername: config.servername },
    keepAlive: 10000
})

const blconn = new Redis(config.connstring, {
    maxRetriesPerRequest: null,
    tls: { servername: config.servername },
    keepAlive: 10000
})

try {
    await conn.ping()
    await blconn.ping()
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
    console.log(type == "info" ? `[\x1b[33mINFO\x1b[0m] ${date.getHours()}:${date.getMinutes()}:${date.getSeconds()} ${param}`
        : (type == "error" ? `[\x1b[31mERR\x1b[0m] ${date.getHours()}:${date.getMinutes()}:${date.getSeconds()} ${param}` : param))
}

const sockets = new Map<string, Socket>()
setImmediate(async () => {
    while (true) {
        const payload = await blconn.brpopBuffer(`inform`, 0)
        const extractIv = payload![1].subarray(0, 12)
        const tag = payload![1].subarray(12, 28)
        const encryptedChunk = payload![1].subarray(28)
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

                let max = 1024 * 1024 * 2 // 2MB start
                const blconn1 = new Redis(config.connstring, {
                    maxRetriesPerRequest: null,
                    tls: { servername: config.servername },
                    keepAlive: 10000
                })

                const ackconn = new Redis(config.connstring, {
                    maxRetriesPerRequest: null,
                    tls: { servername: config.servername },
                    keepAlive: 10000
                })

                try {
                    await blconn1.ping()
                    await ackconn.ping()
                } catch (e) {
                    return
                }

                const imed = setImmediate(async () => {
                    while (true) {
                        const buffered = (await ackconn.brpopBuffer(`ack,${connectionID}`, 0))?.[1]
                        if (!buffered) {
                            clearInterval(pinger)
                            clearImmediate(imed)
                            blconn1.quit().catch(() => { })
                            ackconn.quit().catch(() => { })
                            sockets.delete(connectionID)
                            break
                        }
                        const extractIv = buffered.subarray(0, 12)
                        const tag = buffered.subarray(12, 28)
                        const encryptedChunk = buffered.subarray(28)
                        const decipher = crypto.createDecipheriv("aes-256-gcm", symmetricKey, extractIv)
                        decipher.setAuthTag(tag)
                        const decryptedChunk = Buffer.concat([decipher.update(encryptedChunk), decipher.final()])
                        const ack = decryptedChunk.toString('utf8')
                        // YEA I KNOW ITS NOT FUCKING RTT
                        const rtt = parseInt(ack)
                        if (rtt)
                            if (rtt > 10000)
                                max = Math.max((max / 2), 256 * 1024)
                            else
                                if (max < (1024 * 1024 * 2))
                                    max += (500 * 1024)
                        if (max > (2 * 1024 * 1024))
                            max = Math.max((max / 2), 256 * 1024)
                    }
                })

                const pinger = setInterval(async () => {
                    try {
                        await blconn1.ping()
                        await ackconn.ping()
                    } catch (e) {
                        clearInterval(pinger)
                        clearImmediate(imed)
                        blconn1.quit().catch(() => { })
                        ackconn.quit().catch(() => { })
                        sockets.delete(connectionID)
                    }
                }, 10000)

                while (true) {
                    const request = (await blconn1.brpopBuffer(`proxy,${connectionID}`, 0))?.[1]
                    if (!request) {
                        clearInterval(pinger)
                        clearImmediate(imed)
                        blconn1.quit().catch(() => { })
                        ackconn.quit().catch(() => { })
                        sockets.delete(connectionID)
                        break
                    }
                    const extractIv = request.subarray(0, 12)
                    const tag = request.subarray(12, 28)
                    const encryptedChunk = request.subarray(28)
                    const decipher = crypto.createDecipheriv("aes-256-gcm", symmetricKey, extractIv)
                    decipher.setAuthTag(tag)
                    const decryptedChunk = Buffer.concat([decipher.update(encryptedChunk), decipher.final()])

                    if (!Buffer.from('end', 'binary').compare(decryptedChunk)) {
                        sockets.delete(connectionID)
                        clearInterval(pinger)
                        clearImmediate(imed)
                        blconn1.quit().catch(() => { })
                        ackconn.quit().catch(() => { })
                        break
                    }
                    if (!sockets.has(connectionID)) {
                        let fastestWorkingIP: string | null
                        if (atyp === "3")
                            fastestWorkingIP = await getFastestIP(dstaddr, dstport)
                        else
                            fastestWorkingIP = dstaddr
                        if (!fastestWorkingIP) {
                            const msg = Buffer.from('end', 'binary')
                            const iv = crypto.randomBytes(12)
                            const cipher = crypto.createCipheriv("aes-256-gcm", symmetricKey, iv)
                            const encryptedMsg = Buffer.concat([cipher.update(msg), cipher.final()])
                            const tag = cipher.getAuthTag()
                            await conn.lpush(`appserver,${connectionID}`, Buffer.concat([iv, tag, encryptedMsg]))
                            clearInterval(pinger)
                            clearImmediate(imed)
                            blconn1.quit().catch(() => { })
                            ackconn.quit().catch(() => { })
                            sockets.delete(connectionID)
                            break
                        }
                        const appServer = net.createConnection(dstport, fastestWorkingIP)
                        sockets.set(connectionID, appServer)
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
                            clearInterval(pinger)
                            clearImmediate(imed)
                            sockets.delete(connectionID)
                            blconn1.quit().catch(() => { })
                            ackconn.quit().catch(() => { })
                            break
                        }

                        sockets.get(connectionID)?.write(decryptedChunk)
                        let buffass: Buffer[] = []
                        let timeout: NodeJS.Timeout
                        let pqueue = new PQueue({ concurrency: 1 })
                        let length = 0
                        appServer.on('data', (data: Buffer) => {
                            pqueue.add(() => {
                                if (timeout)
                                    clearTimeout(timeout)
                                length += data.length
                                buffass.push(data)
                                pqueue.add(async () => {
                                    if (length > max) { // bigger than 2mb
                                        const msg = Buffer.concat(buffass)
                                        const iv = crypto.randomBytes(12)
                                        const cipher = crypto.createCipheriv("aes-256-gcm", symmetricKey, iv)
                                        const encryptedMsg = Buffer.concat([cipher.update(msg), cipher.final()])
                                        const tag = cipher.getAuthTag()
                                        await conn.lpush(`appserver,${connectionID}`, Buffer.concat([iv, tag, encryptedMsg]))
                                        buffass = []
                                        length = 0
                                    }
                                })
                                timeout = setTimeout(() => {
                                    if (!length)
                                        return
                                    pqueue.add(async () => {
                                        const msg = Buffer.concat(buffass)
                                        const iv = crypto.randomBytes(12)
                                        const cipher = crypto.createCipheriv("aes-256-gcm", symmetricKey, iv)
                                        const encryptedMsg = Buffer.concat([cipher.update(msg), cipher.final()])
                                        const tag = cipher.getAuthTag()
                                        await conn.lpush(`appserver,${connectionID}`, Buffer.concat([iv, tag, encryptedMsg]))
                                        buffass = []
                                        length = 0
                                    })
                                }, 100)
                            })
                        })
                        // notify the proxy appserver dont sends data anymore (half close)
                        appServer.on('end', async () => {
                            clearInterval(pinger)
                            clearImmediate(imed)
                            blconn1.quit().catch(() => { })
                            ackconn.quit().catch(() => { })
                            sockets.delete(connectionID)
                        })
                    } else
                        sockets.get(connectionID)?.write(decryptedChunk)
                }
            } catch (e) {
            }
        })
    }
})

setInterval(async () => {
    try {
        await conn.ping()
        await blconn.ping()
    } catch (e) {
    }
}, 10000)

process.on('uncaughtException', (error) => {
    logger(`${error.cause}:${error.message}:${error.name}`, "error")
})

process.on('SIGTERM', async () => {
    await conn.flushdb()
    exit(0)
})

process.on('SIGINT', async () => {
    await conn.flushdb()
    exit(0)
})
