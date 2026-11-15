import * as net from 'net';

/**
 * TCP proxy in front of one Kafka broker. Every broker advertises its proxy address, so ALL client traffic
 * flows through here, which lets tests inject a realistic network fault: the broker receives and applies a
 * Produce request, but the client never sees the response and its connection dies, so it retries.
 */
export class KafkaProxy {
    private server?: net.Server;
    private sockets = new Set<net.Socket>();

    constructor(
        private readonly listenPort: number,
        private readonly targetPort: number,
        private readonly group: ProxyGroup,
    ) {}

    start(): Promise<void> {
        this.server = net.createServer((client) => {
            const upstream = net.connect(this.targetPort, '127.0.0.1');
            this.sockets.add(client).add(upstream);
            let swallow = false;
            const closeBoth = () => { client.destroy(); upstream.destroy(); };
            client.on('error', closeBoth); upstream.on('error', closeBoth);
            client.on('close', closeBoth); upstream.on('close', closeBoth);

            client.on('data', (chunk) => {
                // Kafka request frame: int32 size, int16 apiKey (0 = Produce)
                if (this.group.armed && chunk.length >= 6 && chunk.readInt16BE(4) === 0) {
                    this.group.fire();
                    swallow = true;
                    upstream.write(chunk);
                    // give the broker time to append (and replicate) the batch, then kill the client's connection
                    setTimeout(closeBoth, 1500);
                    return;
                }
                upstream.write(chunk);
            });
            upstream.on('data', (chunk) => { if (!swallow) client.write(chunk); });
        });
        return new Promise((resolve) => this.server!.listen(this.listenPort, '127.0.0.1', resolve));
    }

    async stop() {
        this.sockets.forEach((s) => s.destroy());
        await new Promise((resolve) => this.server?.close(resolve));
    }
}

export class ProxyGroup {
    armed = false;
    drops = 0;
    private proxies: KafkaProxy[] = [];

    constructor(ports: Array<[listen: number, target: number]>) {
        this.proxies = ports.map(([l, t]) => new KafkaProxy(l, t, this));
    }

    /** The next Produce request seen on any broker has its response swallowed and the client connection closed. */
    armDropNextProduceResponse() { this.armed = true; this.drops = 0; }
    fire() { this.armed = false; this.drops++; }

    async start() { await Promise.all(this.proxies.map((p) => p.start())); }
    async stop() { await Promise.all(this.proxies.map((p) => p.stop())); }
}

/**
 * Plain TCP proxy used to simulate a database outage: `pause()` kills every open connection and refuses new ones
 * (ECONNREFUSED), `resume()` lets traffic through again.
 */
export class TcpProxy {
    private server?: net.Server;
    private sockets = new Set<net.Socket>();

    constructor(private readonly listenPort: number, private readonly targetPort: number) {}

    async start(): Promise<void> {
        this.server = net.createServer((client) => {
            const upstream = net.connect(this.targetPort, '127.0.0.1');
            this.sockets.add(client).add(upstream);
            const closeBoth = () => { client.destroy(); upstream.destroy(); this.sockets.delete(client); this.sockets.delete(upstream); };
            client.on('error', closeBoth); upstream.on('error', closeBoth);
            client.on('close', closeBoth); upstream.on('close', closeBoth);
            client.pipe(upstream);
            upstream.pipe(client);
        });
        await new Promise<void>((resolve) => this.server!.listen(this.listenPort, '127.0.0.1', resolve));
    }

    async pause(): Promise<void> {
        const server = this.server!;
        this.server = undefined;
        this.sockets.forEach((s) => s.destroy());
        this.sockets.clear();
        await new Promise((resolve) => server.close(resolve));
    }

    resume(): Promise<void> {
        return this.start();
    }

    async stop(): Promise<void> {
        if (this.server) await this.pause();
    }
}
