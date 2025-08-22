import { createClient } from "redis";
import { attempt, attemptAsync } from "ts-utils/check";
import { v4 } from "uuid";
import { ComplexEventEmitter, EventEmitter } from "ts-utils/event-emitter";
import { z } from "zod";
import { sleep } from "ts-utils/sleep";


/**
 * Listens to events emitted from other microservices via Redis pub/sub.
 * @template Events Event schema map
 */
export class ListeningService<Events extends Record<string, z.ZodTypeAny>> {
    private readonly em = new ComplexEventEmitter<{
        [K in keyof Events]: [{
            data: z.infer<Events[K]>;
            timestamp: number;
        }];
    }>();

    public readonly on = this.em.on.bind(this.em);
    public readonly off = this.em.off.bind(this.em);
    private readonly emit = this.em.emit.bind(this.em);
    public readonly once = this.em.once.bind(this.em);
    
    constructor(
        public readonly redis: Redis,
        public readonly target: string,
        public readonly events: Events,
    ) {}

    /**
     * Initializes the listener and subscribes to the target's channel.
     * @param config Optional configuration
     */
    public init(config?: {}) {
        return attempt(() => {
            this.redis.sub.subscribe(`channel:${this.target}`, (message) => {
                const parsed = z.object({
                    event: z.string().refine(e => Object.keys(this.events).includes(e), {
                        message: 'Unknown event',
                    }),
                    data: z.unknown(),
                    timestamp: z.number(),
                }).safeParse(JSON.parse(message));
                if (!parsed.success) {
                    this.redis.log(`Failed to parse message from ${this.target}:`, parsed.error);
                    return;
                }
                const { event, data, timestamp } = parsed.data;
                const schema = this.events[event as keyof Events];
                if (!schema) {
                    this.redis.log(`Received unknown event ${event} from ${this.target}`);
                    return;
                }
                const dataParsed = schema.safeParse(data);
                if (!dataParsed.success) {
                    this.redis.log(`Failed to parse data for event ${event} from ${this.target}:`, dataParsed.error);
                    return;
                }
                this.redis.log(`Received event ${event} from ${this.target}:`, dataParsed.data);
                this.emit(event as keyof Events, { data: dataParsed.data, timestamp });
            });
        });
    }
}

/**
 * Provides two-way, ack-based, type-safe communication between two microservices using Redis queues.
 * @template Events Event schema map
 */
export class ConnectionService<Events extends Record<string, z.ZodTypeAny>> {
    private messageId = -1;

    private readonly sendQueue: QueueService<{
        event: string;
        data?: unknown;
        id: number;
    }>;
    private readonly recieveQueue: QueueService<{
        event: string;
        data?: unknown;
        id: number;
    }>;

    private readonly ackQueue = new Map<number, (data: unknown) => void>();
    private readonly subscribers = new Map<keyof Events, (data: z.infer<Events[keyof Events]>) => unknown>();

    constructor(
        public readonly redis: Redis,
        public readonly target: string,
        public readonly events: Events,
    ) {
        const eventSchema = z.object({
            event: z.string().refine(e => Object.keys(events).includes(e), {
                message: 'Unknown event',
            }),
            data: z.unknown(),
            id: z.number(),
        });

        // cache if the connection dies
        this.sendQueue = redis.createQueue(`connection:${this.redis.name}:${this.target}`, eventSchema);
        this.recieveQueue = redis.createQueue(`connection:${this.target}:${this.redis.name}`, eventSchema);
    }

    /**
     * Initializes the connection service and starts listening for events and acks.
     * @param config Optional polling configuration
     */
    init(config?: {
        polling?: number;
    }) {
        return attempt(() => {
            this.sendQueue.init(config).unwrap();
            this.recieveQueue.init(config).unwrap();

            this.recieveQueue.on('data', async (data) => {
                const { event, data: eventData, id } = data;
                const schema = this.events[event as keyof Events];
                if (!schema) {
                    this.redis.log(`Received unknown event ${event} from ${this.target}`);
                    return;
                }
                const parsed = schema.safeParse(eventData);
                if (!parsed.success) {
                    this.redis.log(`Failed to parse event ${event} from ${this.target}:`, parsed.error);
                    return;
                }
                this.redis.log(`Received event ${event} from ${this.target}:`, parsed.data);

                const listener = this.subscribers.get(event as keyof Events);
                if (listener) {
                    try {
                        const res = await listener(parsed.data);
                        this.ack(id, res);
                    } catch (err) {
                        this.redis.log(`Error in listener for event ${event} from ${this.target}:`, err);
                    }
                } else {
                    this.redis.log(`No listener for event ${event} from ${this.target}`);
                }
            });

            this.redis.sub.subscribe(`connection:ack:${this.redis.name}:${this.target}`, (message) => {
                const parsed = z.object({
                    id: z.number(),
                    data: z.unknown(),
                }).safeParse(JSON.parse(message));
                if (!parsed.success) {
                    this.redis.log(`Received invalid ack message: ${message}`);
                    return;
                }
                const { id, data } = parsed.data;
                if (isNaN(id)) {
                    this.redis.log(`Received invalid ack message: ${message}`);
                    return;
                }
                this.redis.log(`Received ack for message id ${id} from ${this.target}`);
                const ack = this.ackQueue.get(id);
                if (ack) {
                    ack(data);
                }
            });
        });
    }

    /**
     * Sends an event to the target service and waits for an ack (and optional return value).
     * @template K Event key
     * @template R Return type
     * @param event Event name
     * @param config Data, optional timeout, and optional returnType for response
     * @returns Promise resolving to R (if returnType is provided) or void
     */
    send<K extends keyof Events, R = undefined>(event: K, config: {
        data: z.infer<Events[K]>;
        timeout?: number;
        returnType?: z.ZodType<R>;
    }) {
        return attemptAsync<R>(async () => {
            await this.sendQueue.add({ event: event as string, data: config.data, id: ++this.messageId });
            this.redis.log(`Sent event ${String(event)} to ${this.target}:`, config.data);

            return new Promise<R>((res, rej) => {
                const t = setTimeout(() => {
                    this.ackQueue.delete(this.messageId);
                    rej(new Error(`Ack timeout for message id ${this.messageId}`));
                }, config.timeout);

                this.ackQueue.set(this.messageId, (data) => {
                    clearTimeout(t);
                    this.ackQueue.delete(this.messageId);
                    if (config.returnType) {
                        const parsed = config.returnType.safeParse(data);
                        if (!parsed.success) {
                            rej(new Error(`Failed to parse return data for message id ${this.messageId}: ${parsed.error}`));
                            return;
                        }
                        res(parsed.data);
                    } else {
                        res(undefined as R);
                    }
                });
            });
        });
    }

    /**
     * Sends an ack for a received message back to the sender.
     * @param id Message id
     * @param data Optional return data
     */
    private ack(id: number, data: unknown) {
        return attemptAsync(async () => {
            await this.redis.pub.publish(`connection:ack:${this.target}:${this.redis.name}`, JSON.stringify({ id, data }));
            this.redis.log(`Sent ack for message id ${id} to ${this.target}`);
        });
    }

    /**
     * Subscribes to a specific event from the target service.
     * @template K Event key
     * @param event Event name
     * @param listener Listener function
     * @returns Unsubscribe function
     */
    public subscribe<K extends keyof Events>(event: K, listener: (data: z.infer<Events[K]>) => unknown) {
        if (this.subscribers.has(event)) {
            throw new Error(`Listener for event ${String(event)} already exists`);
        }
        this.subscribers.set(event, listener);
        return () => {
            this.subscribers.delete(event);
        }
    }
}

/**
 * Provides a type-safe Redis-backed queue with event emission for new data.
 * @template T Data type
 */
export class QueueService<T> {
    private readonly em = new ComplexEventEmitter<{
        data: [T];
        clear: void;
    }>();

    public readonly on = this.em.on.bind(this.em);
    public readonly off = this.em.off.bind(this.em);
    private readonly emit = this.em.emit.bind(this.em);
    public readonly once = this.em.once.bind(this.em);

    constructor(
        public readonly redis: Redis,
        public readonly name: string,
        public readonly schema: z.ZodType<T>,
    ) {}

    /**
     * Initializes the queue and starts polling for new items.
     * @param config Optional polling and jitter configuration
     */
    init(config?: {
        polling?: number;
        jitter?: number;
    }) {
        return attempt(() => {
            this.redis.log(`QueueService initialized for ${this.name}`);

            // listens for new items in the queue
            const listen = async () => {
                while (true) {
                    await sleep(config?.polling || 50 + Math.floor(Math.random() * (config?.jitter || 10)));
                    const item = await this.redis.cache.lPop(`queue:${this.name}`);
                    if (item) {
                        const parsed = this.schema.safeParse(JSON.parse(item));
                        if (parsed.success) {
                            this.redis.log(`Dequeued value for queue ${this.name}:`, parsed.data);
                            this.emit('data', parsed.data);
                        } else {
                            this.redis.log(`Failed to parse dequeued item for queue ${this.name}:`, parsed.error);
                        }
                    }
                }
            }

            listen().catch((err) => {
                this.redis.log(`Error in listening to queue ${this.name}:`, err);
            });
        });
    }

    /**
     * Returns all items currently in the queue.
     * @returns Promise of parsed items
     */
    stack() {
        return attemptAsync(async () => {
            const data = await this.redis.cache.lRange(`queue:${this.name}`, 0, -1);
            return data.map((item) => this.schema.parse(JSON.parse(item)));
        });
    }

    /**
     * Adds a value to the queue.
     * @param value Value to enqueue
     * @returns Promise of queue length after add
     */
    add(value: T) {
        return attemptAsync(async () => {
            const i = await this.redis.cache.rPush(`queue:${this.name}`, JSON.stringify(value));
            this.redis.log(`Enqueued value for queue ${this.name}:`, value);
            return i;
        });
    }

    /**
     * Returns the current length of the queue.
     * @returns Promise of queue length
     */
    length() {
        return attemptAsync(async () => {
            const len = await this.redis.cache.lLen(`queue:${this.name}`);
            this.redis.log(`Queue ${this.name} length:`, len);
            return len;
        });
    }

    /**
     * Clears all items from the queue.
     * @returns Promise<void>
     */
    clear() {
        return attemptAsync(async () => {
            await this.redis.cache.del(`queue:${this.name}`);
            this.redis.log(`Cleared queue ${this.name}`);
        });
    }
}

/**
 * Provides type-safe get/set/delete/expire operations for a single Redis key.
 * @template T Data type
 */
export class ItemService<T> {
    constructor(
        public readonly redis: Redis,
        public readonly name: string,
        public readonly schema: z.ZodType<T>,
    ) {
        this.redis.log(`ItemService initialized for ${name}`);
    }

    /**
     * Gets the value for this item from Redis.
     * @returns Promise of parsed value
     */
    get() {
        return attemptAsync<T>(async () => {
            const data = await this.redis.cache.get(`item:${this.name}`);
            if (data === null) {
                throw new Error(`Item ${this.name} not found`);
            }
            return this.schema.parse(JSON.parse(data));
        });
    }

    /**
     * Deletes this item from Redis.
     * @returns Promise<void>
     */
    delete() {
        return attemptAsync(async () => {
            await this.redis.cache.del(`item:${this.name}`);
            this.redis.log(`Deleted item ${this.name}`);
        });
    }

    /**
     * Sets the value for this item in Redis.
     * @param value Value to set
     * @returns Promise<void>
     */
    set(value: T) {
        return attemptAsync(async () => {
            await this.redis.cache.set(`item:${this.name}`, JSON.stringify(value));
            this.redis.log(`Set raw value for item ${this.name}:`, value);
        });
    }

    /**
     * Sets expiration for this item in seconds.
     * @param seconds Expiration time in seconds
     * @returns Promise<void>
     */
    expire(seconds: number) {
        return attemptAsync(async () => {
            await this.redis.cache.expire(`item:${this.name}`, seconds);
            this.redis.log(`Set expiration for item ${this.name} to ${seconds} seconds`);
        });
    }

    /**
     * Sets expiration for this item at a specific timestamp.
     * @param timestamp Unix timestamp (seconds)
     * @returns Promise<void>
     */
    expiresAt(timestamp: number) {
        return attemptAsync(async () => {
            await this.redis.cache.expireat(`item:${this.name}`, timestamp);
            this.redis.log(`Set expiration for item ${this.name} at ${new Date(timestamp * 1000).toISOString()}`);
        });
    }
}

export class NumberService extends ItemService<number> {
    constructor(redis: Redis, name: string) {
        super(redis, name, z.number());
        this.redis.log(`NumberService initialized for ${name}`);
    }

    /**
     * Increments the value by a given amount.
     * @param amount Amount to increment by
     * @returns Promise of new value
     */
    incr(amount = 1) {
        return attemptAsync(async () => {
            const val = await this.get().unwrapOr(0);
            const newValue = val + amount;
            await this.set(newValue);
            this.redis.log(`Incremented ${this.name} by ${amount}, new value: ${newValue}`);
            return newValue;
        });
    }

    /**
     * Decrements the value by a given amount.
     * @param amount Amount to decrement by
     * @returns Promise of new value
     */
    decr(amount = 1) {
        return this.incr(-amount);
    }
}

export class StringService extends ItemService<string> {
    constructor(redis: Redis, name: string) {
        super(redis, name, z.string());
        this.redis.log(`StringService initialized for ${name}`);
    }

    /**
     * Returns the length of the string value.
     * @returns Promise<number>
     */
    length() {
        return attemptAsync(async () => {
            return this.get().unwrap().then(v => v.length);
        });
    }
}


/**
 * Main Redis utility class for microservice communication, queue, and item management.
 */
export class Redis {
    public readonly id: string;
    public readonly pub: ReturnType<typeof createClient>;
    public readonly sub: ReturnType<typeof createClient>;
    public readonly cache: ReturnType<typeof createClient>;

    private readonly em = new ComplexEventEmitter();

    public readonly on = this.em.on.bind(this.em);
    public readonly off = this.em.off.bind(this.em);
    private readonly _emit = this.em.emit.bind(this.em);
    public readonly once = this.em.once.bind(this.em);
    
    constructor(
        public readonly config: {
            name: string;
            port?: number;
            host?: string;
            password?: string;
            id?: string;
            debug?: boolean;
        }
    ) {
        const { port = 6379, host = "localhost", password } = config;
        this.pub = createClient({ url: `redis://${host}:${port}`, password });
        this.sub = createClient({ url: `redis://${host}:${port}`, password });
        this.cache = createClient({ url: `redis://${host}:${port}`, password });
        this.id = config.id || v4();
    }

    // can be rewritten for NTP
    /**
     * Returns the current timestamp (ms). Can be replaced for NTP.
     */
    public now = () => Date.now();

    /**
     * Logs data if debug is enabled.
     * @param data Data to log
     */
    log(...data: unknown[]) {
        if (this.config.debug) {
            console.log(`[Redis ${this.config.name}]`, ...data);
        }
    }
    
    /**
     * Returns the configured name for this Redis instance.
     */
    get name() {
        return this.config.name;
    }

    /**
     * Initializes all Redis clients and performs discovery handshake.
     * @param timeout Optional timeout in ms
     * @returns Promise<void>
     */
    init(timeout?: number) {
        return attemptAsync(async () => {

            [this.pub, this.sub, this.cache].forEach((client) => {});


            await Promise.all([this.pub.connect(), this.sub.connect(), this.cache.connect()]);
            return new Promise<void>((res, rej) => {
                this.sub.subscribe('discovery:i_am', (message) => {
                    this.log(`Received discovery:iam message: ${message}`);
                    const [name, instanceId] = message.split(':');
                    this.log(`Discovery message from instance: ${name} (${instanceId})`, this.id);
                    if (instanceId === this.id) return res(); // Ignore our own message and resolve. The pub/sub system is working.
                    this.pub.publish('discovery:welcome', this.name + ':' + instanceId);
                    this.log(`Discovered instance: ${name} (${instanceId})`);
                });
                this.sub.subscribe('discovery:welcome', (message) => {
                    this.log(`Received discovery:welcome message: ${message}`);
                    const [name, instanceId] = message.split(':');
                    if (instanceId === this.id) return; // Ignore our own message

                    this.log(`Welcome message from instance: ${name} (${instanceId})`);
                    if (name === this.name) {
                        console.warn(
                            `Another instance of Redis with name "${this.name}" is already running. This may cause conflicts.`
                        );
                        res();
                    }
                });
                this.pub.publish('discovery:i_am', this.name + ':' + this.id);
                setTimeout(() => {
                    rej(new Error('Redis connection timed out. Please check your Redis server.'));
                }, timeout || 1000); // Wait for a second to ensure the discovery messages are processed
            });
        });
    }

    /**
     * Emits an event to this instance's channel.
     * @param event Event name
     * @param data Event data
     * @returns Promise<void>
     */
    emit(event: string, data: unknown) {
        return attemptAsync(async () => {
            this.pub.publish(`channel:${this.name}`, JSON.stringify({ event, data, timestamp: this.now() }) );
        });
    }

    /**
     * Creates an item service for a given key and type.
     * @template T Data type
     * @param name Key name
     * @param type Data type ('object', 'number', 'string')
     * @param schema Zod schema (required for 'object')
     * @returns ItemService, NumberService, or StringService
     */
    createItem<T>(name: string, type: 'object', schema: z.ZodType<T>): ItemService<T>;
    createItem(name: string, type: 'number'): NumberService;
    createItem(name: string, type: 'string'): StringService;
    createItem<T>(name: string, type: 'object' | 'number' | 'string', schema?: z.ZodType<T>) {
        this.log(`Creating item service for ${name} of type ${type}`);
        switch (type) {
            case 'object':
                if (!schema) throw new Error('Schema is required for object type');
                return new ItemService<T>(this, name, schema);
            case 'number':
                return new NumberService(this, name);
            case 'string':
                return new StringService(this, name);
            default:
                throw new Error(`Unknown item type: ${type}`);
        }
    }

    /**
     * Creates a queue service for a given name and schema.
     * @template T Data type
     * @param name Queue name
     * @param schema Zod schema
     * @returns QueueService<T>
     */
    createQueue<T>(name: string, schema: z.ZodType<T>) {
        this.log(`Creating queue service for ${name}`);
        return new QueueService<T>(this, name, schema);
    }

    /**
     * Creates a connection service for two-way communication with a target.
     * @template Events Event schema map
     * @param target Target service name
     * @param events Event schema map
     * @returns ConnectionService<Events>
     */
    createConnection<Events extends Record<string, z.ZodTypeAny>>(target: string, events: Events) {
        return new ConnectionService<Events>(this, target, events);
    }

    private readonly listeningServices = new Set<ListeningService<any>>();

    /**
     * Creates a listening service for pub/sub events from a target.
     * @template Events Event schema map
     * @param target Target service name
     * @param events Event schema map
     * @returns ListeningService<Events>
     */
    createListener<Events extends Record<string, z.ZodTypeAny>>(target: string, events: Events) {
        const service = new ListeningService<Events>(this, target, events);
        this.listeningServices.add(service);
        return service;
    }


	/**
	 * Closes all Redis clients and cleans up resources.
	 * @returns Promise<void>
	 */
	close() {
		return attemptAsync(async () => {
			await Promise.all([
				this.pub.quit(),
				this.sub.quit(),
				this.cache.quit(),
			]);
			this.listeningServices.clear();
		});
	}
}