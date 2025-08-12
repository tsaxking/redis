/* eslint-disable @typescript-eslint/no-explicit-any */

import { createClient } from 'redis';
import { attemptAsync, type ResultPromise, attempt, type Result } from 'ts-utils/check';
import { ComplexEventEmitter, EventEmitter } from 'ts-utils/event-emitter';
import { z } from 'zod';
import { v4 as u } from 'uuid';
import type { Stream } from 'ts-utils/stream';
import { sleep } from 'ts-utils/sleep';

const log = (...data: unknown[]) => {
	console.log(`[Redis]`, ...data);
};

export namespace Redis {
	// Devs can reassign this to a different UUID generator if they want
	// eslint-disable-next-line prefer-const
	export let uuid = u;

	export let REDIS_NAME = 'default';
	let messageId = -1;
	export const clientId = uuid();
	export let _sub: ReturnType<typeof createClient> | undefined;
	export let _pub: ReturnType<typeof createClient> | undefined;
	export let _queue: ReturnType<typeof createClient> | undefined;


	/**
	* Connects to Redis and initializes pub/sub/queue clients.
	* Attaches event listeners and performs instance discovery.
	* @param name Optional name for the Redis instance.
	* @param timeout Optional timeout in ms for discovery.
	* @returns ResultPromise<void>
	*/
	export const connect = (config: {
		name?: string;
		timeout?: number;
		port?: number;
		host?: string;
		password?: string;
		redisUrl?: string;
	}): ResultPromise<void> => {
		return attemptAsync(async () => {
            if (config.name?.includes(':')) {
                throw new Error(`Redis name "${name}" cannot contain a colon (:) character.`);
            }
            REDIS_NAME = config.name || REDIS_NAME;
			if (_sub?.isOpen && _pub?.isOpen && _sub?.isReady && _pub?.isReady) {
				return; // Already connected
			}
			_sub = createClient({
				url: config.redisUrl || `redis://${config.host || 'localhost'}:${config.port || 6379}`,
				password: config.password,
			});
			_pub = createClient({
				url: config.redisUrl || `redis://${config.host || 'localhost'}:${config.port || 6379}`,
				password: config.password,
			});
			_queue = createClient({
				url: config.redisUrl || `redis://${config.host || 'localhost'}:${config.port || 6379}`,
				password: config.password,
			});

            _sub.on('error', (error: Error) => {
                globalEmitter.emit('sub-error', error);
            });

            _sub.on('connect', () => {
                globalEmitter.emit('sub-connect');
            });
            _sub.on('disconnect', () => {
                globalEmitter.emit('sub-disconnect');
            });
            _sub.on('reconnect', () => {
                globalEmitter.emit('sub-reconnect');
            });

            _pub.on('error', (error: Error) => {
                globalEmitter.emit('pub-error', error);
            });
            _pub.on('connect', () => {
                globalEmitter.emit('pub-connect');
            });
            _pub.on('disconnect', () => {
                globalEmitter.emit('pub-disconnect');
            });
            _pub.on('reconnect', () => {
                globalEmitter.emit('pub-reconnect');
            });

            // Listen for direct messages
            _sub.subscribe(`direct:${REDIS_NAME}`, (message: string) => {
                
            });

            // Subscribe to direct messages


			await Promise.all([_sub.connect(), _pub.connect(), _queue.connect()]);
			return new Promise<void>((res, rej) => {
				_sub?.subscribe('discovery:i_am', (message) => {
					log(`Received discovery:iam message: ${message}`);
					const [name, instanceId] = message.split(':');
					log(`Discovery message from instance: ${name} (${instanceId})`, clientId);
					if (instanceId === clientId) return res(); // Ignore our own message and resolve. The pub/sub system is working.
					_pub?.publish('discovery:welcome', REDIS_NAME + ':' + instanceId);
					log(`Discovered instance: ${name} (${instanceId})`);
				});
				_sub?.subscribe('discovery:welcome', (message) => {
					log(`Received discovery:welcome message: ${message}`);
					const [name, instanceId] = message.split(':');
					if (instanceId === clientId) return; // Ignore our own message

					log(`Welcome message from instance: ${name} (${instanceId})`);
					if (name === REDIS_NAME) {
						console.warn(
							`Another instance of Redis with name "${REDIS_NAME}" is already running. This may cause conflicts.`
						);
						res();
					}
				});
				_pub?.publish('discovery:i_am', REDIS_NAME + ':' + clientId);
				setTimeout(() => {
					rej(new Error('Redis connection timed out. Please check your Redis server.'));
				}, timeout || 1000); // Wait for a second to ensure the discovery messages are processed
			});
		});
	};

	/**
	* Disconnects all Redis clients and cleans up resources.
	* @returns ResultPromise<void>
	*/
	export const disconnect = () => {
		return attemptAsync(async () => {
			if (_sub) {
				await _sub.close();
				_sub = undefined;
			}
			if (_pub) {
				await _pub.close();
				_pub = undefined;
			}
            if (_queue) {
                await _queue.close();
                _queue = undefined;
            }
		});
	};

	/**
	* Publishes a typed message to this server's Redis channel for other services to listen to.
	* @template T Event schema type
	* @param message The message to publish
	* @returns ResultPromise<void>
	*/
	const publish = <
		T extends {
			[key: string]: z.ZodType;
		}
	>(
		message: RedisMessage<T>
	): ResultPromise<void> => {
		return attemptAsync(async () => {
			const payload = JSON.stringify({
				event: message.event,
				data: message.data,
				date: message.date.toISOString(),
				id: message.id
			});
			_pub?.publish('channel:' + REDIS_NAME, payload);
		});
	};

	/**
	* Represents a typed Redis message for pub/sub.
	* @template T Event schema type
	*/
	type RedisMessage<
		T extends {
			[key: string]: z.ZodType;
		}
	> = {
		event: keyof T;
		data: z.infer<T[keyof T]>;
		date: Date;
		id: number;
	};

	/**
	* Service for listening to typed Redis events on a channel.
	* @template Events Event schema type
	* @template Name Channel name type
	*/
	class ListeningService<
		Events extends {
			[key: string]: z.ZodType;
		},
		Name extends string
	> {
		// public static services = new Map<string, ListeningService<any, any>>();

		private readonly em = new EventEmitter<{
			[K in keyof Events]: {
				date: Date;
				id: number;
				data: z.infer<Events[K]>;
			};
		}>();

		public on = this.em.on.bind(this.em);
		public once = this.em.once.bind(this.em);
		public off = this.em.off.bind(this.em);
		private emit = this.em.emit.bind(this.em);

		constructor(
			public readonly name: Name,
			public readonly events: Events
		) {
			if (name === REDIS_NAME) {
				console.warn(
					`Service name "${name}" cannot be the same as the Redis instance name "${REDIS_NAME}".`
				);
			}

			_sub?.subscribe('channel:' + this.name, (message: string) => {
				try {
					const parsed = z
						.object({
							event: z.string(),
							data: z.unknown(),
							date: z.string().transform((v) => new Date(v)),
							id: z.number()
						})
						.parse(JSON.parse(message));

					if (parsed.event in this.events) {
						const event = parsed.event as keyof Events;
						const dataSchema = this.events[event];
						const data = dataSchema.parse(parsed.data);
						this.emit(event, {
							data,
							date: parsed.date,
							id: parsed.id
						});
					}
				} catch (error) {
					console.error(`[Redis:${this.name}] Error parsing message for service:`, error);
				}
			});

			// ListeningService.services.set(this.name, this);
		}
	}

	/**
	* Creates a new ListeningService for a channel and event schema.
	* @template E Event schema type
	* @template Name Channel name type
	* @param name Channel name
	* @param events Event schema
	* @returns ListeningService<E, Name>
	*/
	export const createListeningService = <
		E extends {
			[key: string]: z.ZodType;
		},
		Name extends string
	>(
		name: Name,
		events: E
	) => {
		if (name.includes(':')) {
			throw new Error(`Service name "${name}" cannot contain a colon (:) character.`);
		}
		return new ListeningService(name, events);
	};

	/**
	* Emits an event to this server's Redis channel.
	* @param event Event name
	* @param data Event data
	* @returns ResultPromise<void>
	*/
	export const emit = (event: string, data: unknown) => {
		return publish({
			event,
			data,
			date: new Date(),
			id: messageId++
		});
	};

	/**
	* Sends an event to a specific server's Redis channel.
	* @param server Target server name
	* @param event Event name
	* @param data Event data
	* @returns ResultPromise<void>
	*/
	export const sendTo = (server: string, event: string, data: unknown) => {
        return attemptAsync<void>(async () => {
            const pub = getPub().unwrap();
            await pub.publish(
                `channel:${server}`,
                JSON.stringify({
                    event,
                    data,
                    date: new Date().toISOString(),
                    id: messageId++
                }),
            );
        });
    };

	/**
	* Global Redis events emitted by the system.
	*/
	type GlobalEvents = {
		'pub-error': [Error];
		'pub-connect': void;
		'pub-disconnect': void;
		'pub-reconnect': void;
		'sub-error': [Error];
		'sub-connect': void;
		'sub-disconnect': void;
		'sub-reconnect': void;
		'message': [string, unknown]; // Event name and data
	};

	const globalEmitter = new ComplexEventEmitter<GlobalEvents>();

	/**
	* Subscribes to a global Redis event.
	*/
	export const on = globalEmitter.on.bind(globalEmitter);
	/**
	 * Subscribes once to a global Redis event.
	 */
	export const once = globalEmitter.once.bind(globalEmitter);
	/**
	 * Unsubscribes from a global Redis event.
	 */
	export const off = globalEmitter.off.bind(globalEmitter);

	/**
	* Sends a query to a service and waits for a typed response.
	* @template Req Request type
	* @template Res Response type
	* @param service Target service name
	* @param event Event name
	* @param data Request data
	* @param returnType Zod schema for response
	* @param timeoutMs Timeout in ms
	* @returns ResultPromise<Res>
	*/
	export const query = <Req, Res>(
		service: string,
		event: string,
		data: Req,
		returnType: z.ZodType<Res>,
		timeoutMs = 1000
	) => {
		return attemptAsync<Res>(async () => {
			const requestId = uuid();
			const responseChannel = `response:${service}:${requestId}`;
			const queryChannel = `query:${service}:${event}`;

			const responsePromise = new Promise<Res>((resolve, reject) => {
				const timeout = setTimeout(() => {
					_sub?.unsubscribe(responseChannel);
					reject(new Error(`Request timed out after ${timeoutMs}ms`));
				}, timeoutMs);

				const ondata = (message: string) => {
					try {
						const parsed = z
							.object({
								data: z.unknown(),
								date: z.string().transform((v) => new Date(v)),
								id: z.number()
							})
							.parse(JSON.parse(message));

						const validated = returnType.safeParse(parsed.data);
						if (!validated.success) {
							return reject(
								new Error(`Invalid response data: ${JSON.stringify(validated.error.issues)}`)
							);
						}
						clearTimeout(timeout);
						resolve(validated.data);
					} catch (err) {
						clearTimeout(timeout);
						reject(err);
					}
					_sub?.unsubscribe(responseChannel);
				};

				_sub?.subscribe(responseChannel, ondata);
			});

			await _pub?.publish(
				queryChannel,
				JSON.stringify({
					data,
					requestId,
					responseChannel,
					date: new Date().toISOString(),
					id: messageId++
				})
			);

			return responsePromise;
		});
	};

	/**
	* Handler for query requests.
	* @template Req Request type
	*/
	type QueryHandler<Req> = (args: {
		data: Req;
		id: number;
		date: Date;
		requestId: string;
		responseChannel: string;
	}) => Promise<unknown> | unknown;

	/**
	* Listens for queries on a service/event and responds using a handler.
	* @template Req Request type
	* @param service Service name
	* @param event Event name
	* @param reqSchema Zod schema for request
	* @param handler Handler function
	*/
	export const queryListen = <Req>(
		service: string,
		event: string,
		reqSchema: z.ZodType<Req>,
		handler: QueryHandler<Req>
	) => {
		const channel = `query:${service}:${event}`;

		_sub?.subscribe(channel, async (message: string) => {
			try {
				const parsed = z
					.object({
						data: z.unknown(),
						requestId: z.string(),
						responseChannel: z.string(),
						date: z.string().transform((v) => new Date(v)),
						id: z.number()
					})
					.parse(JSON.parse(message));

				const validatedReq = reqSchema.safeParse(parsed.data);
				if (!validatedReq.success) {
					console.error(`[queryListen:${channel}] Invalid request:`, validatedReq.error);
					return;
				}

				const responseData = await handler({
					data: validatedReq.data,
					id: parsed.id,
					date: parsed.date,
					requestId: parsed.requestId,
					responseChannel: parsed.responseChannel
				});

				await _pub?.publish(
					parsed.responseChannel,
					JSON.stringify({
						data: responseData,
						date: new Date().toISOString(),
						id: messageId++
					})
				);
			} catch (err) {
				console.error(`[queryListen:${channel}] Error:`, err);
			}
		});
	};

	/**
	* Enqueues a task to a Redis queue.
	* @template T Task type
	* @param queueName Queue name
	* @param task Task data
	* @param notify Whether to notify subscribers
	* @returns ResultPromise<void>
	*/
	const enqueue = <T>(queueName: string, task: T, notify = false) => {
		return attemptAsync(async () => {
			const serialized = JSON.stringify(task);
			await _queue?.rPush(`queue:${queueName}`, serialized);
			if (notify) await _pub?.publish(`queue:${queueName}`, serialized); // Optional: publish to notify subscribers
		});
	};

	/**
	* Dequeues a task from a Redis queue, validating with schema.
	* @template T Task type
	* @param queueName Queue name
	* @param schema Zod schema for task
	* @param timeout Timeout in seconds
	* @returns ResultPromise<T | null>
	*/
	const dequeue = <T>(queueName: string, schema: z.ZodType<T>, timeout = 0) => {
		return attemptAsync<T | null>(async () => {
			const key = `queue:${queueName}`;
			const result = await _queue?.blPop(key, timeout);

			if (!result || !result.element) return null;

			try {
				const parsed = JSON.parse(result.element);
				return schema.parse(parsed);
			} catch (err) {
				console.error(`[dequeue:${key}] Failed to parse or validate task`, err);
				throw err;
			}
		});
	};

	/**
	* Clears all tasks from a Redis queue.
	* @param queueName Queue name
	* @returns ResultPromise<void>
	*/
	const clearQueue = (queueName: string) => {
		return attemptAsync(async () => {
			const key = `queue:${queueName}`;
			await _queue?.del(key);
		});
	};

	/**
	* Gets the length of a Redis queue.
	* @param queueName Queue name
	* @returns ResultPromise<number>
	*/
	const getQueueLength = (queueName: string) => {
		return attemptAsync(async () => {
			const key = `queue:${queueName}`;
			const length = await _queue?.lLen(key);
			return length ?? 0;
		});
	};

	/**
	* Service for managing a Redis-backed queue with typed tasks.
	* @template T Task type
	*/
	class QueueService<T> {
		private _running = false;
		private em = new EventEmitter<{
			data: T;
			stop: void;
			error: Error;
			start: void;
		}>();

		public on = this.em.on.bind(this.em);
		public once = this.em.once.bind(this.em);
		public off = this.em.off.bind(this.em);

		constructor(
			public readonly name: string,
			public readonly schema: z.ZodType<T>
		) {}

		put(data: T, notify = false) {
			return enqueue(this.name, data, notify);
		}

		length() {
			return getQueueLength(this.name);
		}

		clear() {
			return clearQueue(this.name);
		}

		start() {
			if (this._running) {
				console.warn(`QueueService "${this.name}" is already running.`);
				return this.stop.bind(this);
			}

			this._running = true;
			const run = async () => {
				while (this._running) {
					try {
						const task = await dequeue(this.name, this.schema, 1000).unwrap();
						if (task) {
							this.em.emit('data', task);
						} else {
							// No task available, wait a bit before checking again
							await new Promise((resolve) => setTimeout(resolve, 100));
						}
					} catch (err) {
						console.error(`[QueueService:${this.name}] Error processing task:`, err);
						this.em.emit('error', err as Error);
					}
					await sleep(100); // Prevent tight loop
				}
			};

			run().catch((err) => {
				console.error(`[QueueService:${this.name}] Error in run loop:`, err);
				this.em.emit('error', err as Error);
			});

			return this.stop.bind(this);
		}

		stop() {
			this._running = false;
		}

		get running() {
			return this._running;
		}
	}

	/**
	* Creates a new QueueService for a queue and schema.
	* @template T Task type
	* @param queueName Queue name
	* @param schema Zod schema for task
	* @returns QueueService<T>
	*/
	export const createQueueService = <T>(queueName: string, schema: z.ZodType<T>) => {
		return new QueueService(queueName, schema);
	};

	/**
	* Data packet for a Redis stream.
	* @template T Stream data type
	*/
	type StreamData<T> = {
		data: T;
		date: Date;
		packet: number;
		id: number;
	};

	/**
	* End packet for a Redis stream.
	*/
	type StreamEnd = {
		id: number;
		date: Date;
	};

	/**
	* Emits a stream of data packets to a Redis stream channel.
	* @template T Stream data type
	* @param streamName Stream channel name
	* @param stream Stream object
	* @returns ResultPromise<void>
	*/
	export const emitStream = <T>(streamName: string, stream: Stream<T>) => {
		return attemptAsync(async () => {
			const id = messageId++;
			let packet = 0;

			stream.on('data', async (data) => {
				const payload: StreamData<T> = {
					data,
					date: new Date(),
					packet: packet++,
					id
				};

				const serialized = JSON.stringify(payload);
				await _pub?.publish(`stream:${streamName}`, serialized);
			});

			stream.once('end', async () => {
				const endPayload: StreamEnd = {
					id,
					date: new Date()
				};

				const serializedEnd = JSON.stringify(endPayload);
				await _pub?.publish(`stream:${streamName}`, serializedEnd);
			});

			return new Promise<void>((res) => {
				stream.on('end', res);
			});
		});
	};

	/**
	* Listens for data packets on a Redis stream channel.
	* @template T Stream data type
	* @param streamName Stream channel name
	* @param schema Zod schema for stream data
	* @param handler Handler for data packets
	* @param onEnd Optional handler for stream end
	* @returns ResultPromise<void>
	*/
	export const listenStream = <T>(
		streamName: string,
		schema: z.ZodType<T>,
		handler: (data: T, date: Date, packet: number, id: number) => void,
		onEnd?: (id: number, date: Date) => void
	) => {
		return attemptAsync<void>(async () => {
			const streamDataSchema = z.object({
				data: z.unknown(),
				date: z.string().transform((v) => new Date(v)),
				packet: z.number(),
				id: z.number()
			});

			const streamEndSchema = z.object({
				id: z.number(),
				date: z.string().transform((v) => new Date(v))
			});

			await _sub?.subscribe(`stream:${streamName}`, (message: string) => {
				try {
					const raw = JSON.parse(message);

					// Try parsing as StreamData
					if ('data' in (raw as any) && 'packet' in (raw as any)) {
						const parsed = streamDataSchema.parse(raw);
						const validated = schema.parse(parsed.data);
						handler(validated, parsed.date, parsed.packet, parsed.id);
					} else {
						// Fallback to StreamEnd
						const parsed = streamEndSchema.parse(raw);
						onEnd?.(parsed.id, parsed.date);
					}
				} catch (err) {
					console.error(`[listenStream:${streamName}] Invalid stream message:`, err);
				}
			});
		});
	};

	/**
	* Sets a value in Redis with optional TTL.
	* @param key Redis key
	* @param value Value to set
	* @param ttl Optional time-to-live in seconds
	* @returns ResultPromise<void>
	*/
	export const setValue = (key: string, value: unknown, ttl?: number) => {
		return attemptAsync(async () => {
			const serializedValue = JSON.stringify(value);
			await _pub?.set(key, serializedValue);
			if (ttl) {
				await _pub?.expire(key, ttl);
			}
			// log(`Set value for key "${key}":`, value);
		});
	};

	/**
	* Gets and validates a value from Redis.
	* @param key Redis key
	* @param returnType Zod schema for value
	* @returns ResultPromise<any | null>
	*/
	export const getValue = (key: string, returnType: z.ZodType) => {
		return attemptAsync(async () => {
			const serializedValue = await _pub?.get(key);
			if (!serializedValue) {
				// log(`No value found for key "${key}"`);
				return null;
			}
			try {
				const parsedValue = JSON.parse(serializedValue);
				return returnType.parse(parsedValue);
			} catch (err) {
				console.error(`Error parsing value for key "${key}":`, err);
				throw new Error(`Invalid value for key "${key}"`);
			}
		});
	};

	/**
	* Gets the Redis publisher client, or throws if not initialized.
	* @returns Result<RedisClient>
	*/
	export const getPub = (): Result<ReturnType<typeof createClient>> => {
		return attempt(() => {
			if (!_pub) {
				throw new Error('Redis publisher client is not initialized. Call connect() first.');
			}
			return _pub;
		});
	};

	/**
	* Gets the Redis subscriber client, or throws if not initialized.
	* @returns Result<RedisClient>
	*/
	export const getSub = (): Result<ReturnType<typeof createClient>> => {
		return attempt(() => {
			if (!_sub) {
				throw new Error('Redis subscriber client is not initialized. Call connect() first.');
			}
			return _sub;
		});
	};

	/**
	* Increments a Redis key by a given value, initializing if needed.
	* @param key Redis key
	* @param increment Amount to increment by
	* @returns ResultPromise<number>
	*/
	export const incr = (key: string, increment = 1) => {
		return attemptAsync(async () => {
			if (!_pub) {
				throw new Error('Redis publisher client is not initialized. Call connect() first.');
			}
			const has = await _pub.exists(key);
			if (!has) {
				// log(`Key "${key}" does not exist. Initializing to 0 before incrementing.`);
				await _pub.set(key, '0');
			}

			const count = await _pub.incrBy(key, increment);
			// log(`Incremented key "${key}" by ${increment}. New value: ${count}`);
			return count;
		});
	};

	/**
	* Sets expiration for a Redis key in seconds.
	* @param key Redis key
	* @param seconds Expiration time in seconds
	* @returns ResultPromise<number>
	*/
	export const expire = (key: string, seconds: number) => {
		return attemptAsync(async () => {
			if (!_pub) {
				throw new Error('Redis publisher client is not initialized. Call connect() first.');
			}
			const result = await _pub.expire(key, seconds);
			// if (result) {
			// 	log(`Set expiration for key "${key}" to ${seconds} seconds.`);
			// } else {
			// 	log(`Failed to set expiration for key "${key}". Key may not exist.`);
			// }
			return result;
		});
	};
}
