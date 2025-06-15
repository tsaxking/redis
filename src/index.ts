import { createClient } from 'redis';
import { attemptAsync, type ResultPromise } from '../node_modules/ts-utils/src/check';
import { EventEmitter } from '../node_modules/ts-utils/src/event-emitter';
import { z } from 'zod';

export namespace Redis {
    const sub = createClient();
    const pub = createClient();

    sub.on('error', (error: Error) => {
        globalEmitter.emit('sub-error', error);
    });

    sub.on('connect', () => {
        globalEmitter.emit('sub-connect', undefined);
    });
    sub.on('disconnect', () => {
        globalEmitter.emit('sub-disconnect', undefined);
    });
    sub.on('reconnect', () => {
        globalEmitter.emit('sub-reconnect', undefined);
    });

    pub.on('error', (error: Error) => {
        globalEmitter.emit('pub-error', error);
    });
    pub.on('connect', () => {
        globalEmitter.emit('pub-connect', undefined);
    });
    pub.on('disconnect', () => {
        globalEmitter.emit('pub-disconnect', undefined);
    });
    pub.on('reconnect', () => {
        globalEmitter.emit('pub-reconnect', undefined);
    });


    export const connect = (): ResultPromise<void> => {
        return attemptAsync(async () => {
            Promise.all([
                sub.connect(),
                pub.connect()
            ]);
        });
    };

    const send = <
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
            pub.publish(message.event as string, payload);
        });
    }

    type RedisMessage<
        T extends {
            [key: string]: z.ZodType;
        }
    > = {
        event: keyof T;
        data: z.infer<T[keyof T]>;
        date: Date;
        id: number;
    }

    type RedisCallback<E extends string, Type extends z.ZodType> = (data: z.infer<Type>, metadata: {
        event: E;
        date: Date;
        id: number;
    }) => void;


    class Channel<From extends {
        [key: string]: z.ZodType;
    }, To extends {
        [key: string]: z.ZodType;
    }> {
        private id = -1;

        constructor(
            public readonly channel: string,
            private readonly fromEvents: From,
            private readonly toEvents: To
        ) {
            sub.subscribe(channel, (message) => {
                try {
                    const parsed = z.object({
                        event: z.string(),
                        data: z.unknown(),
                        date: z.string().transform(date => new Date(date)),
                        id: z.number(),
                    }).parse(JSON.parse(message));
                    const event = parsed.event as keyof From;
                    if (!this.listeners.has(event)) {
                        console.warn(`No listeners for event ${String(event)} in channel ${this.channel}`);
                        return;
                    }

                    const data = this.fromEvents[event].parse(parsed.data);
                    const metadata = {
                        event: parsed.event,
                        date: parsed.date,
                        id: parsed.id
                    };
                    const cbs = this.listeners.get(event) || [];
                    for (const cb of cbs) {
                        cb(data, metadata as any);
                    }

                } catch (error) {
                    globalEmitter.emit('sub-error', error as Error);
                }
            });
        }

        private listeners = new Map<keyof From, RedisCallback<Extract<keyof From, string>, From[keyof From]>[]>();

        send(event: keyof To, data: z.infer<To[keyof To]>) {
            return attemptAsync(async () => {
                if (!(event in this.toEvents)) {
                    throw new Error(`Event ${String(event)} is not defined in the channel ${this.channel}`);
                }

                return send<To>({
                    event,
                    data,
                    date: new Date(),
                    id: this.id++
                }).unwrap();
            });
        }

        on<E extends keyof From>(
            event: E,
            cb: RedisCallback<Extract<E, string>, From[E]>
        ) {
            if (!(event in this.fromEvents)) {
                throw new Error(`Event ${String(event)} is not defined in the channel ${this.channel}`);
            }

            if (!this.listeners.has(event)) {
                this.listeners.set(event, []);
            }
            this.listeners.get(event)?.push(cb as any);

            return () => {
                const cbs = this.listeners.get(event);
                if (cbs) {
                    this.listeners.set(event, cbs.filter(callback => callback !== cb));
                }
            }
        }
    }

    type GlobalEvents = {
        'pub-error': Error;
        'pub-connect': void;
        'pub-disconnect': void;
        'pub-reconnect': void;
        'sub-error': Error;
        'sub-connect': void;
        'sub-disconnect': void;
        'sub-reconnect': void;
    }

    const globalEmitter = new EventEmitter<GlobalEvents>();

    export const on = globalEmitter.on.bind(globalEmitter);
    export const emit = globalEmitter.emit.bind(globalEmitter);
    export const once = globalEmitter.once.bind(globalEmitter);
    export const off = globalEmitter.off.bind(globalEmitter);
    export const createChannel = <
    From extends {
        [key: string]: z.ZodType;
    }, To extends {
        [key: string]: z.ZodType;
    }>(channel: string, fromEvents: From, toEvents: To) => {
        return new Channel<From, To>(channel, fromEvents, toEvents);
    };
}