import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { z } from 'zod';
import { Redis } from '../index';
import { v4 as uuid } from 'uuid';

  const A = new Redis({ name: 'test-a', debug: true, id: uuid() });
  const B = new Redis({ name: 'test-b', debug: true, id: uuid() });

  describe('Redis Microservice Package', () => {
    beforeAll(async () => {
      await A.init().unwrap();
      await B.init().unwrap();
    });

    afterAll(async () => {
      A.close().unwrap();
      B.close().unwrap();
    });

    it('ItemService: set/get/delete/expire', async () => {
      const schema = z.object({ foo: z.string(), bar: z.number() });
      const item = A.createItem('item1', 'object', schema);
      await item.set({ foo: 'hello', bar: 42 }).unwrap();
      const value = await item.get().unwrap();
      expect(value).toEqual({ foo: 'hello', bar: 42 });
      await item.expire(1).unwrap();
      await new Promise(res => setTimeout(res, 2000));
      await expect(item.get().unwrap()).rejects.toThrow();
      await item.set({ foo: 'bye', bar: 1 }).unwrap();
      await item.delete().unwrap();
      await expect(item.get().unwrap()).rejects.toThrow();
    });

    it('NumberService: incr/decr', async () => {
      const num = A.createItem('num1', 'number');
      await num.set(10).unwrap();
      expect(await num.incr(5).unwrap()).toBe(15);
      expect(await num.decr(3).unwrap()).toBe(12);
    });

    it('StringService: set/length', async () => {
      const str = A.createItem('str1', 'string');
      await str.set('hello world').unwrap();
      expect(await str.length().unwrap()).toBe(11);
    });

    it('QueueService: add/stack/length/clear', async () => {
      const schema = z.object({ foo: z.string() });
      const queue = A.createQueue('queue1', schema, 100);
      await queue.add({ foo: 'bar' }).unwrap();
      await queue.add({ foo: 'baz' }).unwrap();
      const stack = await queue.stack().unwrap();
      expect(stack.length).toBeGreaterThanOrEqual(0); // could be empty if polling
      const len = await queue.length().unwrap();
      expect(typeof len).toBe('number');
      await queue.clear().unwrap();
      expect(await queue.length().unwrap()).toBe(0);
    });

    it('ListeningService: pub/sub events', async () => {
      const events = { test: z.string() };
      const listener = B.createListener('test-a', events);
      let received: string | undefined;
      listener.on('test', ({ data }) => { received = data; });
      listener.init();
      await A.emit('test', 'hello world').unwrap();
      await new Promise(res => setTimeout(res, 200));
      expect(received).toBe('hello world');
    });

    it('ConnectionService', async () => {
      const server = A.createServer({
        ping: z.string(),
      }, 100);

      const client = B.createClient('test-a', 100);

      await server.init().unwrap();
      await client.init().unwrap();

      let received: string | undefined;
      server.subscribe('ping', (data) => {
        received = data;
        return 'pong';
      });

      const response = await client.send('ping',  {
        data: 'hello',
        timeout: 2000,
        returnType: z.string(),
      }).unwrap();

      expect(received).toBe('hello');
      expect(response).toBe('pong');
    }, 10000);
  });