// In-memory stand-ins for the Durable Object context and container.
import {webcrypto, timingSafeEqual} from 'node:crypto';

if (!webcrypto.subtle.timingSafeEqual) {
  webcrypto.subtle.timingSafeEqual = (a, b) => timingSafeEqual(Buffer.from(a), Buffer.from(b));
}

export class FakeStorage {
  constructor() { this.data = new Map(); this.alarmAt = null; }
  async get(key) { return this.data.get(key); }
  async put(key, value) { this.data.set(key, value); }
  async deleteAll() { this.data.clear(); this.alarmAt = null; }
  async setAlarm(at) { this.alarmAt = at; }
  async transaction(fn) { return fn(this); }
}

export class FakeContainer {
  constructor() {
    this.running = false;
    this.images = {default: 'registry/default:1', big: 'registry/big:1'};
    this.started = [];
    this.signals = [];
    this.destroyed = [];
    this.exitOnSigterm = true;
    this.startError = null;
    this.timeoutMs = null;
  }
  start(options) {
    if (this.startError) throw this.startError;
    if (this.running) throw new Error('already running');
    this.running = true;
    this.started.push(options);
    this.exit = new Promise((resolve, reject) => { this.resolveExit = resolve; this.rejectExit = reject; });
    this.exit.catch(() => {});
  }
  monitor() { return this.exit; }
  // Simulates the main process ending.
  finish(code) {
    this.running = false;
    if (code === 0) this.resolveExit();
    else this.rejectExit(Object.assign(new Error(`exit ${code}`), {exitCode: code}));
  }
  signal(sig) { this.signals.push(sig); if (sig === 15 && this.exitOnSigterm) this.finish(143); }
  async destroy(reason) { this.destroyed.push(reason); this.running = false; this.resolveExit(); }
  async setInactivityTimeout(ms) { this.timeoutMs = ms; }
}

export function fakeCtx() {
  const container = new FakeContainer();
  return {storage: new FakeStorage(), container, waitUntil: () => {}};
}

export const COMMAND = ['dagster', 'api', 'execute_run', '{"run_id":"r"}'];

export function request(method, body) {
  return new Request('https://w.example/runs/0b6f0f8e-1111-4222-8333-944445555666', {
    method,
    headers: {Authorization: 'Bearer secret', 'Content-Type': 'application/json'},
    body: body === undefined ? undefined : JSON.stringify(body),
  });
}
