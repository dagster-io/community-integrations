import {DurableObject} from 'cloudflare:workers';
import {RunObject, route} from './run.js';

export class RunContainer extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.run = new RunObject(ctx, env);
  }

  fetch(request) {
    return this.run.fetch(request);
  }

  alarm() {
    return this.run.alarm();
  }
}

export default {fetch: route};
