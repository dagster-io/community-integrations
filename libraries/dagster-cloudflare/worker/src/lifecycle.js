// Run states kept by the Durable Object. Dagster run IDs map 1:1 to objects,
// so a repeated launch request never starts a second container.
export const ACTIVE = new Set(['starting', 'running', 'canceling']);
export const TERMINAL = new Set(['success', 'failed', 'canceled']);

export function transition(state, event) {
  if (event === 'launch') return state ? {state, action: 'none'} : {state: 'starting', action: 'start'};
  if (!state || TERMINAL.has(state)) return {state, action: 'none'};
  switch (event) {
    case 'started':
      return {state: state === 'starting' ? 'running' : state, action: 'none'};
    case 'cancel':
      return state === 'canceling' ? {state, action: 'none'} : {state: 'canceling', action: 'stop'};
    case 'cancel_failed':
      return {state: state === 'canceling' ? 'running' : state, action: 'none'};
    case 'success':
    case 'failed':
      // An exit seen while canceling is the result of the cancel request.
      return {state: state === 'canceling' ? 'canceled' : event, action: 'none'};
    default:
      throw new Error(`Invalid lifecycle event: ${event}`);
  }
}
