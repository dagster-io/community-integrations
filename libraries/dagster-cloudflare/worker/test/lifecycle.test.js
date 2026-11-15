import {test} from 'node:test';
import assert from 'node:assert/strict';
import {transition} from '../src/lifecycle.js';

test('a repeated launch never starts a second container', () => {
  assert.deepEqual(transition(undefined, 'launch'), {state: 'starting', action: 'start'});
  for (const state of ['starting', 'running', 'canceling', 'canceled', 'success', 'failed'])
    assert.deepEqual(transition(state, 'launch'), {state, action: 'none'});
});

test('an exit seen while canceling ends as canceled', () => {
  assert.deepEqual(transition('running', 'cancel'), {state: 'canceling', action: 'stop'});
  assert.deepEqual(transition('canceling', 'success'), {state: 'canceled', action: 'none'});
  assert.deepEqual(transition('canceling', 'failed'), {state: 'canceled', action: 'none'});
});

test('terminal states do not change', () => {
  for (const state of ['canceled', 'success', 'failed'])
    for (const event of ['started', 'cancel', 'success', 'failed', 'cancel_failed'])
      assert.deepEqual(transition(state, event), {state, action: 'none'});
});

test('a failed cancel returns the run to running', () => {
  assert.deepEqual(transition('canceling', 'cancel_failed'), {state: 'running', action: 'none'});
});
