# Local integration smoke-test results

The smoke test was executed against the included coordinator and three separately launched worker processes communicating over gRPC in the local development container. This is a local test, not a test of the public Render deployment.

Observed output:

```text
PASS: 12 tasks completed over gRPC.
Assignment attempts: {'worker-1': 4, 'worker-2': 4, 'worker-3': 4}
Worker IDs used: ['worker-1', 'worker-2', 'worker-3']
PASS: 6 one-second tasks completed concurrently in 1.02s.
PASS: disabled worker was excluded from all six tasks.
```

The test verifies three things: batch work was assigned to all three workers; six one-second sleep tasks completed concurrently in approximately one wave; and a worker disabled in the scheduler received no new assignments. Hosted performance may differ, and the cloud deployment should be smoke-tested again after deployment.
