# Production deployment topology

TimeFusion must not publish TCP 5432 in Docker host mode. A single-node Swarm
cannot schedule a start-first replacement while the predecessor owns the same
host-mode port. `srv-timefusion-pgwire-proxy` owns that stable external port
and forwards to TimeFusion's overlay-network VIP instead.

Production proxy state:

- service: `srv-timefusion-pgwire-proxy`
- network: `captain-overlay-network`
- config: `timefusion-pgwire-proxy-v1`
- image: `haproxy@sha256:7c8dac975b9def049d6585b7efe865486acaa7b6ec5e74eec45f08fde8bb2ad4`
- published port: TCP 5432, host mode
- backend: `srv-captain--timefusion:5432`

The CapRover app publishes only TCP 50051. Apply
`caprover-service-override.yml` as its Service Update Override. Forward
deployments are start-first; rollbacks are stop-first.

The deployment workflow executes `FLUSH` and then leased `HANDOFF` before it
submits an update. A replacement binds an unhealthy 57P03 responder, waits for
the exclusive WAL-directory lock, and requests takeover. Only a drained
predecessor honors that request. The proxy continues routing to the healthy
old task until the new WAL owner reaches real PGWire readiness.

The HAProxy configuration deployed in Docker config
`timefusion-pgwire-proxy-v1` is recorded in `timefusion-pgwire-proxy.cfg`.
Docker Swarm persists both the service and config across daemon and host
restarts. Keep the proxy independent of the TimeFusion CapRover app so an app
deployment cannot replace the external listener.
