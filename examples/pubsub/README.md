# GCP Pub/Sub Transport

Production-ready pipeline with Google Cloud Pub/Sub transport.

**Showcases:**
- Pub/Sub transport with gRPC streaming pull
- Emulator detection via `PUBSUB_EMULATOR_HOST`
- Graceful shutdown with `transport.Close()`
- Workload Identity / ADC credential support

**Run:**
```bash
# Local (Google Pub/Sub emulator on port 8085)
gcloud beta emulators pubsub start --project=test-project
export PUBSUB_EMULATOR_HOST=localhost:8085
go run ./examples/pubsub/

# Production (GKE with Workload Identity)
export GCP_PROJECT_ID=my-project
export PUBSUB_TOPIC_ID=orders-topic
export PUBSUB_SUBSCRIPTION_ID=orders-sub
go run ./examples/pubsub/
```
