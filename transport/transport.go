// Package transport defines the Transport interface for queue backends.
//
// The Transport interface is defined in ringq (ringq/transport.go) to avoid
// circular imports, since it references ringq.Message and ringq.ConsumeHandler.
//
// Implementations:
//   - sqs:    github.com/vianhanif/go-task-orbit/transport/sqs
//   - memory: github.com/vianhanif/go-task-orbit/transport/memory
package transport
