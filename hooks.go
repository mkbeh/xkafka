package xkafka

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Hook may implement one or more xkafka hook interfaces.
//
// Register hooks with [WithHooks]. Hooks are called in registration order.
// Context-returning hooks are chained, so each hook receives the context
// returned by the previous hook.
//
// Hook implementations must be safe for concurrent use and should return
// quickly.
type Hook any

type hooks []Hook

func (hs hooks) each(fn func(Hook)) {
	for _, h := range hs {
		fn(h)
	}
}

// HookNewClient is called after [NewClient] successfully creates a Client.
type HookNewClient interface {
	OnNewClient(*Client)
}

// HookClientClosed is called once after [Client.Shutdown] closes a Client.
type HookClientClosed interface {
	OnClientClosed(*Client)
}

// HookNewGroupTransactSession is called after [NewGroupTransactSession]
// successfully creates a GroupTransactSession.
type HookNewGroupTransactSession interface {
	OnNewGroupTransactSession(*GroupTransactSession)
}

// HookGroupTransactSessionClosed is called once after
// [GroupTransactSession.Shutdown] closes a GroupTransactSession.
type HookGroupTransactSessionClosed interface {
	OnGroupTransactSessionClosed(*GroupTransactSession)
}

// HookProduceStart is called before a synchronous produce operation starts.
//
// The returned context is passed to subsequent produce hooks and the underlying
// produce operation.
type HookProduceStart interface {
	OnProduceStart(ctx context.Context, records []*kgo.Record) context.Context
}

// HookProduceRecord is called before a record is passed to franz-go for
// producing.
//
// It is called for both synchronous and asynchronous produce operations. For
// synchronous produces, ctx is the context returned by the [HookProduceStart]
// chain. For asynchronous produces, record.Context is used when non-nil;
// otherwise the produce call context is used.
//
// Implementations may modify record before it is passed to franz-go, for
// example to inject propagation headers.
type HookProduceRecord interface {
	OnProduceRecord(ctx context.Context, record *kgo.Record)
}

// HookProduceEnd is called after a synchronous produce operation completes.
//
// duration measures the underlying synchronous produce operation. err is
// non-nil if any record failed to produce.
type HookProduceEnd interface {
	OnProduceEnd(ctx context.Context, records []*kgo.Record, duration time.Duration, err error)
}

// HookProduceError is called for each record that fails to produce.
type HookProduceError interface {
	OnProduceError(record *kgo.Record, err error)
}

// HookFetchError is called for each error reported while fetching records.
//
// recoverable reports whether the individual error is classified as
// non-terminal.
type HookFetchError interface {
	OnFetchError(ctx context.Context, topic string, partition int32, recoverable bool, err error)
}

// HookOffsetCommit is called after each manual consumer offset commit attempt.
//
// duration measures the commit attempt.
type HookOffsetCommit interface {
	OnOffsetCommit(ctx context.Context, duration time.Duration, err error)
}

// HookHandleStart is called before records are passed to the configured handler.
//
// The returned context is passed to subsequent handler hooks and the handler.
type HookHandleStart interface {
	OnHandleStart(ctx context.Context, records []*kgo.Record) context.Context
}

// HookHandleEnd is called after a handler attempt completes.
//
// duration measures handler execution. err contains the handler error,
// including an error converted from a recovered handler panic.
type HookHandleEnd interface {
	OnHandleEnd(ctx context.Context, records []*kgo.Record, duration time.Duration, err error)
}

// ShareAckOutcome describes the outcome assigned to a Share Group record.
type ShareAckOutcome string

const (
	// ShareAckAccept indicates that a record was successfully processed.
	ShareAckAccept ShareAckOutcome = "accept"

	// ShareAckRelease indicates that a record was released for redelivery.
	ShareAckRelease ShareAckOutcome = "release"

	// ShareAckReject indicates that a record was rejected.
	ShareAckReject ShareAckOutcome = "reject"
)

// HookShareAck is called when an acknowledgement outcome is assigned to one or
// more Share Group records.
//
// recordCount is the number of records assigned outcome. The hook is not called
// when recordCount is zero.
type HookShareAck interface {
	OnShareAck(ctx context.Context, outcome ShareAckOutcome, recordCount int)
}

// HookShareAckFlush is called after each Share Group acknowledgement flush
// attempt.
//
// duration measures the flush attempt.
type HookShareAckFlush interface {
	OnShareAckFlush(ctx context.Context, duration time.Duration, err error)
}

// TransactionType identifies the kind of transaction reported to transaction
// hooks.
type TransactionType string

const (
	// TransactionTypeProducer identifies a producer transaction started by
	// Client.RunInTx.
	TransactionTypeProducer TransactionType = "producer"

	// TransactionTypeGroup identifies a consume-process-produce transaction
	// managed by GroupTransactSession.
	TransactionTypeGroup TransactionType = "group"
)

// TransactionOutcome describes how a transaction attempt ended.
type TransactionOutcome string

const (
	// TransactionOutcomeCommit indicates that the transaction committed
	// successfully.
	TransactionOutcomeCommit TransactionOutcome = "commit"

	// TransactionOutcomeAbort indicates that the transaction was aborted
	// successfully.
	TransactionOutcomeAbort TransactionOutcome = "abort"

	// TransactionOutcomeError indicates that the transaction attempt ended with
	// an error and was not reported as a successful commit or abort.
	TransactionOutcomeError TransactionOutcome = "error"
)

// HookTransactionStart is called when a transaction attempt starts.
//
// The returned context is passed to subsequent transaction hooks and transaction
// processing.
type HookTransactionStart interface {
	OnTransactionStart(ctx context.Context, transactionType TransactionType) context.Context
}

// HookTransactionEnd is called after a transaction attempt has finished,
// including cleanup.
//
// outcome describes the final transaction result. duration covers the entire
// transaction attempt, including cleanup. err reports the associated error, if
// any.
type HookTransactionEnd interface {
	OnTransactionEnd(ctx context.Context, transactionType TransactionType, outcome TransactionOutcome, duration time.Duration, err error)
}

func (hs hooks) onNewClient(client *Client) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookNewClient); ok {
			h.OnNewClient(client)
		}
	})
}

func (hs hooks) onClientClosed(client *Client) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookClientClosed); ok {
			h.OnClientClosed(client)
		}
	})
}

func (hs hooks) onNewGroupTransactSession(session *GroupTransactSession) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookNewGroupTransactSession); ok {
			h.OnNewGroupTransactSession(session)
		}
	})
}

func (hs hooks) onGroupTransactSessionClosed(session *GroupTransactSession) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookGroupTransactSessionClosed); ok {
			h.OnGroupTransactSessionClosed(session)
		}
	})
}

func (hs hooks) onProduceStart(ctx context.Context, records []*kgo.Record) context.Context {
	hs.each(func(h Hook) {
		if h, ok := h.(HookProduceStart); ok {
			ctx = h.OnProduceStart(ctx, records)
		}
	})

	return ctx
}

func (hs hooks) onProduceRecord(ctx context.Context, record *kgo.Record) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookProduceRecord); ok {
			h.OnProduceRecord(ctx, record)
		}
	})
}

func (hs hooks) onProduceEnd(ctx context.Context, records []*kgo.Record, duration time.Duration, err error) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookProduceEnd); ok {
			h.OnProduceEnd(ctx, records, duration, err)
		}
	})
}

func (hs hooks) onProduceError(record *kgo.Record, err error) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookProduceError); ok {
			h.OnProduceError(record, err)
		}
	})
}

func (hs hooks) onFetchError(ctx context.Context, topic string, partition int32, recoverable bool, err error) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookFetchError); ok {
			h.OnFetchError(ctx, topic, partition, recoverable, err)
		}
	})
}

func (hs hooks) onOffsetCommit(ctx context.Context, duration time.Duration, err error) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookOffsetCommit); ok {
			h.OnOffsetCommit(ctx, duration, err)
		}
	})
}

func (hs hooks) onHandleStart(ctx context.Context, records []*kgo.Record) context.Context {
	hs.each(func(h Hook) {
		if h, ok := h.(HookHandleStart); ok {
			ctx = h.OnHandleStart(ctx, records)
		}
	})

	return ctx
}

func (hs hooks) onHandleEnd(ctx context.Context, records []*kgo.Record, duration time.Duration, err error) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookHandleEnd); ok {
			h.OnHandleEnd(ctx, records, duration, err)
		}
	})
}

func (hs hooks) onShareAck(ctx context.Context, outcome ShareAckOutcome, recordCount int) {
	if recordCount == 0 {
		return
	}

	hs.each(func(h Hook) {
		if h, ok := h.(HookShareAck); ok {
			h.OnShareAck(ctx, outcome, recordCount)
		}
	})
}

func (hs hooks) onShareAckFlush(ctx context.Context, duration time.Duration, err error) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookShareAckFlush); ok {
			h.OnShareAckFlush(ctx, duration, err)
		}
	})
}

func (hs hooks) onTransactionStart(ctx context.Context, transactionType TransactionType) context.Context {
	hs.each(func(h Hook) {
		if h, ok := h.(HookTransactionStart); ok {
			ctx = h.OnTransactionStart(ctx, transactionType)
		}
	})

	return ctx
}

func (hs hooks) onTransactionEnd(ctx context.Context, transactionType TransactionType, outcome TransactionOutcome, duration time.Duration, err error) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookTransactionEnd); ok {
			h.OnTransactionEnd(ctx, transactionType, outcome, duration, err)
		}
	})
}
