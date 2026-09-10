package xkafka

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

///////////////////////////////////////////////////////////////
// NOTE:                                                     //
// NOTE: Make sure new hooks are checked in implementsAnyHook //
// NOTE:                                                     //
///////////////////////////////////////////////////////////////

// Hook is a hook to be called when something happens in xkafka.
//
// The base Hook interface is meaningless, but wherever a hook can occur in
// xkafka, the client checks if the hook implements the appropriate interface.
// If so, the hook is called.
//
// This allows hooks to implement only the behavior they care about, and allows
// xkafka to add more hooks in the future. Hooks must be safe for concurrent use
// and are expected to be fast.
type Hook any

type hooks []Hook

func (hs hooks) each(fn func(Hook)) {
	for _, h := range hs {
		fn(h)
	}
}

// HookNewClient is called after a Client is created.
type HookNewClient interface {
	OnNewClient(*Client)
}

// HookClientClosed is called after a Client is closed.
type HookClientClosed interface {
	OnClientClosed(*Client)
}

// HookNewGroupTransactSession is called after a GroupTransactSession is created.
type HookNewGroupTransactSession interface {
	OnNewGroupTransactSession(*GroupTransactSession)
}

// HookGroupTransactSessionClosed is called after a GroupTransactSession is closed.
type HookGroupTransactSessionClosed interface {
	OnGroupTransactSessionClosed(*GroupTransactSession)
}

// HookProduceStart is called before a synchronous produce operation starts.
//
// The returned context is passed to HookProduceRecord, the produce operation,
// and HookProduceEnd.
type HookProduceStart interface {
	OnProduceStart(ctx context.Context, records []*kgo.Record) context.Context
}

// HookProduceRecord is called before a record is passed to franz-go for producing.
//
// Implementations may modify the record before it is passed to franz-go, for
// example to inject propagation headers.
type HookProduceRecord interface {
	OnProduceRecord(ctx context.Context, record *kgo.Record)
}

// HookProduceEnd is called after a synchronous produce operation ends.
type HookProduceEnd interface {
	OnProduceEnd(ctx context.Context, records []*kgo.Record, duration time.Duration, err error)
}

// HookProduceError is called when producing a record fails.
type HookProduceError interface {
	OnProduceError(record *kgo.Record, err error)
}

// HookFetchError is called when fetching records fails.
type HookFetchError interface {
	OnFetchError(ctx context.Context, topic string, partition int32, recoverable bool, err error)
}

// HookOffsetCommit is called after a consumer offset commit attempt ends.
type HookOffsetCommit interface {
	OnOffsetCommit(ctx context.Context, duration time.Duration, err error)
}

// HookHandleStart is called before records are passed to the configured handler.
//
// The returned context is passed to subsequent handler hooks and to the handler.
type HookHandleStart interface {
	OnHandleStart(ctx context.Context, records []*kgo.Record) context.Context
}

// HookHandleEnd is called after the configured handler returns.
type HookHandleEnd interface {
	OnHandleEnd(ctx context.Context, records []*kgo.Record, duration time.Duration, err error)
}

// ShareAckOutcome describes the outcome of a Share Group acknowledgement.
type ShareAckOutcome string

const (
	ShareAckAccept  ShareAckOutcome = "accept"
	ShareAckRelease ShareAckOutcome = "release"
	ShareAckReject  ShareAckOutcome = "reject"
)

// HookShareAck is called when acknowledgement outcomes are assigned to
// Share Group records.
type HookShareAck interface {
	OnShareAck(ctx context.Context, outcome ShareAckOutcome, recordCount int)
}

// HookShareAckFlush is called after a Share Group acknowledgement flush attempt ends.
type HookShareAckFlush interface {
	OnShareAckFlush(ctx context.Context, duration time.Duration, err error)
}

// TransactionType identifies the xkafka transaction runtime.
type TransactionType string

const (
	TransactionTypeProducer TransactionType = "producer"
	TransactionTypeGroup    TransactionType = "group"
)

// TransactionOutcome describes how a transaction ended.
type TransactionOutcome string

const (
	TransactionOutcomeCommit TransactionOutcome = "commit"
	TransactionOutcomeAbort  TransactionOutcome = "abort"
	TransactionOutcomeError  TransactionOutcome = "error"
)

// HookTransactionStart is called when a transaction attempt starts.
//
// The returned context is passed to subsequent transaction hooks and transaction
// processing.
type HookTransactionStart interface {
	OnTransactionStart(ctx context.Context, transactionType TransactionType) context.Context
}

// HookTransactionEnd is called when a transaction attempt ends.
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

// implementsAnyHook checks the incoming Hook for any Hook implementation.
func implementsAnyHook(h Hook) bool {
	switch h.(type) {
	case HookNewClient,
		HookClientClosed,
		HookNewGroupTransactSession,
		HookGroupTransactSessionClosed,
		HookProduceStart,
		HookProduceRecord,
		HookProduceEnd,
		HookProduceError,
		HookFetchError,
		HookOffsetCommit,
		HookHandleStart,
		HookHandleEnd,
		HookShareAck,
		HookShareAckFlush,
		HookTransactionStart,
		HookTransactionEnd:
		return true
	}

	return false
}
