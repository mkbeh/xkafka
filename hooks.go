package xkafka

import (
	"context"
	"time"
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
// xkafka to add more hooks in the future. Hooks must be safe for concurrent use.
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

// HookProduceError is called when producing a record fails.
type HookProduceError interface {
	OnProduceError(err error)
}

// HookFetchError is called when fetching records fails.
type HookFetchError interface {
	OnFetchError(ctx context.Context, err error)
}

// HookOffsetCommitError is called when committing consumer offsets fails.
type HookOffsetCommitError interface {
	OnOffsetCommitError(ctx context.Context, err error)
}

// HookHandleStart is called before records are passed to the configured handler.
//
// The returned context is passed to subsequent handler hooks and to the handler.
type HookHandleStart interface {
	OnHandleStart(ctx context.Context, recordCount int) context.Context
}

// HookHandleEnd is called after the configured handler returns.
type HookHandleEnd interface {
	OnHandleEnd(
		ctx context.Context,
		recordCount int,
		duration time.Duration,
		err error,
	)
}

// HookHandleRetry is called before retrying a failed handler invocation.
type HookHandleRetry interface {
	OnHandleRetry(ctx context.Context, retry int)
}

// HookHandleRetryExhausted is called when handler retries are exhausted.
type HookHandleRetryExhausted interface {
	OnHandleRetryExhausted(ctx context.Context, retries int, err error)
}

// ShareAckOutcome describes the outcome of a Share Group acknowledgement.
type ShareAckOutcome string

const (
	ShareAckAccept  ShareAckOutcome = "accept"
	ShareAckRelease ShareAckOutcome = "release"
	ShareAckReject  ShareAckOutcome = "reject"
)

// HookShareAck is called when Share Group records are acknowledged.
type HookShareAck interface {
	OnShareAck(
		ctx context.Context,
		outcome ShareAckOutcome,
		recordCount int,
	)
}

// HookShareAckError is called when flushing Share Group acknowledgements fails.
type HookShareAckError interface {
	OnShareAckError(ctx context.Context, err error)
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

// HookTransactionStart is called when a transaction starts.
//
// The returned context is passed to subsequent transaction hooks and transaction
// processing.
type HookTransactionStart interface {
	OnTransactionStart(
		ctx context.Context,
		transactionType TransactionType,
	) context.Context
}

// HookTransactionEnd is called when a transaction ends.
type HookTransactionEnd interface {
	OnTransactionEnd(
		ctx context.Context,
		transactionType TransactionType,
		outcome TransactionOutcome,
		duration time.Duration,
		err error,
	)
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

func (hs hooks) onProduceError(err error) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookProduceError); ok {
			h.OnProduceError(err)
		}
	})
}

func (hs hooks) onFetchError(ctx context.Context, err error) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookFetchError); ok {
			h.OnFetchError(ctx, err)
		}
	})
}

func (hs hooks) onOffsetCommitError(ctx context.Context, err error) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookOffsetCommitError); ok {
			h.OnOffsetCommitError(ctx, err)
		}
	})
}

func (hs hooks) onHandleStart(ctx context.Context, recordCount int) context.Context {
	hs.each(func(h Hook) {
		if h, ok := h.(HookHandleStart); ok {
			ctx = h.OnHandleStart(ctx, recordCount)
		}
	})

	return ctx
}

func (hs hooks) onHandleEnd(
	ctx context.Context,
	recordCount int,
	duration time.Duration,
	err error,
) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookHandleEnd); ok {
			h.OnHandleEnd(ctx, recordCount, duration, err)
		}
	})
}

func (hs hooks) onHandleRetry(ctx context.Context, retry int) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookHandleRetry); ok {
			h.OnHandleRetry(ctx, retry)
		}
	})
}

func (hs hooks) onHandleRetryExhausted(ctx context.Context, retries int, err error) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookHandleRetryExhausted); ok {
			h.OnHandleRetryExhausted(ctx, retries, err)
		}
	})
}

func (hs hooks) onShareAck(
	ctx context.Context,
	outcome ShareAckOutcome,
	recordCount int,
) {
	if recordCount == 0 {
		return
	}

	hs.each(func(h Hook) {
		if h, ok := h.(HookShareAck); ok {
			h.OnShareAck(ctx, outcome, recordCount)
		}
	})
}

func (hs hooks) onShareAckError(ctx context.Context, err error) {
	hs.each(func(h Hook) {
		if h, ok := h.(HookShareAckError); ok {
			h.OnShareAckError(ctx, err)
		}
	})
}

func (hs hooks) onTransactionStart(
	ctx context.Context,
	transactionType TransactionType,
) context.Context {
	hs.each(func(h Hook) {
		if h, ok := h.(HookTransactionStart); ok {
			ctx = h.OnTransactionStart(ctx, transactionType)
		}
	})

	return ctx
}

func (hs hooks) onTransactionEnd(
	ctx context.Context,
	transactionType TransactionType,
	outcome TransactionOutcome,
	duration time.Duration,
	err error,
) {
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
		HookProduceError,
		HookFetchError,
		HookOffsetCommitError,
		HookHandleStart,
		HookHandleEnd,
		HookHandleRetry,
		HookHandleRetryExhausted,
		HookShareAck,
		HookShareAckError,
		HookTransactionStart,
		HookTransactionEnd:
		return true
	}

	return false
}
