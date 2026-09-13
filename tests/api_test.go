package xkafka_test

import "github.com/mkbeh/xkafka"

type (
	Client               = xkafka.Client
	GroupTransactSession = xkafka.GroupTransactSession
	Opt                  = xkafka.Opt
	PromiseFunc          = xkafka.PromiseFunc
	ShareAckOutcome      = xkafka.ShareAckOutcome
	TransactionOutcome   = xkafka.TransactionOutcome
	TransactionType      = xkafka.TransactionType
	Tx                   = xkafka.Tx
)

const (
	ShareAckAccept  = xkafka.ShareAckAccept
	ShareAckRelease = xkafka.ShareAckRelease
	ShareAckReject  = xkafka.ShareAckReject

	TransactionOutcomeCommit = xkafka.TransactionOutcomeCommit
	TransactionOutcomeAbort  = xkafka.TransactionOutcomeAbort
	TransactionOutcomeError  = xkafka.TransactionOutcomeError

	TransactionTypeProducer = xkafka.TransactionTypeProducer
	TransactionTypeGroup    = xkafka.TransactionTypeGroup
)

var (
	NewClient               = xkafka.NewClient
	NewGroupTransactSession = xkafka.NewGroupTransactSession

	WithBatchHandler                     = xkafka.WithBatchHandler
	WithGroupTransactSessionBatchHandler = xkafka.WithGroupTransactSessionBatchHandler
	WithHooks                            = xkafka.WithHooks
	WithKafkaOptions                     = xkafka.WithKafkaOptions
	WithMaxPollRecords                   = xkafka.WithMaxPollRecords
	WithMaxRetries                       = xkafka.WithMaxRetries
	WithPollInterval                     = xkafka.WithPollInterval
	WithProducePromise                   = xkafka.WithProducePromise
	WithShareRejectAfterDeliveries       = xkafka.WithShareRejectAfterDeliveries
	WithShareReleaseTimeout              = xkafka.WithShareReleaseTimeout
	WithSuspendCommittingTimeout         = xkafka.WithSuspendCommittingTimeout
	WithSuspendProcessingTimeout         = xkafka.WithSuspendProcessingTimeout
)
