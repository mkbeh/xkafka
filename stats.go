package xkafka

import (
	"sync/atomic"
	"time"
)

// Stats is a detached snapshot of Kafka client statistics.
//
// Concurrent client activity may continue while Stats is being collected, so
// individual fields are not guaranteed to represent one globally atomic instant.
type Stats struct {
	// ProduceErrorCount is the cumulative number of records that failed to produce.
	ProduceErrorCount int64

	// FetchErrorCount is the cumulative number of Kafka fetch errors.
	FetchErrorCount int64

	// HandleCount is the cumulative number of handler invocations, including retries.
	HandleCount int64

	// HandleRecordCount is the cumulative number of records passed to handlers.
	HandleRecordCount int64

	// HandleErrorCount is the cumulative number of handler invocations that returned
	// an error or panicked.
	HandleErrorCount int64

	// HandleDuration is the cumulative time spent executing handlers.
	HandleDuration time.Duration

	// OffsetCommitErrorCount is the cumulative number of failed consumer offset commit attempts.
	OffsetCommitErrorCount int64

	// ShareAckErrorCount is the cumulative number of failed Share Group acknowledgment flush attempts.
	ShareAckErrorCount int64

	// TransactionCommitCount is the cumulative number of successfully committed RunInTx transactions.
	TransactionCommitCount int64

	// TransactionAbortCount is the cumulative number of cleanly aborted RunInTx transactions.
	TransactionAbortCount int64

	// TransactionErrorCount is the cumulative number of RunInTx calls that could not complete cleanly.
	TransactionErrorCount int64

	// TransactionDuration is the cumulative duration of RunInTx calls.
	TransactionDuration time.Duration

	// GroupTransactionCommitCount is the cumulative number of committed group transactions.
	GroupTransactionCommitCount int64

	// GroupTransactionAbortCount is the cumulative number of cleanly aborted group transactions.
	GroupTransactionAbortCount int64

	// GroupTransactionErrorCount is the cumulative number of failed group transaction operations.
	GroupTransactionErrorCount int64

	// GroupTransactionDuration is the cumulative duration of group transactions.
	GroupTransactionDuration time.Duration
}

type transactionOutcome uint8

const (
	transactionOutcomeError transactionOutcome = iota
	transactionOutcomeCommit
	transactionOutcomeAbort
)

type statsCollector struct {
	produceErrorCount atomic.Int64
	fetchErrorCount   atomic.Int64

	handleCount         atomic.Int64
	handleRecordCount   atomic.Int64
	handleErrorCount    atomic.Int64
	handleDurationNanos atomic.Int64

	offsetCommitErrorCount atomic.Int64
	shareAckErrorCount     atomic.Int64

	transactionCommitCount   atomic.Int64
	transactionAbortCount    atomic.Int64
	transactionErrorCount    atomic.Int64
	transactionDurationNanos atomic.Int64

	groupTransactionCommitCount   atomic.Int64
	groupTransactionAbortCount    atomic.Int64
	groupTransactionErrorCount    atomic.Int64
	groupTransactionDurationNanos atomic.Int64
}

func (s *statsCollector) snapshot() Stats {
	return Stats{
		ProduceErrorCount: s.produceErrorCount.Load(),
		FetchErrorCount:   s.fetchErrorCount.Load(),

		HandleCount:       s.handleCount.Load(),
		HandleRecordCount: s.handleRecordCount.Load(),
		HandleErrorCount:  s.handleErrorCount.Load(),
		HandleDuration:    time.Duration(s.handleDurationNanos.Load()),

		OffsetCommitErrorCount: s.offsetCommitErrorCount.Load(),
		ShareAckErrorCount:     s.shareAckErrorCount.Load(),

		TransactionCommitCount: s.transactionCommitCount.Load(),
		TransactionAbortCount:  s.transactionAbortCount.Load(),
		TransactionErrorCount:  s.transactionErrorCount.Load(),
		TransactionDuration:    time.Duration(s.transactionDurationNanos.Load()),

		GroupTransactionCommitCount: s.groupTransactionCommitCount.Load(),
		GroupTransactionAbortCount:  s.groupTransactionAbortCount.Load(),
		GroupTransactionErrorCount:  s.groupTransactionErrorCount.Load(),
		GroupTransactionDuration:    time.Duration(s.groupTransactionDurationNanos.Load()),
	}
}

func (s *statsCollector) recordProduceError() {
	s.produceErrorCount.Add(1)
}

func (s *statsCollector) recordFetchError() {
	s.fetchErrorCount.Add(1)
}

func (s *statsCollector) recordHandle(recordCount int, duration time.Duration, err error) {
	s.handleCount.Add(1)
	s.handleRecordCount.Add(int64(recordCount))
	s.handleDurationNanos.Add(duration.Nanoseconds())

	if err != nil {
		s.handleErrorCount.Add(1)
	}
}

func (s *statsCollector) recordOffsetCommitError() {
	s.offsetCommitErrorCount.Add(1)
}

func (s *statsCollector) recordShareAckError() {
	s.shareAckErrorCount.Add(1)
}

func (s *statsCollector) recordTransaction(outcome transactionOutcome, duration time.Duration) {
	s.transactionDurationNanos.Add(duration.Nanoseconds())

	switch outcome {
	case transactionOutcomeCommit:
		s.transactionCommitCount.Add(1)
	case transactionOutcomeAbort:
		s.transactionAbortCount.Add(1)
	default:
		s.transactionErrorCount.Add(1)
	}
}

func (s *statsCollector) recordGroupTransaction(committed bool, err error, duration time.Duration) {
	s.groupTransactionDurationNanos.Add(duration.Nanoseconds())

	switch {
	case err != nil:
		s.groupTransactionErrorCount.Add(1)
	case committed:
		s.groupTransactionCommitCount.Add(1)
	default:
		s.groupTransactionAbortCount.Add(1)
	}
}
