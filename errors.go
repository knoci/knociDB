package knocidb

import "errors"

var (
	ErrKeyIsEmpty               = errors.New("the key is empty")
	ErrKeyNotFound              = errors.New("key not found in database")
	ErrDatabaseIsUsing          = errors.New("the database directory is used by another process")
	ErrReadOnlyBatch            = errors.New("the batch is read only")
	ErrBatchCommitted           = errors.New("the batch is committed")
	ErrDBClosed                 = errors.New("the database is closed")
	ErrDBDirectoryISEmpty       = errors.New("the database directory path can not be empty")
	ErrWaitMemtableSpaceTimeOut = errors.New("wait memtable space timeout, try again later")
	ErrUnsupportedIndex         = errors.New("unsupported index type")
	ErrCrossShardBatch          = errors.New("batch spans multiple shards")
	ErrCrossShardScan           = errors.New("cross-shard scan is not supported")
)
