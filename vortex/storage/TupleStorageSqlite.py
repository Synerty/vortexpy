import logging
import sqlite3
from collections import namedtuple
from datetime import datetime
from pathlib import Path
from typing import Optional
from typing import Union

import pytz
from twisted.internet import defer
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredSemaphore
from twisted.internet.defer import inlineCallbacks

from vortex.DeferUtil import deferToThreadWrapWithLogger
from vortex.Payload import Payload
from vortex.Tuple import Tuple
from vortex.TupleSelector import TupleSelector

# ------------------------------------------------------------------------------

CREATE_TABLE_SQL = """CREATE TABLE IF NOT EXISTS tuples
     (
        tupleSelector TEXT,
        dateTime REAL,
        payload TEXT,
        PRIMARY KEY (tupleSelector)
     )"""

DROP_TABLE_SQL = """DROP TABLE IF NOT EXISTS tuples"""

DELETE_BY_SELECTOR_SQL = """DELETE
                 FROM tuples
                 WHERE tupleSelector = ?"""

DELETE_BY_DATE_SQL = """DELETE
                 FROM tuples
                 WHERE dateTime < ?"""

INSERT_SQL = """INSERT OR REPLACE INTO tuples
                 (tupleSelector, dateTime, payload)
                 VALUES (?, ?, ?)"""

SELECT_SQL = """SELECT payload
                 FROM tuples
                 WHERE tupleSelector = ?"""

SELECT_ALL_PAYLOADS_SQL = """SELECT payload from tuples"""

# ------------------------------------------------------------------------------

TupleStorageBatchSaveArguments = namedtuple(
    "TupleStorageBatchSaveArguments", ["tupleSelector", "encodedPayload"]
)


# ------------------------------------------------------------------------------


class TupleStorageSqlite:
    TRANSACTIONS_BEFORE_VACUUM = 50

    def __init__(self, databaseDirectory: Path, databaseName: str):
        databaseDirectory = databaseDirectory.expanduser()
        self._databaseDirectory = databaseDirectory
        self._databaseName = databaseName.lower().replace(".sqlite", "")
        self._databasePath = (
            databaseDirectory / f"{databaseName.lower()}.sqlite"
        )

        self._logger = logging.getLogger(f"{__name__} DB={self._databaseName}")

        self._isDbOpen = False
        self._transactionSemaphore = DeferredSemaphore(tokens=1)

        self._transactionsSinceVacuum = 0

    @property
    def databasePath(self) -> Path:
        return self._databasePath

    def open(self) -> Deferred:
        return self._transactionSemaphore.run(
            deferToThreadWrapWithLogger(self._logger)(self._openBlocking)
        )

    def _openBlocking(self):
        self._closeBlocking()
        self._isDbOpen = False
        try:
            with sqlite3.connect(self._databasePath) as conn:
                conn.execute(CREATE_TABLE_SQL)
                conn.commit()
            self._isDbOpen = True
        except sqlite3.Error as e:
            self._isDbOpen = False
            raise

    def isOpen(self) -> Deferred:
        return defer.succeed(self._isDbOpen)

    def _isOpenBlocking(self) -> bool:
        return self._isDbOpen

    def close(self) -> Deferred:
        return self._transactionSemaphore.run(
            deferToThreadWrapWithLogger(self._logger)(self._closeBlocking)
        )

    def _closeBlocking(self):
        self._isDbOpen = False

    def truncateStorage(self) -> Deferred:
        return self._transactionSemaphore.run(
            deferToThreadWrapWithLogger(self._logger)(
                self._truncateStorageBlocking
            )
        )

    def _truncateStorageBlocking(self):
        self._closeBlocking()
        self._databasePath.unlink(missing_ok=True)
        self._openBlocking()

    def snapshotToPath(self, path: Path) -> Deferred:
        return self._transactionSemaphore.run(
            deferToThreadWrapWithLogger(self._logger)(
                self._snapshotToPathBlocking
            ),
            path=path,
        )

    def _snapshotToPathBlocking(self, path: Path):
        assert self._isOpenBlocking(), "Database is not open"
        path.unlink(missing_ok=True)
        self._db.execute("VACUUM main INTO '%s'" % path)

    def loadTuplesAndAggregateAllTuples(self, batchSize=250) -> Deferred:
        return self._transactionSemaphore.run(
            deferToThreadWrapWithLogger(self._logger)(
                self._loadTuplesAndAggregateAllTuplesBlocking
            ),
            batchSize=batchSize,
        )

    def _loadTuplesAndAggregateAllTuplesBlocking(
        self, batchSize: int
    ) -> list[Tuple]:
        startTime = datetime.now(pytz.UTC)

        tuples = []

        with sqlite3.connect(self._databasePath) as conn:
            # Perform the transaction
            cur = conn.cursor()
            cur.execute(SELECT_ALL_PAYLOADS_SQL)
            while True:
                fetchStartTime = datetime.now(pytz.UTC)
                rows = cur.fetchmany(batchSize)
                if not rows:
                    break
                newTuples = []
                for row in rows:
                    newTuples.extend(
                        Payload().fromEncodedPayload(row[0]).tuples
                    )

                tuples.extend(newTuples)

                self._logger.debug(
                    "PROGRESS loadTuplesAndAggregateAllTuples"
                    " took %s for %s rows, %s tuples, total tuples = %s",
                    datetime.now(pytz.UTC) - fetchStartTime,
                    len(rows),
                    len(newTuples),
                    len(tuples),
                )

        self._logger.debug(
            "FINISHED loadTuplesAndAggregateAllTuples took %s for %s tuples",
            datetime.now(pytz.UTC) - startTime,
            len(tuples),
        )

        return tuples

    @inlineCallbacks
    def loadTuples(self, tupleSelector: TupleSelector) -> Deferred:
        startTime = datetime.now(pytz.UTC)

        encodedPayload = yield self.loadTuplesEncoded(tupleSelector)
        if not encodedPayload:
            return []

        payload = yield Payload().fromEncodedPayloadDefer(encodedPayload)

        self._logger.debug(
            "loadTuples took %s for %s tuples for %s",
            datetime.now(pytz.UTC) - startTime,
            len(payload.tuples),
            tupleSelector,
        )

        return payload.tuples

    def loadTuplesEncoded(self, tupleSelector: TupleSelector) -> Deferred:
        return self._transactionSemaphore.run(
            deferToThreadWrapWithLogger(self._logger)(
                self._loadTuplesEncodedBlocking
            ),
            tupleSelector=tupleSelector,
        )

    def _loadTuplesEncodedBlocking(
        self, tupleSelector: Union[TupleSelector, str]
    ) -> Optional[bytes]:
        tupleSelectorStr = (
            tupleSelector
            if isinstance(tupleSelector, str)
            else tupleSelector.toJsonStr()
        )

        # Make sure the database is open
        assert self._isOpenBlocking(), "Database is not open"

        with sqlite3.connect(self._databasePath) as conn:
            # Perform the transaction
            cur = conn.cursor()
            cur.execute(SELECT_SQL, (tupleSelectorStr,))
            rows = cur.fetchall()
        return rows[0][0] if rows else None

    @inlineCallbacks
    def saveTuples(
        self, tupleSelector: TupleSelector, tuples: list[Tuple]
    ) -> Deferred:
        startTime = datetime.now(pytz.UTC)

        encodedPayload = yield Payload(tuples=tuples).toEncodedPayloadDefer()
        yield self.saveTuplesEncoded(tupleSelector, encodedPayload)

        self._logger.debug(
            "saveTuples took %s for %s tuples for %s",
            datetime.now(pytz.UTC) - startTime,
            len(tuples),
            tupleSelector,
        )

    @inlineCallbacks
    def saveTuplesEncoded(
        self, tupleSelector: Union[TupleSelector, str], encodedPayload: bytes
    ) -> Deferred:
        tupleSelectorStr = (
            tupleSelector
            if isinstance(tupleSelector, str)
            else tupleSelector.toJsonStr()
        )

        dateInt = round(datetime.now(pytz.UTC).timestamp() * 1000)
        params = [(tupleSelectorStr, dateInt, encodedPayload)]
        yield self._transaction(INSERT_SQL, params)

    @inlineCallbacks
    def batchSaveTuplesEncoded(
        self, data: list[TupleStorageBatchSaveArguments]
    ) -> Deferred:
        if not data:
            return

        dateInt = round(datetime.now(pytz.UTC).timestamp() * 1000)
        params = [
            (
                (
                    d.tupleSelector
                    if isinstance(d.tupleSelector, str)
                    else d.tupleSelector.toJsonStr()
                ),
                dateInt,
                d.encodedPayload,
            )
            for d in data
        ]
        yield self._transaction(INSERT_SQL, params)

    @inlineCallbacks
    def batchDeleteTuples(
        self, tupleSelectors: list[Union[TupleSelector, str]]
    ) -> Deferred:
        params = [
            (
                (
                    tupleSelector
                    if isinstance(tupleSelector, str)
                    else tupleSelector.toJsonStr()
                ),  # This comma creates the tuple required for parameters
            )
            for tupleSelector in tupleSelectors
        ]

        yield self._transaction(DELETE_BY_SELECTOR_SQL, params)

    @inlineCallbacks
    def deleteTuples(
        self, tupleSelector: Union[TupleSelector, str]
    ) -> Deferred:
        tupleSelectorStr = (
            tupleSelector
            if isinstance(tupleSelector, str)
            else tupleSelector.toJsonStr()
        )

        yield self._transaction(DELETE_BY_SELECTOR_SQL, [(tupleSelectorStr,)])

    @inlineCallbacks
    def deleteOldTuples(self, deleteDataBeforeDate: datetime) -> Deferred:
        dateInt = round(
            deleteDataBeforeDate.astimezone(pytz.UTC).timestamp() * 1000
        )

        yield self._transaction(DELETE_BY_DATE_SQL, [(dateInt,)])

    def _transaction(
        self, sql: str, params: Optional[list[tuple]] = None
    ) -> Deferred:
        return self._transactionSemaphore.run(
            deferToThreadWrapWithLogger(self._logger)(
                self._transactionBlocking
            ),
            sql=sql,
            params=params,
        )

    def _transactionBlocking(
        self, sql: str, params: Optional[list[tuple]] = None
    ):
        if not params:
            params = []

        # Make sure the input data is correct.
        assert isinstance(params, list), "Parameters should be a list of tuples"
        assert not len(params) or isinstance(
            params[0], tuple
        ), "Parameters should be a list of tuples"

        # Make sure the database is open
        assert self._isOpenBlocking(), "Database is not open"

        # Perform the transaction

        with sqlite3.connect(self._databasePath) as conn:
            cur = conn.cursor()
            cur.executemany(sql, params)
            conn.commit()

            self._transactionsSinceVacuum += 1

            if self._transactionsSinceVacuum > self.TRANSACTIONS_BEFORE_VACUUM:
                conn.execute("VACUUM")
                self._transactionsSinceVacuum = 0
