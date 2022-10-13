"""Stream type classes for tap-infinity."""

from datetime import datetime
import gzip
import json
import random
from typing import IO, Any, Iterable, Optional
from uuid import uuid4
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.streams import Stream
from singer_sdk.helpers._batch import BaseBatchFileEncoding, BatchConfig


def property_name(column_index: int) -> th.Property:
    if column_index == 0:
        return "id"
    elif column_index == 1:
        return "rep_key"

    mod_value = column_index % 4
    if mod_value == 0:
        return f"Col_{column_index}_int"
    elif mod_value == 1:
        return f"Col_{column_index}_float"
    elif mod_value == 2:
        return f"Col_{column_index}_string"
    else:
        return f"Col_{column_index}_datetime"


def property_type(column_index: int) -> th.Property:
    if column_index == 0:
        return th.Property(property_name(column_index), th.IntegerType)
    elif column_index == 1:
        return th.Property(property_name(column_index), th.IntegerType)

    mod_value = column_index % 4
    if mod_value == 0:
        return th.Property(property_name(column_index), th.IntegerType)
    elif mod_value == 1:
        return th.Property(property_name(column_index), th.NumberType)
    elif mod_value == 2:
        return th.Property(property_name(column_index), th.StringType)
    else:
        return th.Property(property_name(column_index), th.DateTimeType)


def property_value(row_index: int, column_index: int) -> Any:
    if column_index == 0:
        # ID
        return row_index
    elif column_index == 1:
        # rep_key
        return row_index

    mod_value = column_index % 4
    if mod_value == 0:
        return random.randint(0, 2_000_000_000)
    elif mod_value == 1:
        return random.random()
    elif mod_value == 2:
        return f"{uuid4()}"
    else:
        return datetime.now()


class InfinityOneStream(Stream):
    """Define custom stream."""

    name = "infinity_one"
    primary_keys = ["id"]
    replication_key = "rep_key"
    _force_batch_message = False

    @property
    def schema(self) -> dict:
        column_count = max(2, self.config.get("column_count", 30))

        props = [property_type(col_index) for col_index in range(column_count)]

        return th.PropertiesList(*props).to_dict()

    @property
    def batch_size(self) -> int:
        return self.config.get("batch_size", 1_000_000)

    @property
    def is_sorted(self) -> bool:
        return True

    @property
    def is_timestamp_replication_key(self) -> bool:
        return False

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of record-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        row_count = self.config.get("row_count", 100_000)
        column_count = self.config.get("column_count", 30)

        for row_index in range(row_count):
            yield {
                property_name(column_index=column_index): property_value(
                    row_index=row_index, column_index=column_index
                )
                for column_index in range(column_count)
            }

    def get_batches(
        self,
        batch_config: BatchConfig,
        context: Optional[dict] = None,
    ) -> Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        """Batch generator function.

        Developers are encouraged to override this method to customize batching
        behavior for databases, bulk APIs, etc.

        Args:
            batch_config: Batch config for this stream.
            context: Stream partition or context dictionary.

        Yields:
            A tuple of (encoding, manifest) for each batch.
        """
        sync_id = f"{self.tap_name}--{self.name}-{uuid4()}"
        prefix = batch_config.storage.prefix or ""

        i = 1
        chunk_size = 0
        filename: Optional[str] = None
        f: Optional[IO] = None
        gz: Optional[gzip.GzipFile] = None

        with batch_config.storage.fs() as fs:
            for record in self._sync_records(context, write_messages=False):
                # Why do this first? Because get_records can change the batch size
                # but that won't be visible until the NEXT record
                if self._force_batch_message or chunk_size >= self.batch_size:
                    gz.close()
                    gz = None
                    f.close()
                    f = None
                    file_url = fs.geturl(filename)
                    yield batch_config.encoding, [file_url]

                    filename = None

                    i += 1
                    chunk_size = 0

                    # Reset force flag
                    self._force_batch_message = False

                if filename is None:
                    filename = f"{prefix}{sync_id}-{i}.json.gz"
                    f = fs.open(filename, "wb")
                    gz = gzip.GzipFile(fileobj=f, mode="wb")

                gz.write((json.dumps(record) + "\n").encode())
                chunk_size += 1

            if chunk_size > 0:
                gz.close()
                f.close()
                file_url = fs.geturl(filename)
                yield batch_config.encoding, [file_url]
