"""Infinity tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.helpers._classproperty import classproperty
from tap_infinity.streams import InfinityOneStream
from singer_sdk.helpers.capabilities import (
    CapabilitiesEnum,
    PluginCapabilities,
    TapCapabilities,
)


class TapInfinity(Tap):
    """Infinity tap class."""

    name = "tap-infinity"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "row_count",
            th.IntegerType,
            required=False,
            default=100_000,
            description="Number of rows to emit",
        ),
        th.Property(
            "column_count",
            th.IntegerType,
            required=False,
            default=30,
            description="Number of columns in each row",
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            required=False,
            default=1_000_000,
            description="Size of batch files",
        ),
        th.Property(
            "batch_config",
            th.ObjectType(
                th.Property(
                    "encoding",
                    th.ObjectType(
                        th.Property("format", th.StringType, required=True),
                        th.Property("compression", th.StringType, required=True),
                    ),
                    required=True,
                ),
                th.Property(
                    "storage",
                    th.ObjectType(
                        th.Property("root", th.StringType, required=True),
                        th.Property(
                            "prefix", th.StringType, required=False, default=""
                        ),
                    ),
                    required=True,
                ),
            ),
            required=False,
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [InfinityOneStream(tap=self)]

    @classproperty
    def capabilities(self) -> List[CapabilitiesEnum]:
        """Get tap capabilities.

        Returns:
            A list of capabilities supported by this tap.
        """
        return [
            TapCapabilities.CATALOG,
            TapCapabilities.STATE,
            TapCapabilities.DISCOVER,
            PluginCapabilities.ABOUT,
            PluginCapabilities.STREAM_MAPS,
            PluginCapabilities.FLATTENING,
            PluginCapabilities.BATCH,
        ]


if __name__ == "__main__":
    TapInfinity.cli()
