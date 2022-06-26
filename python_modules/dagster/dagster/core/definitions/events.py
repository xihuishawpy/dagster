import re
import warnings
from enum import Enum
from typing import (
    TYPE_CHECKING,
    AbstractSet,
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    TypeVar,
    Union,
    cast,
)

import dagster._check as check
import dagster.seven as seven
from dagster.serdes import DefaultNamedTupleSerializer, whitelist_for_serdes

from .metadata import (
    MetadataEntry,
    MetadataValue,
    PartitionMetadataEntry,
    RawMetadataValue,
    last_file_comp,
    normalize_metadata,
)
from .utils import DEFAULT_OUTPUT, check_valid_name

if TYPE_CHECKING:
    from dagster.core.execution.context.output import OutputContext

ASSET_KEY_SPLIT_REGEX = re.compile("[^a-zA-Z0-9_]")
ASSET_KEY_DELIMITER = "/"
ASSET_KEY_LEGACY_DELIMITER = "."


def parse_asset_key_string(s: str) -> List[str]:
    return list(filter(lambda x: x, re.split(ASSET_KEY_SPLIT_REGEX, s)))


@whitelist_for_serdes
class AssetKey(NamedTuple("_AssetKey", [("path", List[str])])):
    """Object representing the structure of an asset key.  Takes in a sanitized string, list of
    strings, or tuple of strings.

    Example usage:

    .. code-block:: python

        from dagster import op

        @op
        def emit_metadata(context, df):
            yield AssetMaterialization(
                asset_key=AssetKey('flat_asset_key'),
                metadata={"text_metadata": "Text-based metadata for this event"},
            )

        @op
        def structured_asset_key(context, df):
            yield AssetMaterialization(
                asset_key=AssetKey(['parent', 'child', 'grandchild']),
                metadata={"text_metadata": "Text-based metadata for this event"},
            )

        @op
        def structured_asset_key_2(context, df):
            yield AssetMaterialization(
                asset_key=AssetKey(('parent', 'child', 'grandchild')),
                metadata={"text_metadata": "Text-based metadata for this event"},
            )

    Args:
        path (Sequence[str]): String, list of strings, or tuple of strings.  A list of strings
            represent the hierarchical structure of the asset_key.
    """

    def __new__(cls, path: Sequence[str]):
        if isinstance(path, str):
            path = [path]
        else:
            path = list(check.sequence_param(path, "path", of_type=str))

        return super(AssetKey, cls).__new__(cls, path=path)

    def __str__(self):
        return f"AssetKey({self.path})"

    def __repr__(self):
        return f"AssetKey({self.path})"

    def __hash__(self):
        return hash(tuple(self.path))

    def __eq__(self, other):
        return (
            self.to_string() == other.to_string()
            if isinstance(other, AssetKey)
            else False
        )

    def to_string(self, legacy: Optional[bool] = False) -> Optional[str]:
        """
        E.g. '["first_component", "second_component"]'
        """
        if not self.path:
            return None
        if legacy:
            return ASSET_KEY_LEGACY_DELIMITER.join(self.path)
        return seven.json.dumps(self.path)

    def to_user_string(self) -> str:
        """
        E.g. "first_component/second_component"
        """
        return ASSET_KEY_DELIMITER.join(self.path)

    @staticmethod
    def from_user_string(asset_key_string: str) -> "AssetKey":
        return AssetKey(asset_key_string.split(ASSET_KEY_DELIMITER))

    @staticmethod
    def from_db_string(asset_key_string: Optional[str]) -> Optional["AssetKey"]:
        if not asset_key_string:
            return None
        if asset_key_string[0] == "[":
            # is a json string
            try:
                path = seven.json.loads(asset_key_string)
            except seven.JSONDecodeError:
                path = parse_asset_key_string(asset_key_string)
        else:
            path = parse_asset_key_string(asset_key_string)
        return AssetKey(path)

    @staticmethod
    def get_db_prefix(path: List[str], legacy: Optional[bool] = False):
        check.list_param(path, "path", of_type=str)
        if legacy:
            return ASSET_KEY_LEGACY_DELIMITER.join(path)
        return seven.json.dumps(path)[:-2]  # strip trailing '"]' from json string

    @staticmethod
    def from_graphql_input(asset_key: Mapping[str, List[str]]) -> Optional["AssetKey"]:
        if asset_key and asset_key.get("path"):
            return AssetKey(asset_key["path"])
        return None

    @staticmethod
    def from_coerceable(arg: "CoercibleToAssetKey") -> "AssetKey":
        if isinstance(arg, AssetKey):
            return check.inst_param(arg, "arg", AssetKey)
        elif isinstance(arg, str):
            return AssetKey([arg])
        elif isinstance(arg, list):
            check.list_param(arg, "arg", of_type=str)
            return AssetKey(arg)
        else:
            check.tuple_param(arg, "arg", of_type=str)
            return AssetKey(arg)


CoercibleToAssetKey = Union[AssetKey, str, Sequence[str]]
CoercibleToAssetKeyPrefix = Union[str, Sequence[str]]


DynamicAssetKey = Callable[["OutputContext"], Optional[AssetKey]]


@whitelist_for_serdes
class AssetLineageInfo(
    NamedTuple("_AssetLineageInfo", [("asset_key", AssetKey), ("partitions", AbstractSet[str])])
):
    def __new__(cls, asset_key, partitions=None):
        asset_key = check.inst_param(asset_key, "asset_key", AssetKey)
        partitions = check.opt_set_param(partitions, "partitions", str)
        return super(AssetLineageInfo, cls).__new__(cls, asset_key=asset_key, partitions=partitions)


T = TypeVar("T")


class Output(Generic[T]):
    """Event corresponding to one of a op's outputs.

    Op compute functions must explicitly yield events of this type when they have more than
    one output, or when they also yield events of other types, or when defining a op using the
    :py:class:`OpDefinition` API directly.

    Outputs are values produced by ops that will be consumed by downstream ops in a job.
    They are type-checked at op boundaries when their corresponding :py:class:`Out`
    or the downstream :py:class:`In` is typed.

    Args:
        value (Any): The value returned by the compute function.
        output_name (Optional[str]): Name of the corresponding out. (default:
            "result")
        metadata_entries (Optional[Union[MetadataEntry, PartitionMetadataEntry]]):
            (Experimental) A set of metadata entries to attach to events related to this Output.
        metadata (Optional[Dict[str, Union[str, float, int, Dict, MetadataValue]]]):
            Arbitrary metadata about the failure.  Keys are displayed string labels, and values are
            one of the following: string, float, int, JSON-serializable dict, JSON-serializable
            list, and one of the data classes returned by a MetadataValue static method.
    """

    def __init__(
        self,
        value: T,
        output_name: Optional[str] = DEFAULT_OUTPUT,
        metadata_entries: Optional[Sequence[Union[MetadataEntry, PartitionMetadataEntry]]] = None,
        metadata: Optional[Dict[str, RawMetadataValue]] = None,
    ):

        metadata = check.opt_dict_param(metadata, "metadata", key_type=str)
        metadata_entries = check.opt_list_param(
            metadata_entries,
            "metadata_entries",
            of_type=(MetadataEntry, PartitionMetadataEntry),
        )
        self._value = value
        self._output_name = check.str_param(output_name, "output_name")
        self._metadata_entries = normalize_metadata(metadata, metadata_entries)

    @property
    def metadata_entries(self) -> List[Union[PartitionMetadataEntry, MetadataEntry]]:
        return self._metadata_entries

    @property
    def value(self) -> Any:
        return self._value

    @property
    def output_name(self) -> str:
        return self._output_name

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, Output)
            and self.value == other.value
            and self.output_name == other.output_name
            and self.metadata_entries == other.metadata_entries
        )


class DynamicOutput(Generic[T]):
    """
    Variant of :py:class:`Output <dagster.Output>` used to support
    dynamic mapping & collect. Each ``DynamicOutput`` produced by an op represents
    one item in a set that can be processed individually with ``map`` or gathered
    with ``collect``.

    Each ``DynamicOutput`` must have a unique ``mapping_key`` to distinguish it with it's set.

    Args:
        value (Any):
            The value returned by the compute function.
        mapping_key (str):
            The key that uniquely identifies this dynamic value relative to its peers.
            This key will be used to identify the downstream ops when mapped, ie
            ``mapped_op[example_mapping_key]``
        output_name (Optional[str]):
            Name of the corresponding :py:class:`DynamicOut` defined on the op.
            (default: "result")
        metadata_entries (Optional[Union[MetadataEntry, PartitionMetadataEntry]]):
            (Experimental) A set of metadata entries to attach to events related to this output.
        metadata (Optional[Dict[str, Union[str, float, int, Dict, MetadataValue]]]):
            Arbitrary metadata about the failure.  Keys are displayed string labels, and values are
            one of the following: string, float, int, JSON-serializable dict, JSON-serializable
            list, and one of the data classes returned by a MetadataValue static method.
    """

    def __init__(
        self,
        value: T,
        mapping_key: str,
        output_name: Optional[str] = DEFAULT_OUTPUT,
        metadata_entries: Optional[List[Union[PartitionMetadataEntry, MetadataEntry]]] = None,
        metadata: Optional[Dict[str, RawMetadataValue]] = None,
    ):

        metadata = check.opt_dict_param(metadata, "metadata", key_type=str)
        metadata_entries = check.opt_list_param(
            metadata_entries, "metadata_entries", of_type=MetadataEntry
        )
        self._mapping_key = check_valid_name(check.str_param(mapping_key, "mapping_key"))
        self._output_name = check.str_param(output_name, "output_name")
        self._metadata_entries = normalize_metadata(metadata, metadata_entries)
        self._value = value

    @property
    def metadata_entries(self) -> List[Union[PartitionMetadataEntry, MetadataEntry]]:
        return self._metadata_entries

    @property
    def mapping_key(self) -> str:
        return self._mapping_key

    @property
    def value(self) -> T:
        return self._value

    @property
    def output_name(self) -> str:
        return self._output_name

    def __eq__(self, other: object) -> bool:
        return (
            isinstance(other, DynamicOutput)
            and self.value == other.value
            and self.output_name == other.output_name
            and self.mapping_key == other.mapping_key
            and self.metadata_entries == other.metadata_entries
        )


@whitelist_for_serdes
class AssetObservation(
    NamedTuple(
        "_AssetObservation",
        [
            ("asset_key", AssetKey),
            ("description", Optional[str]),
            ("metadata_entries", List[MetadataEntry]),
            ("partition", Optional[str]),
        ],
    )
):
    """Event that captures metadata about an asset at a point in time.

    Args:
        asset_key (Union[str, List[str], AssetKey]): A key to identify the asset.
        metadata_entries (Optional[List[MetadataEntry]]): Arbitrary metadata about the asset.
        partition (Optional[str]): The name of a partition of the asset that the metadata
            corresponds to.
        metadata (Optional[Dict[str, Union[str, float, int, Dict, MetadataValue]]]):
            Arbitrary metadata about the asset.  Keys are displayed string labels, and values are
            one of the following: string, float, int, JSON-serializable dict, JSON-serializable
            list, and one of the data classes returned by a MetadataValue static method.
    """

    def __new__(
        cls,
        asset_key: Union[List[str], AssetKey, str],
        description: Optional[str] = None,
        metadata_entries: Optional[List[MetadataEntry]] = None,
        partition: Optional[str] = None,
        metadata: Optional[Dict[str, RawMetadataValue]] = None,
    ):
        if isinstance(asset_key, AssetKey):
            check.inst_param(asset_key, "asset_key", AssetKey)
        elif isinstance(asset_key, str):
            asset_key = AssetKey(parse_asset_key_string(asset_key))
        elif isinstance(asset_key, list):
            check.list_param(asset_key, "asset_key", of_type=str)
            asset_key = AssetKey(asset_key)
        else:
            check.tuple_param(asset_key, "asset_key", of_type=str)
            asset_key = AssetKey(asset_key)

        metadata = check.opt_dict_param(metadata, "metadata", key_type=str)
        metadata_entries = check.opt_list_param(
            metadata_entries, "metadata_entries", of_type=MetadataEntry
        )

        return super(AssetObservation, cls).__new__(
            cls,
            asset_key=asset_key,
            description=check.opt_str_param(description, "description"),
            metadata_entries=cast(
                List[MetadataEntry], normalize_metadata(metadata, metadata_entries)
            ),
            partition=check.opt_str_param(partition, "partition"),
        )

    @property
    def label(self) -> str:
        return " ".join(self.asset_key.path)


@whitelist_for_serdes
class AssetMaterialization(
    NamedTuple(
        "_AssetMaterialization",
        [
            ("asset_key", AssetKey),
            ("description", Optional[str]),
            ("metadata_entries", List[Union[MetadataEntry, PartitionMetadataEntry]]),
            ("partition", Optional[str]),
        ],
    )
):
    """Event indicating that an op has materialized an asset.

    Op compute functions may yield events of this type whenever they wish to indicate to the
    Dagster framework (and the end user) that they have produced a materialized value as a
    side effect of computation. Unlike outputs, asset materializations can not be passed to other
    ops, and their persistence is controlled by op logic, rather than by the Dagster
    framework.

    Op authors should use these events to organize metadata about the side effects of their
    computations, enabling tooling like the Assets dashboard in Dagit.

    Args:
        asset_key (Union[str, List[str], AssetKey]): A key to identify the materialized asset across job
            runs
        description (Optional[str]): A longer human-readable description of the materialized value.
        metadata_entries (Optional[List[Union[MetadataEntry, PartitionMetadataEntry]]]): Arbitrary metadata about the
            materialized value.
        partition (Optional[str]): The name of the partition that was materialized.
        metadata (Optional[Dict[str, RawMetadataValue]]):
            Arbitrary metadata about the asset.  Keys are displayed string labels, and values are
            one of the following: string, float, int, JSON-serializable dict, JSON-serializable
            list, and one of the data classes returned by a MetadataValue static method.
    """

    def __new__(
        cls,
        asset_key: CoercibleToAssetKey,
        description: Optional[str] = None,
        metadata_entries: Optional[Sequence[Union[MetadataEntry, PartitionMetadataEntry]]] = None,
        partition: Optional[str] = None,
        metadata: Optional[Mapping[str, RawMetadataValue]] = None,
    ):
        if isinstance(asset_key, AssetKey):
            check.inst_param(asset_key, "asset_key", AssetKey)
        elif isinstance(asset_key, str):
            asset_key = AssetKey(parse_asset_key_string(asset_key))
        elif isinstance(asset_key, list):
            check.sequence_param(asset_key, "asset_key", of_type=str)
            asset_key = AssetKey(asset_key)
        else:
            check.tuple_param(asset_key, "asset_key", of_type=str)
            asset_key = AssetKey(asset_key)

        metadata = check.opt_mapping_param(metadata, "metadata", key_type=str)
        metadata_entries = check.opt_sequence_param(
            metadata_entries, "metadata_entries", of_type=(MetadataEntry, PartitionMetadataEntry)
        )

        return super(AssetMaterialization, cls).__new__(
            cls,
            asset_key=asset_key,
            description=check.opt_str_param(description, "description"),
            metadata_entries=normalize_metadata(metadata, metadata_entries),
            partition=check.opt_str_param(partition, "partition"),
        )

    @property
    def label(self) -> str:
        return " ".join(self.asset_key.path)

    @staticmethod
    def file(
        path: str,
        description: Optional[str] = None,
        asset_key: Optional[Union[str, List[str], AssetKey]] = None,
    ) -> "AssetMaterialization":
        """Static constructor for standard materializations corresponding to files on disk.

        Args:
            path (str): The path to the file.
            description (Optional[str]): A human-readable description of the materialization.
        """
        if not asset_key:
            asset_key = path

        return AssetMaterialization(
            asset_key=cast(Union[str, AssetKey, List[str]], asset_key),
            description=description,
            metadata_entries=[MetadataEntry("path", value=MetadataValue.path(path))],
        )


class MaterializationSerializer(DefaultNamedTupleSerializer):
    @classmethod
    def value_from_unpacked(cls, unpacked_dict, klass):
        # override the default `from_storage_dict` implementation in order to skip the deprecation
        # warning for historical Materialization events, loaded from event_log storage
        return Materialization(skip_deprecation_warning=True, **unpacked_dict)


@whitelist_for_serdes(serializer=MaterializationSerializer)
class Materialization(
    NamedTuple(
        "_Materialization",
        [
            ("label", str),
            ("description", Optional[str]),
            ("metadata_entries", List[MetadataEntry]),
            ("asset_key", AssetKey),
            ("partition", Optional[str]),
        ],
    )
):
    """Event indicating that an op has materialized a value.

    Solid compute functions may yield events of this type whenever they wish to indicate to the
    Dagster framework (and the end user) that they have produced a materialized value as a
    side effect of computation. Unlike outputs, materializations can not be passed to other ops,
    and their persistence is controlled by op logic, rather than by the Dagster framework.

    Solid authors should use these events to organize metadata about the side effects of their
    computations to enable downstream tooling like artifact catalogues and diff tools.

    Args:
        label (str): A short display name for the materialized value.
        description (Optional[str]): A longer human-radable description of the materialized value.
        metadata_entries (Optional[List[MetadataEntry]]): Arbitrary metadata about the
            materialized value.
        asset_key (Optional[Union[str, AssetKey]]): An optional parameter to identify the materialized asset
            across runs
        partition (Optional[str]): The name of the partition that was materialized.
    """

    def __new__(
        cls,
        label: Optional[str] = None,
        description: Optional[str] = None,
        metadata_entries: Optional[List[MetadataEntry]] = None,
        asset_key: Optional[Union[str, AssetKey]] = None,
        partition: Optional[str] = None,
        skip_deprecation_warning: Optional[bool] = False,
    ):
        if asset_key and isinstance(asset_key, str):
            asset_key = AssetKey(parse_asset_key_string(asset_key))
        else:
            check.opt_inst_param(asset_key, "asset_key", AssetKey)

        asset_key = cast(AssetKey, asset_key)
        if not label:
            check.param_invariant(
                asset_key and asset_key.path,
                "label",
                "Either label or asset_key with a path must be provided",
            )
            label = asset_key.to_string()

        if not skip_deprecation_warning:
            warnings.warn("`Materialization` is deprecated; use `AssetMaterialization` instead.")

        metadata_entries = check.opt_list_param(
            metadata_entries, "metadata_entries", of_type=MetadataEntry
        )

        return super(Materialization, cls).__new__(
            cls,
            label=check.str_param(label, "label"),
            description=check.opt_str_param(description, "description"),
            metadata_entries=check.opt_list_param(
                metadata_entries, "metadata_entries", of_type=MetadataEntry
            ),
            asset_key=asset_key,
            partition=check.opt_str_param(partition, "partition"),
        )

    @staticmethod
    def file(
        path: str,
        description: Optional[str] = None,
        asset_key: Optional[Union[str, AssetKey]] = None,
    ) -> "Materialization":
        """Static constructor for standard materializations corresponding to files on disk.

        Args:
            path (str): The path to the file.
            description (Optional[str]): A human-readable description of the materialization.
        """
        return Materialization(
            label=last_file_comp(path),
            description=description,
            metadata_entries=[MetadataEntry("path", value=MetadataValue.path(path))],
            asset_key=asset_key,
        )


@whitelist_for_serdes
class ExpectationResult(
    NamedTuple(
        "_ExpectationResult",
        [
            ("success", bool),
            ("label", Optional[str]),
            ("description", Optional[str]),
            ("metadata_entries", List[MetadataEntry]),
        ],
    )
):
    """Event corresponding to a data quality test.

    Op compute functions may yield events of this type whenever they wish to indicate to the
    Dagster framework (and the end user) that a data quality test has produced a (positive or
    negative) result.

    Args:
        success (bool): Whether the expectation passed or not.
        label (Optional[str]): Short display name for expectation. Defaults to "result".
        description (Optional[str]): A longer human-readable description of the expectation.
        metadata_entries (Optional[List[MetadataEntry]]): Arbitrary metadata about the
            expectation.
        metadata (Optional[Dict[str, RawMetadataValue]]):
            Arbitrary metadata about the failure.  Keys are displayed string labels, and values are
            one of the following: string, float, int, JSON-serializable dict, JSON-serializable
            list, and one of the data classes returned by a MetadataValue static method.
    """

    def __new__(
        cls,
        success: bool,
        label: Optional[str] = None,
        description: Optional[str] = None,
        metadata_entries: Optional[List[MetadataEntry]] = None,
        metadata: Optional[Dict[str, RawMetadataValue]] = None,
    ):
        metadata_entries = check.opt_list_param(
            metadata_entries, "metadata_entries", of_type=MetadataEntry
        )
        metadata = check.opt_dict_param(metadata, "metadata", key_type=str)

        return super(ExpectationResult, cls).__new__(
            cls,
            success=check.bool_param(success, "success"),
            label=check.opt_str_param(label, "label", "result"),
            description=check.opt_str_param(description, "description"),
            metadata_entries=cast(
                List[MetadataEntry], normalize_metadata(metadata, metadata_entries)
            ),
        )


@whitelist_for_serdes
class TypeCheck(
    NamedTuple(
        "_TypeCheck",
        [
            ("success", bool),
            ("description", Optional[str]),
            ("metadata_entries", List[MetadataEntry]),
        ],
    )
):
    """Event corresponding to a successful typecheck.

    Events of this type should be returned by user-defined type checks when they need to encapsulate
    additional metadata about a type check's success or failure. (i.e., when using
    :py:func:`as_dagster_type`, :py:func:`@usable_as_dagster_type <dagster_type>`, or the underlying
    :py:func:`PythonObjectDagsterType` API.)

    Solid compute functions should generally avoid yielding events of this type to avoid confusion.

    Args:
        success (bool): ``True`` if the type check succeeded, ``False`` otherwise.
        description (Optional[str]): A human-readable description of the type check.
        metadata_entries (Optional[List[MetadataEntry]]): Arbitrary metadata about the
            type check.
        metadata (Optional[Dict[str, RawMetadataValue]]):
            Arbitrary metadata about the failure.  Keys are displayed string labels, and values are
            one of the following: string, float, int, JSON-serializable dict, JSON-serializable
            list, and one of the data classes returned by a MetadataValue static method.
    """

    def __new__(
        cls,
        success: bool,
        description: Optional[str] = None,
        metadata_entries: Optional[List[MetadataEntry]] = None,
        metadata: Optional[Dict[str, RawMetadataValue]] = None,
    ):

        metadata_entries = check.opt_list_param(
            metadata_entries, "metadata_entries", of_type=MetadataEntry
        )
        metadata = check.opt_dict_param(metadata, "metadata", key_type=str)

        return super(TypeCheck, cls).__new__(
            cls,
            success=check.bool_param(success, "success"),
            description=check.opt_str_param(description, "description"),
            metadata_entries=cast(
                List[MetadataEntry], normalize_metadata(metadata, metadata_entries)
            ),
        )


class Failure(Exception):
    """Event indicating op failure.

    Raise events of this type from within op compute functions or custom type checks in order to
    indicate an unrecoverable failure in user code to the Dagster machinery and return
    structured metadata about the failure.

    Args:
        description (Optional[str]): A human-readable description of the failure.
        metadata_entries (Optional[List[MetadataEntry]]): Arbitrary metadata about the
            failure.
        metadata (Optional[Dict[str, RawMetadataValue]]):
            Arbitrary metadata about the failure.  Keys are displayed string labels, and values are
            one of the following: string, float, int, JSON-serializable dict, JSON-serializable
            list, and one of the data classes returned by a MetadataValue static method.
    """

    def __init__(
        self,
        description: Optional[str] = None,
        metadata_entries: Optional[List[MetadataEntry]] = None,
        metadata: Optional[Dict[str, RawMetadataValue]] = None,
    ):
        metadata_entries = check.opt_list_param(
            metadata_entries, "metadata_entries", of_type=MetadataEntry
        )
        metadata = check.opt_dict_param(metadata, "metadata", key_type=str)

        super(Failure, self).__init__(description)
        self.description = check.opt_str_param(description, "description")
        self.metadata_entries = normalize_metadata(metadata, metadata_entries)


class RetryRequested(Exception):
    """
    An exception to raise from an op to indicate that it should be retried.

    Args:
        max_retries (Optional[int]):
            The max number of retries this step should attempt before failing
        seconds_to_wait (Optional[Union[float,int]]):
            Seconds to wait before restarting the step after putting the step in
            to the up_for_retry state

    Example:

        .. code-block:: python

            @op
            def flakes():
                try:
                    flakey_operation()
                except Exception as e:
                    raise RetryRequested(max_retries=3) from e
    """

    def __init__(
        self, max_retries: Optional[int] = 1, seconds_to_wait: Optional[Union[float, int]] = None
    ):
        super(RetryRequested, self).__init__()
        self.max_retries = check.int_param(max_retries, "max_retries")
        self.seconds_to_wait = check.opt_numeric_param(seconds_to_wait, "seconds_to_wait")


class ObjectStoreOperationType(Enum):
    SET_OBJECT = "SET_OBJECT"
    GET_OBJECT = "GET_OBJECT"
    RM_OBJECT = "RM_OBJECT"
    CP_OBJECT = "CP_OBJECT"


class ObjectStoreOperation(
    NamedTuple(
        "_ObjectStoreOperation",
        [
            ("op", ObjectStoreOperationType),
            ("key", str),
            ("dest_key", Optional[str]),
            ("obj", Any),
            ("serialization_strategy_name", Optional[str]),
            ("object_store_name", Optional[str]),
            ("value_name", Optional[str]),
            ("version", Optional[str]),
            ("mapping_key", Optional[str]),
        ],
    )
):
    """This event is used internally by Dagster machinery when values are written to and read from
    an ObjectStore.

    Users should not import this class or yield events of this type from user code.

    Args:
        op (ObjectStoreOperationType): The type of the operation on the object store.
        key (str): The key of the object on which the operation was performed.
        dest_key (Optional[str]): The destination key, if any, to which the object was copied.
        obj (Any): The object, if any, retrieved by the operation.
        serialization_strategy_name (Optional[str]): The name of the serialization strategy, if any,
            employed by the operation
        object_store_name (Optional[str]): The name of the object store that performed the
            operation.
        value_name (Optional[str]): The name of the input/output
        version (Optional[str]): (Experimental) The version of the stored data.
        mapping_key (Optional[str]): The mapping key when a dynamic output is used.
    """

    def __new__(
        cls,
        op: ObjectStoreOperationType,
        key: str,
        dest_key: Optional[str] = None,
        obj: Any = None,
        serialization_strategy_name: Optional[str] = None,
        object_store_name: Optional[str] = None,
        value_name: Optional[str] = None,
        version: Optional[str] = None,
        mapping_key: Optional[str] = None,
    ):
        return super(ObjectStoreOperation, cls).__new__(
            cls,
            op=op,
            key=check.str_param(key, "key"),
            dest_key=check.opt_str_param(dest_key, "dest_key"),
            obj=obj,
            serialization_strategy_name=check.opt_str_param(
                serialization_strategy_name, "serialization_strategy_name"
            ),
            object_store_name=check.opt_str_param(object_store_name, "object_store_name"),
            value_name=check.opt_str_param(value_name, "value_name"),
            version=check.opt_str_param(version, "version"),
            mapping_key=check.opt_str_param(mapping_key, "mapping_key"),
        )

    @classmethod
    def serializable(cls, inst, **kwargs):
        return cls(
            **dict(
                {
                    "op": inst.op.value,
                    "key": inst.key,
                    "dest_key": inst.dest_key,
                    "obj": None,
                    "serialization_strategy_name": inst.serialization_strategy_name,
                    "object_store_name": inst.object_store_name,
                    "value_name": inst.value_name,
                    "version": inst.version,
                },
                **kwargs,
            )
        )


class HookExecutionResult(
    NamedTuple("_HookExecutionResult", [("hook_name", str), ("is_skipped", bool)])
):
    """This event is used internally to indicate the execution result of a hook, e.g. whether the
    user-defined hook function is skipped.

    Args:
        hook_name (str): The name of the hook.
        is_skipped (bool): ``False`` if the hook_fn is executed, ``True`` otheriwse.
    """

    def __new__(cls, hook_name: str, is_skipped: Optional[bool] = None):
        return super(HookExecutionResult, cls).__new__(
            cls,
            hook_name=check.str_param(hook_name, "hook_name"),
            is_skipped=cast(bool, check.opt_bool_param(is_skipped, "is_skipped", default=False)),
        )


UserEvent = Union[Materialization, AssetMaterialization, AssetObservation, ExpectationResult]
