import warnings
from typing import (
    AbstractSet,
    Any,
    Callable,
    Dict,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
    overload,
)

import dagster._check as check
from dagster.builtins import Nothing
from dagster.config import Field
from dagster.config.config_schema import UserConfigSchema
from dagster.core.decorator_utils import get_function_params, get_valid_name_permutations
from dagster.core.definitions.decorators.op_decorator import _Op
from dagster.core.definitions.events import AssetKey, CoercibleToAssetKeyPrefix
from dagster.core.definitions.input import In
from dagster.core.definitions.output import Out
from dagster.core.definitions.partition import PartitionsDefinition
from dagster.core.definitions.resource_definition import ResourceDefinition
from dagster.core.definitions.utils import DEFAULT_IO_MANAGER_KEY, NoValueSentinel
from dagster.core.errors import DagsterInvalidDefinitionError
from dagster.core.storage.io_manager import IOManagerDefinition
from dagster.core.types.dagster_type import DagsterType
from dagster.seven import funcsigs
from dagster.utils.backcompat import (
    ExperimentalWarning,
    canonicalize_backcompat_args,
    deprecation_warning,
)

from .asset_in import AssetIn
from .asset_out import AssetOut
from .assets import AssetsDefinition
from .partition_mapping import PartitionMapping


@overload
def asset(
    name: Callable[..., Any],
) -> AssetsDefinition:
    ...


@overload
def asset(
    name: Optional[str] = ...,
    namespace: Optional[Sequence[str]] = ...,
    key_prefix: Optional[CoercibleToAssetKeyPrefix] = None,
    ins: Optional[Mapping[str, AssetIn]] = ...,
    non_argument_deps: Optional[Union[Set[AssetKey], Set[str]]] = ...,
    metadata: Optional[Mapping[str, Any]] = ...,
    description: Optional[str] = ...,
    config_schema: Optional[UserConfigSchema] = None,
    required_resource_keys: Optional[Set[str]] = ...,
    resource_defs: Optional[Mapping[str, ResourceDefinition]] = ...,
    io_manager_def: Optional[IOManagerDefinition] = ...,
    io_manager_key: Optional[str] = ...,
    compute_kind: Optional[str] = ...,
    dagster_type: Optional[DagsterType] = ...,
    partitions_def: Optional[PartitionsDefinition] = ...,
    partition_mappings: Optional[Mapping[str, PartitionMapping]] = ...,
    op_tags: Optional[Dict[str, Any]] = ...,
    group_name: Optional[str] = ...,
) -> Callable[[Callable[..., Any]], AssetsDefinition]:
    ...


def asset(
    name: Optional[Union[Callable[..., Any], Optional[str]]] = None,
    namespace: Optional[Sequence[str]] = None,
    key_prefix: Optional[CoercibleToAssetKeyPrefix] = None,
    ins: Optional[Mapping[str, AssetIn]] = None,
    non_argument_deps: Optional[Union[Set[AssetKey], Set[str]]] = None,
    metadata: Optional[Mapping[str, Any]] = None,
    description: Optional[str] = None,
    config_schema: Optional[UserConfigSchema] = None,
    required_resource_keys: Optional[Set[str]] = None,
    resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
    io_manager_def: Optional[IOManagerDefinition] = None,
    io_manager_key: Optional[str] = None,
    compute_kind: Optional[str] = None,
    dagster_type: Optional[DagsterType] = None,
    partitions_def: Optional[PartitionsDefinition] = None,
    partition_mappings: Optional[Mapping[str, PartitionMapping]] = None,
    op_tags: Optional[Dict[str, Any]] = None,
    group_name: Optional[str] = None,
) -> Union[AssetsDefinition, Callable[[Callable[..., Any]], AssetsDefinition]]:
    """Create a definition for how to compute an asset.

    A software-defined asset is the combination of:
      1. An asset key, e.g. the name of a table.
      2. A function, which can be run to compute the contents of the asset.
      3. A set of upstream assets that are provided as inputs to the function when computing the asset.

    Unlike an op, whose dependencies are determined by the graph it lives inside, an asset knows
    about the upstream assets it depends on. The upstream assets are inferred from the arguments
    to the decorated function. The name of the argument designates the name of the upstream asset.

    Args:
        name (Optional[str]): The name of the asset.  If not provided, defaults to the name of the
            decorated function.
        namespace (Optional[Sequence[str]]): **Deprecated (use `key_prefix`)**. The namespace that
            the asset resides in.  The namespace + the name forms the asset key.
        key_prefix (Optional[Union[str, Sequence[str]]]): If provided, the asset's key is the
            concatenation of the key_prefix and the asset's name, which defaults to the name of
            the decorated function.
        ins (Optional[Mapping[str, AssetIn]]): A dictionary that maps input names to their metadata
            and namespaces.
        non_argument_deps (Optional[Union[Set[AssetKey], Set[str]]]): Set of asset keys that are
            upstream dependencies, but do not pass an input to the asset.
        config_schema (Optional[ConfigSchema): The configuration schema for the asset's underlying
            op. If set, Dagster will check that config provided for the op matches this schema and fail
            if it does not. If not set, Dagster will accept any config provided for the op.
        metadata (Optional[Dict[str, Any]]): A dict of metadata entries for the asset.
        required_resource_keys (Optional[Set[str]]): Set of resource handles required by the op.
        io_manager_key (Optional[str]): The resource key of the IOManager used
            for storing the output of the op as an asset, and for loading it in downstream ops
            (default: "io_manager"). Only one of io_manager_key and io_manager_def can be provided.
        io_manager_def (Optional[IOManagerDefinition]): The definition of the IOManager used for
            storing the output of the op as an asset,  and for loading it in
            downstream ops. Only one of io_manager_def and io_manager_key can be provided.
        compute_kind (Optional[str]): A string to represent the kind of computation that produces
            the asset, e.g. "dbt" or "spark". It will be displayed in Dagit as a badge on the asset.
        dagster_type (Optional[DagsterType]): Allows specifying type validation functions that
            will be executed on the output of the decorated function after it runs.
        partitions_def (Optional[PartitionsDefinition]): Defines the set of partition keys that
            compose the asset.
        partition_mappings (Optional[Mapping[str, PartitionMapping]]): Defines how to map partition
            keys for this asset to partition keys of upstream assets. Each key in the dictionary
            correponds to one of the input assets, and each value is a PartitionMapping.
            If no entry is provided for a particular asset dependency, the partition mapping defaults
            to the default partition mapping for the partitions definition, which is typically maps
            partition keys to the same partition keys in upstream assets.
        op_tags (Optional[Dict[str, Any]]): A dictionary of tags for the op that computes the asset.
            Frameworks may expect and require certain metadata to be attached to a op. Values that
            are not strings will be json encoded and must meet the criteria that
            `json.loads(json.dumps(value)) == value`.
        group_name (Optional[str]): A string name used to organize multiple assets into groups. If not provided,
            the name "default" is used.
        resource_defs (Optional[Mapping[str, ResourceDefinition]]):
            A mapping of resource keys to resource definitions. These resources
            will be initialized during execution, and can be accessed from the
            context within the body of the function.

    Examples:

        .. code-block:: python

            @asset
            def my_asset(my_upstream_asset: int) -> int:
                return my_upstream_asset + 1
    """
    if callable(name):
        return _Asset()(name)

    key_prefix = canonicalize_backcompat_args(
        key_prefix,
        "key_prefix",
        namespace,
        "namespace",
        "0.16.0",
        additional_warn_txt="key_prefix applies only to the output AssetKey. If you want to modify "
        "the prefix of the input AssetKeys as well, you can do this by explicitly setting the ins "
        "parameter of this asset to a dictionary of the form "
        "'input_name': AssetIn(key_prefix=...).",
    )

    def inner(fn: Callable[..., Any]) -> AssetsDefinition:
        check.invariant(
            not (io_manager_key and io_manager_def),
            "Both io_manager_key and io_manager_def were provided to `@asset` decorator. Please provide one or the other. ",
        )
        return _Asset(
            name=cast(Optional[str], name),  # (mypy bug that it can't infer name is Optional[str])
            key_prefix=key_prefix,
            namespace=namespace,
            ins=ins,
            non_argument_deps=_make_asset_keys(non_argument_deps),
            metadata=metadata,
            description=description,
            config_schema=config_schema,
            required_resource_keys=required_resource_keys,
            resource_defs=resource_defs,
            io_manager=io_manager_def or io_manager_key,
            compute_kind=check.opt_str_param(compute_kind, "compute_kind"),
            dagster_type=dagster_type,
            partitions_def=partitions_def,
            partition_mappings=partition_mappings,
            op_tags=op_tags,
            group_name=group_name,
        )(fn)

    return inner


class _Asset:
    def __init__(
        self,
        name: Optional[str] = None,
        namespace: Optional[Sequence[str]] = None,
        key_prefix: Optional[CoercibleToAssetKeyPrefix] = None,
        ins: Optional[Mapping[str, AssetIn]] = None,
        non_argument_deps: Optional[Set[AssetKey]] = None,
        metadata: Optional[Mapping[str, Any]] = None,
        description: Optional[str] = None,
        config_schema: Optional[UserConfigSchema] = None,
        required_resource_keys: Optional[Set[str]] = None,
        resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
        io_manager: Optional[Union[str, IOManagerDefinition]] = None,
        compute_kind: Optional[str] = None,
        dagster_type: Optional[DagsterType] = None,
        partitions_def: Optional[PartitionsDefinition] = None,
        partition_mappings: Optional[Mapping[str, PartitionMapping]] = None,
        op_tags: Optional[Dict[str, Any]] = None,
        group_name: Optional[str] = None,
    ):
        self.name = name

        self.namespace = namespace
        if isinstance(key_prefix, str):
            key_prefix = [key_prefix]
        self.key_prefix = key_prefix
        self.ins = ins or {}
        self.non_argument_deps = non_argument_deps
        self.metadata = metadata
        self.description = description
        self.required_resource_keys = check.opt_set_param(
            required_resource_keys, "required_resource_keys"
        )
        self.io_manager = io_manager
        self.config_schema = check.opt_dict_param(
            config_schema,
            "config_schema",
            additional_message="Only dicts are supported for asset config_schema.",
        )
        self.compute_kind = compute_kind
        self.dagster_type = dagster_type
        self.partitions_def = partitions_def
        self.partition_mappings = partition_mappings
        self.op_tags = op_tags
        self.resource_defs = dict(check.opt_mapping_param(resource_defs, "resource_defs"))
        self.group_name = group_name

    def __call__(self, fn: Callable) -> AssetsDefinition:
        asset_name = self.name or fn.__name__

        # for backcompat, we prefix input asset keys with the namespace
        asset_ins = build_asset_ins(fn, self.namespace, self.ins or {}, self.non_argument_deps)

        out_asset_key = AssetKey(list(filter(None, [*(self.key_prefix or []), asset_name])))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=ExperimentalWarning)

            required_resource_keys = set(self.required_resource_keys).union(
                set(self.resource_defs.keys())
            )

            if isinstance(self.io_manager, str):
                io_manager_key = cast(str, self.io_manager)
            elif self.io_manager is not None:
                io_manager_def = check.inst_param(
                    self.io_manager, "io_manager", IOManagerDefinition
                )
                out_asset_resource_key = "__".join(out_asset_key.path)
                io_manager_key = f"{out_asset_resource_key}__io_manager"
                self.resource_defs[io_manager_key] = cast(ResourceDefinition, io_manager_def)
            else:
                io_manager_key = DEFAULT_IO_MANAGER_KEY

            out = Out(
                metadata=self.metadata or {},
                io_manager_key=io_manager_key,
                dagster_type=self.dagster_type if self.dagster_type else NoValueSentinel,
                description=self.description,
            )

            op = _Op(
                name="__".join(out_asset_key.path).replace("-", "_"),
                description=self.description,
                ins=dict(asset_ins.values()),
                out=out,
                required_resource_keys=required_resource_keys,
                tags={
                    **({"kind": self.compute_kind} if self.compute_kind else {}),
                    **(self.op_tags or {}),
                },
                config_schema={
                    "assets": {
                        "input_partitions": Field(dict, is_required=False),
                        "output_partitions": Field(dict, is_required=False),
                    },
                    **self.config_schema,
                },
            )(fn)

        keys_by_input_name = {
            input_name: asset_key for asset_key, (input_name, _) in asset_ins.items()
        }
        return AssetsDefinition(
            keys_by_input_name=keys_by_input_name,
            keys_by_output_name={"result": out_asset_key},
            node_def=op,
            partitions_def=self.partitions_def,
            partition_mappings={
                keys_by_input_name[input_name]: partition_mapping
                for input_name, partition_mapping in self.partition_mappings.items()
            }
            if self.partition_mappings
            else None,
            resource_defs=self.resource_defs,
            group_names_by_key={out_asset_key: self.group_name} if self.group_name else None,
        )


def multi_asset(
    outs: Mapping[str, Union[Out, AssetOut]],
    name: Optional[str] = None,
    ins: Optional[Mapping[str, AssetIn]] = None,
    non_argument_deps: Optional[Union[Set[AssetKey], Set[str]]] = None,
    description: Optional[str] = None,
    config_schema: Optional[UserConfigSchema] = None,
    required_resource_keys: Optional[Set[str]] = None,
    compute_kind: Optional[str] = None,
    internal_asset_deps: Optional[Mapping[str, Set[AssetKey]]] = None,
    partitions_def: Optional[PartitionsDefinition] = None,
    partition_mappings: Optional[Mapping[str, PartitionMapping]] = None,
    op_tags: Optional[Dict[str, Any]] = None,
    can_subset: bool = False,
    resource_defs: Optional[Mapping[str, ResourceDefinition]] = None,
) -> Callable[[Callable[..., Any]], AssetsDefinition]:
    """Create a combined definition of multiple assets that are computed using the same op and same
    upstream assets.

    Each argument to the decorated function references an upstream asset that this asset depends on.
    The name of the argument designates the name of the upstream asset.

    Args:
        name (Optional[str]): The name of the op.
        outs: (Optional[Dict[str, Out]]): The Outs representing the produced assets.
        ins (Optional[Mapping[str, AssetIn]]): A dictionary that maps input names to their metadata
            and namespaces.
        non_argument_deps (Optional[Union[Set[AssetKey], Set[str]]]): Set of asset keys that are upstream
            dependencies, but do not pass an input to the multi_asset.
        config_schema (Optional[ConfigSchema): The configuration schema for the asset's underlying
            op. If set, Dagster will check that config provided for the op matches this schema and fail
            if it does not. If not set, Dagster will accept any config provided for the op.
        required_resource_keys (Optional[Set[str]]): Set of resource handles required by the underlying op.
        io_manager_key (Optional[str]): The resource key of the IOManager used for storing the
            output of the op as an asset, and for loading it in downstream ops
            (default: "io_manager").
        compute_kind (Optional[str]): A string to represent the kind of computation that produces
            the asset, e.g. "dbt" or "spark". It will be displayed in Dagit as a badge on the asset.
        internal_asset_deps (Optional[Mapping[str, Set[AssetKey]]]): By default, it is assumed
            that all assets produced by a multi_asset depend on all assets that are consumed by that
            multi asset. If this default is not correct, you pass in a map of output names to a
            corrected set of AssetKeys that they depend on. Any AssetKeys in this list must be either
            used as input to the asset or produced within the op.
        partitions_def (Optional[PartitionsDefinition]): Defines the set of partition keys that
            compose the assets.
        partition_mappings (Optional[Mapping[str, PartitionMapping]]): Defines how to map partition
            keys for this asset to partition keys of upstream assets. Each key in the dictionary
            correponds to one of the input assets, and each value is a PartitionMapping.
            If no entry is provided for a particular asset dependency, the partition mapping defaults
            to the default partition mapping for the partitions definition, which is typically maps
            partition keys to the same partition keys in upstream assets.
        op_tags (Optional[Dict[str, Any]]): A dictionary of tags for the op that computes the asset.
            Frameworks may expect and require certain metadata to be attached to a op. Values that
            are not strings will be json encoded and must meet the criteria that
            `json.loads(json.dumps(value)) == value`.
        can_subset (bool): If this asset's computation can emit a subset of the asset
            keys based on the context.selected_assets argument. Defaults to False.
        resource_defs (Optional[Mapping[str, ResourceDefinition]]):
            A mapping of resource keys to resource definitions. These resources
            will be initialized during execution, and can be accessed from the
            context within the body of the function.
    """
    asset_deps = check.opt_dict_param(
        internal_asset_deps, "internal_asset_deps", key_type=str, value_type=set
    )
    required_resource_keys = check.opt_set_param(
        required_resource_keys, "required_resource_keys", of_type=str
    )
    resource_defs = check.opt_mapping_param(
        resource_defs, "resource_defs", key_type=str, value_type=ResourceDefinition
    )
    config_schema = check.opt_dict_param(
        config_schema,
        "config_schema",
        additional_message="Only dicts are supported for asset config_schema.",
    )

    required_resource_keys = set(required_resource_keys).union(set(resource_defs.keys()))
    for out in outs.values():
        if isinstance(out, Out) and not isinstance(out, AssetOut):
            deprecation_warning(
                "Passing Out objects as values for the out argument of @multi_asset",
                "0.16.0",
                additional_warn_txt="Use AssetOut instead.",
            )

    def inner(fn: Callable[..., Any]) -> AssetsDefinition:

        op_name = name or fn.__name__
        asset_ins = build_asset_ins(
            fn, None, ins or {}, non_argument_deps=_make_asset_keys(non_argument_deps)
        )
        asset_outs = build_asset_outs(outs)

        # validate that the asset_deps make sense
        valid_asset_deps = set(asset_ins.keys()) | set(asset_outs.keys())
        for out_name, asset_keys in asset_deps.items():
            check.invariant(
                out_name in outs,
                f"Invalid out key '{out_name}' supplied to `internal_asset_deps` argument for multi-asset "
                f"{op_name}. Must be one of the outs for this multi-asset {list(outs.keys())}.",
            )
            invalid_asset_deps = asset_keys.difference(valid_asset_deps)
            check.invariant(
                not invalid_asset_deps,
                f"Invalid asset dependencies: {invalid_asset_deps} specified in `internal_asset_deps` "
                f"argument for multi-asset '{op_name}' on key '{out_name}'. Each specified asset key "
                "must be associated with an input to the asset or produced by this asset. Valid "
                f"keys: {valid_asset_deps}",
            )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=ExperimentalWarning)

            op = _Op(
                name=op_name,
                description=description,
                ins=dict(asset_ins.values()),
                out=dict(asset_outs.values()),
                required_resource_keys=required_resource_keys,
                tags={
                    **({"kind": compute_kind} if compute_kind else {}),
                    **(op_tags or {}),
                },
                config_schema={
                    "assets": {
                        "input_partitions": Field(dict, is_required=False),
                        "output_partitions": Field(dict, is_required=False),
                    },
                    # Mypy scoping bug causing incorrect type inference here
                    **config_schema,  # type: ignore
                },
            )(fn)

        keys_by_input_name = {
            input_name: asset_key for asset_key, (input_name, _) in asset_ins.items()
        }
        keys_by_output_name = {
            output_name: asset_key for asset_key, (output_name, _) in asset_outs.items()
        }
        return AssetsDefinition(
            keys_by_input_name=keys_by_input_name,
            keys_by_output_name=keys_by_output_name,
            node_def=op,
            asset_deps={keys_by_output_name[name]: asset_deps[name] for name in asset_deps},
            partitions_def=partitions_def,
            partition_mappings={
                keys_by_input_name[input_name]: partition_mapping
                for input_name, partition_mapping in partition_mappings.items()
            }
            if partition_mappings
            else None,
            can_subset=can_subset,
            resource_defs=resource_defs,
        )

    return inner


def build_asset_ins(
    fn: Callable,
    asset_key_prefix: Optional[Sequence[str]],
    asset_ins: Mapping[str, AssetIn],
    non_argument_deps: Optional[AbstractSet[AssetKey]],
) -> Mapping[AssetKey, Tuple[str, In]]:
    """
    Creates a mapping from AssetKey to (name of input, In object)
    """
    non_argument_deps = check.opt_set_param(non_argument_deps, "non_argument_deps", AssetKey)

    params = get_function_params(fn)
    is_context_provided = len(params) > 0 and params[0].name in get_valid_name_permutations(
        "context"
    )
    input_params = params[1:] if is_context_provided else params
    non_var_input_param_names = [
        param.name
        for param in input_params
        if param.kind == funcsigs.Parameter.POSITIONAL_OR_KEYWORD
    ]
    has_kwargs = any(param.kind == funcsigs.Parameter.VAR_KEYWORD for param in input_params)

    all_input_names = set(non_var_input_param_names) | asset_ins.keys()

    if not has_kwargs:
        for in_key in asset_ins.keys():
            if in_key not in non_var_input_param_names:
                raise DagsterInvalidDefinitionError(
                    f"Key '{in_key}' in provided ins dict does not correspond to any of the names "
                    "of the arguments to the decorated function"
                )

    ins_by_asset_key: Dict[AssetKey, Tuple[str, In]] = {}
    for input_name in all_input_names:
        asset_key = None

        if input_name in asset_ins:
            asset_key = asset_ins[input_name].key
            metadata = asset_ins[input_name].metadata or {}
            key_prefix = asset_ins[input_name].key_prefix
        else:
            metadata = {}
            key_prefix = None

        asset_key = asset_key or AssetKey(
            list(filter(None, [*(key_prefix or asset_key_prefix or []), input_name]))
        )

        ins_by_asset_key[asset_key] = (input_name.replace("-", "_"), In(metadata=metadata))

    for asset_key in non_argument_deps:
        stringified_asset_key = "_".join(asset_key.path).replace("-", "_")
        # mypy doesn't realize that Nothing is a valid type here
        ins_by_asset_key[asset_key] = (stringified_asset_key, In(cast(type, Nothing)))

    return ins_by_asset_key


def build_asset_outs(
    asset_outs: Mapping[str, Union[Out, AssetOut]]
) -> Mapping[AssetKey, Tuple[str, Out]]:
    """
    Creates a mapping from AssetKey to (name of output, Out object)
    """
    outs_by_asset_key: Dict[AssetKey, Tuple[str, Out]] = {}
    for output_name, asset_out in asset_outs.items():
        if isinstance(asset_out, AssetOut):
            out = asset_out.to_out()
            asset_key = asset_out.key or AssetKey(
                list(filter(None, [*(asset_out.key_prefix or []), output_name]))
            )
        elif isinstance(asset_out, Out):
            out = asset_out
            if isinstance(asset_out.asset_key, AssetKey):
                asset_key = asset_out.asset_key
            elif asset_out.asset_key is None:
                asset_key = AssetKey(output_name)
            else:
                check.failed("Expected AssetKey or None")
        else:
            check.failed("Expected Out or AssetOut")

        outs_by_asset_key[asset_key] = (output_name.replace("-", "_"), out)

    return outs_by_asset_key


def _make_asset_keys(deps: Optional[Union[Set[AssetKey], Set[str]]]) -> Optional[Set[AssetKey]]:
    """Convert all str items to AssetKey in the set."""
    if deps is None:
        return deps

    deps_asset_keys = {
        AssetKey.from_user_string(dep) if isinstance(dep, str) else dep for dep in deps
    }
    return deps_asset_keys
