from typing import Dict, List, Mapping, Optional, Set, TypeVar, cast

import dagster._check as check
from dagster.utils import ensure_single_item, frozendict

from .config_type import ConfigScalarKind, ConfigType, ConfigTypeKind
from .errors import (
    EvaluationError,
    create_array_error,
    create_dict_type_mismatch_error,
    create_enum_type_mismatch_error,
    create_enum_value_missing_error,
    create_field_not_defined_error,
    create_field_substitution_collision_error,
    create_fields_not_defined_error,
    create_map_error,
    create_missing_required_field_error,
    create_missing_required_fields_error,
    create_none_not_allowed_error,
    create_scalar_error,
    create_selector_multiple_fields_error,
    create_selector_multiple_fields_no_field_selected_error,
    create_selector_type_error,
    create_selector_unspecified_value_error,
)
from .evaluate_value_result import EvaluateValueResult
from .field import resolve_to_config_type
from .iterate_types import config_schema_snapshot_from_config_type
from .post_process import post_process_config
from .snap import ConfigFieldSnap, ConfigSchemaSnapshot, ConfigTypeSnap
from .stack import EvaluationStack
from .traversal_context import ValidationContext

VALID_FLOAT_TYPES = int, float

T = TypeVar("T")


def is_config_scalar_valid(config_type_snap: ConfigTypeSnap, config_value: object) -> bool:
    check.inst_param(config_type_snap, "config_type_snap", ConfigTypeSnap)
    check.param_invariant(config_type_snap.kind == ConfigTypeKind.SCALAR, "config_type_snap")
    if config_type_snap.scalar_kind == ConfigScalarKind.INT:
        return not isinstance(config_value, bool) and isinstance(config_value, int)
    elif config_type_snap.scalar_kind == ConfigScalarKind.STRING:
        return isinstance(config_value, str)
    elif config_type_snap.scalar_kind == ConfigScalarKind.BOOL:
        return isinstance(config_value, bool)
    elif config_type_snap.scalar_kind == ConfigScalarKind.FLOAT:
        return isinstance(config_value, VALID_FLOAT_TYPES)
    elif config_type_snap.scalar_kind is None:
        # historical snapshot without scalar kind. do no validation
        return True
    else:
        check.failed(f"Not a supported scalar {config_type_snap}")


def validate_config(config_schema: object, config_value: object) -> EvaluateValueResult:

    config_type = resolve_to_config_type(config_schema)
    config_type = check.inst(cast(ConfigType, config_type), ConfigType)

    config_schema_snapshot = config_schema_snapshot_from_config_type(config_type)

    return validate_config_from_snap(
        config_schema_snapshot=config_schema_snapshot,
        config_type_key=config_type.key,
        config_value=config_value,
    )


def validate_config_from_snap(
    config_schema_snapshot: ConfigSchemaSnapshot, config_type_key: str, config_value: T
) -> EvaluateValueResult[T]:
    check.inst_param(config_schema_snapshot, "config_schema_snapshot", ConfigSchemaSnapshot)
    check.str_param(config_type_key, "config_type_key")
    return _validate_config(
        ValidationContext(
            config_schema_snapshot=config_schema_snapshot,
            config_type_snap=config_schema_snapshot.get_config_snap(config_type_key),
            stack=EvaluationStack(entries=[]),
        ),
        config_value,
    )


def _validate_config(context: ValidationContext, config_value: object) -> EvaluateValueResult:
    check.inst_param(context, "context", ValidationContext)

    kind = context.config_type_snap.kind

    if kind == ConfigTypeKind.NONEABLE:
        return (
            EvaluateValueResult.for_value(config_value)
            if config_value is None
            else _validate_config(context.for_nullable_inner_type(), config_value)
        )

    if kind == ConfigTypeKind.ANY:
        return EvaluateValueResult.for_value(config_value)  # yolo

    if config_value is None:
        return EvaluateValueResult.for_error(create_none_not_allowed_error(context))

    if kind == ConfigTypeKind.SCALAR:
        return (
            EvaluateValueResult.for_value(config_value)
            if is_config_scalar_valid(context.config_type_snap, config_value)
            else EvaluateValueResult.for_error(
                create_scalar_error(context, config_value)
            )
        )

    elif kind == ConfigTypeKind.SELECTOR:
        return validate_selector_config(context, config_value)
    elif kind == ConfigTypeKind.STRICT_SHAPE:
        return validate_shape_config(context, config_value)
    elif kind == ConfigTypeKind.PERMISSIVE_SHAPE:
        return validate_permissive_shape_config(context, config_value)
    elif kind == ConfigTypeKind.MAP:
        return validate_map_config(context, config_value)
    elif kind == ConfigTypeKind.ARRAY:
        return validate_array_config(context, config_value)
    elif kind == ConfigTypeKind.ENUM:
        return validate_enum_config(context, config_value)
    elif kind == ConfigTypeKind.SCALAR_UNION:
        return _validate_scalar_union_config(context, config_value)
    else:
        check.failed(f"Unsupported ConfigTypeKind {kind}")


def _validate_scalar_union_config(
    context: ValidationContext, config_value: T
) -> EvaluateValueResult[T]:
    check.inst_param(context, "context", ValidationContext)
    check.param_invariant(context.config_type_snap.kind == ConfigTypeKind.SCALAR_UNION, "context")
    check.not_none_param(config_value, "config_value")

    if isinstance(config_value, (dict, list)):
        return _validate_config(
            context.for_new_config_type_key(context.config_type_snap.non_scalar_type_key),
            cast(T, config_value),
        )
    else:
        return _validate_config(
            context.for_new_config_type_key(context.config_type_snap.scalar_type_key),
            config_value,
        )


def _validate_empty_selector_config(context: ValidationContext) -> EvaluateValueResult[Dict]:
    fields = check.not_none(context.config_type_snap.fields)
    if len(fields) > 1:
        return EvaluateValueResult.for_error(
            create_selector_multiple_fields_no_field_selected_error(context)
        )

    defined_field_snap = fields[0]

    if defined_field_snap.is_required:
        return EvaluateValueResult.for_error(create_selector_unspecified_value_error(context))

    return EvaluateValueResult.for_value({})


def validate_selector_config(
    context: ValidationContext, config_value: object
) -> EvaluateValueResult[Dict[str, object]]:
    check.inst_param(context, "context", ValidationContext)
    check.param_invariant(context.config_type_snap.kind == ConfigTypeKind.SELECTOR, "selector_type")
    check.not_none_param(config_value, "config_value")

    # Special case the empty dictionary, meaning no values provided for the
    # value of the selector. # E.g. {'logging': {}}
    # If there is a single field defined on the selector and if it is optional
    # it passes validation. (e.g. a single logger "console")
    if config_value == {}:
        return _validate_empty_selector_config(context)  # type: ignore

    # Now we ensure that the used-provided config has only a a single entry
    # and then continue the validation pass

    if not isinstance(config_value, dict):
        return EvaluateValueResult.for_error(create_selector_type_error(context, config_value))

    if len(config_value) > 1:
        return EvaluateValueResult.for_error(
            create_selector_multiple_fields_error(context, config_value)
        )

    field_name, field_value = ensure_single_item(config_value)

    if not context.config_type_snap.has_field(field_name):
        return EvaluateValueResult.for_error(create_field_not_defined_error(context, field_name))

    field_snap = context.config_type_snap.get_field(field_name)

    child_evaluate_value_result = _validate_config(
        context.for_field_snap(field_snap),
        # This is a very particular special case where we want someone
        # to be able to select a selector key *without* a value
        #
        # e.g.
        # storage:
        #   filesystem:
        #
        # And we want the default values of the child elements of filesystem:
        # to "fill in"
        {}
        if field_value is None
        and ConfigTypeKind.has_fields(
            context.config_schema_snapshot.get_config_snap(field_snap.type_key).kind
        )
        else field_value,
    )

    if child_evaluate_value_result.success:
        return EvaluateValueResult.for_value(  # type: ignore
            frozendict({field_name: child_evaluate_value_result.value})
        )
    else:
        return child_evaluate_value_result  # type: ignore


def _validate_shape_config(
    context: ValidationContext, config_value: object, check_for_extra_incoming_fields: bool
) -> EvaluateValueResult[Dict[str, object]]:
    check.inst_param(context, "context", ValidationContext)
    check.not_none_param(config_value, "config_value")
    check.bool_param(check_for_extra_incoming_fields, "check_for_extra_incoming_fields")

    field_aliases = check.opt_dict_param(
        cast(Dict[str, str], context.config_type_snap.field_aliases),
        "field_aliases",
        key_type=str,
        value_type=str,
    )

    if not isinstance(config_value, dict):
        return EvaluateValueResult.for_error(create_dict_type_mismatch_error(context, config_value))
    config_value = cast(Dict[str, object], config_value)

    field_snaps = check.not_none(context.config_type_snap.fields)
    defined_field_names = {cast(str, fs.name) for fs in field_snaps}
    defined_field_names = defined_field_names.union(set(field_aliases.values()))

    incoming_field_names = set(config_value.keys())

    errors: List[EvaluationError] = []

    if check_for_extra_incoming_fields:
        _append_if_error(
            errors,
            _check_for_extra_incoming_fields(
                context,
                defined_field_names,
                incoming_field_names,
            ),
        )

    _append_if_error(
        errors,
        _compute_missing_fields_error(context, field_snaps, incoming_field_names, field_aliases),
    )

    # dict is well-formed. now recursively validate all incoming fields

    field_errors = []
    field_snaps = check.not_none(context.config_type_snap.fields)
    for field_snap in field_snaps:
        name = field_snap.name
        aliased_name = field_aliases.get(name)
        if aliased_name is not None and aliased_name in config_value and name in config_value:
            field_errors.append(
                create_field_substitution_collision_error(
                    context.for_field_snap(field_snap), name=name, aliased_name=aliased_name
                )
            )
        elif name in config_value:
            field_evr = _validate_config(context.for_field_snap(field_snap), config_value[name])

            if field_evr.errors:
                field_errors += field_evr.errors
        elif aliased_name is not None and aliased_name in config_value:
            field_evr = _validate_config(
                context.for_field_snap(field_snap), config_value[aliased_name]
            )

            if field_evr.errors:
                field_errors += field_evr.errors

    if field_errors:
        errors += field_errors

    if errors:
        return EvaluateValueResult.for_errors(errors)
    else:
        return EvaluateValueResult.for_value(frozendict(config_value))  # type: ignore


def validate_permissive_shape_config(
    context: ValidationContext, config_value: object
) -> EvaluateValueResult[Dict[str, object]]:
    check.inst_param(context, "context", ValidationContext)
    check.invariant(context.config_type_snap.kind == ConfigTypeKind.PERMISSIVE_SHAPE)
    check.not_none_param(config_value, "config_value")

    return _validate_shape_config(context, config_value, check_for_extra_incoming_fields=False)


def validate_map_config(
    context: ValidationContext, config_value: object
) -> EvaluateValueResult[Dict[str, object]]:
    check.inst_param(context, "context", ValidationContext)
    check.invariant(context.config_type_snap.kind == ConfigTypeKind.MAP)
    check.not_none_param(config_value, "config_value")

    if not isinstance(config_value, dict):
        return EvaluateValueResult.for_error(create_map_error(context, config_value))
    config_value = cast(Dict[object, object], config_value)

    evaluation_results = [
        _validate_config(context.for_map_key(key), key) for key in config_value.keys()
    ] + [
        _validate_config(context.for_map_value(key), config_item)
        for key, config_item in config_value.items()
    ]

    errors = []
    for result in evaluation_results:
        if not result.success:
            errors += cast(List, result.errors)

    return EvaluateValueResult(not bool(errors), frozendict(config_value), errors)  # type: ignore


def validate_shape_config(
    context: ValidationContext, config_value: object
) -> EvaluateValueResult[Dict[str, object]]:
    check.inst_param(context, "context", ValidationContext)
    check.invariant(context.config_type_snap.kind == ConfigTypeKind.STRICT_SHAPE)
    check.not_none_param(config_value, "config_value")

    return _validate_shape_config(context, config_value, check_for_extra_incoming_fields=True)


def _append_if_error(errors: List[EvaluationError], maybe_error: Optional[EvaluationError]) -> None:
    if maybe_error:
        errors.append(maybe_error)


def _check_for_extra_incoming_fields(
    context: ValidationContext, defined_field_names: Set[str], incoming_field_names: Set[str]
) -> Optional[EvaluationError]:
    if extra_fields := list(incoming_field_names - defined_field_names):
        if len(extra_fields) == 1:
            return create_field_not_defined_error(context, extra_fields[0])
        else:
            return create_fields_not_defined_error(context, extra_fields)
    return None


def _compute_missing_fields_error(
    context: ValidationContext,
    field_snaps: List[ConfigFieldSnap],
    incoming_fields: Set[str],
    field_aliases: Dict[str, str],
) -> Optional[EvaluationError]:
    missing_fields = []

    for field_snap in field_snaps:

        field_alias = field_aliases.get(cast(str, field_snap.name))
        if (
            field_snap.is_required
            and field_snap.name not in incoming_fields
            and (field_alias is None or field_alias not in incoming_fields)
        ):
            missing_fields.append(cast(str, field_snap.name))

    if missing_fields:
        if len(missing_fields) == 1:
            return create_missing_required_field_error(context, missing_fields[0])
        else:
            return create_missing_required_fields_error(context, missing_fields)
    return None


def validate_array_config(
    context: ValidationContext, config_value: object
) -> EvaluateValueResult[List[object]]:
    check.inst_param(context, "context", ValidationContext)
    check.invariant(context.config_type_snap.kind == ConfigTypeKind.ARRAY)
    check.not_none_param(config_value, "config_value")

    if not isinstance(config_value, list):
        return EvaluateValueResult.for_error(create_array_error(context, config_value))

    evaluation_results = [
        _validate_config(context.for_array(index), config_item)
        for index, config_item in enumerate(config_value)
    ]

    values = []
    errors = []
    for result in evaluation_results:
        if result.success:
            values.append(result.value)
        else:
            errors += cast(List, result.errors)

    return EvaluateValueResult(not bool(errors), values, errors)  # type: ignore


def validate_enum_config(
    context: ValidationContext, config_value: object
) -> EvaluateValueResult[str]:
    check.inst_param(context, "context", ValidationContext)
    check.invariant(context.config_type_snap.kind == ConfigTypeKind.ENUM)
    check.not_none_param(config_value, "config_value")

    if not isinstance(config_value, str):
        return EvaluateValueResult.for_error(create_enum_type_mismatch_error(context, config_value))

    if not context.config_type_snap.has_enum_value(config_value):
        return EvaluateValueResult.for_error(create_enum_value_missing_error(context, config_value))

    return EvaluateValueResult.for_value(config_value)


def process_config(config_type: object, config_dict: Mapping) -> EvaluateValueResult[Dict]:
    config_type = resolve_to_config_type(config_type)
    config_type = check.inst(cast(ConfigType, config_type), ConfigType)
    validate_evr = validate_config(config_type, config_dict)
    if not validate_evr.success:
        return validate_evr

    return post_process_config(config_type, validate_evr.value)
