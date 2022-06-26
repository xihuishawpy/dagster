from typing import Optional

from dagster import PipelineDefinition
from dagster import _check as check
from dagster.config.config_type import ConfigType, ConfigTypeKind
from dagster.core.definitions import create_run_config_schema


def scaffold_pipeline_config(
    pipeline_def: PipelineDefinition, skip_non_required: bool = True, mode: Optional[str] = None
):
    check.inst_param(pipeline_def, "pipeline_def", PipelineDefinition)
    check.bool_param(skip_non_required, "skip_non_required")

    env_config_type = create_run_config_schema(pipeline_def, mode=mode).config_type

    env_dict = {}

    for env_field_name, env_field in env_config_type.fields.items():  # type: ignore
        if skip_non_required and not env_field.is_required:
            continue

        # unfortunately we have to treat this special for now
        if (
            env_field_name == "context"
            and skip_non_required
            and not env_config_type.fields["context"].is_required
        ):
            continue

        env_dict[env_field_name] = scaffold_type(env_field.config_type, skip_non_required)

    return env_dict


def scaffold_type(config_type: ConfigType, skip_non_required: bool = True):
    check.inst_param(config_type, "config_type", ConfigType)
    check.bool_param(skip_non_required, "skip_non_required")

    # Right now selectors and composites have the same
    # scaffolding logic, which might not be wise.
    if ConfigTypeKind.has_fields(config_type.kind):
        return {
            field_name: scaffold_type(field.config_type, skip_non_required)
            for field_name, field in config_type.fields.items()
            if not skip_non_required or field.is_required
        }

    elif config_type.kind == ConfigTypeKind.ANY:
        return "AnyType"
    elif config_type.kind == ConfigTypeKind.SCALAR:
        defaults = {"String": "", "Int": 0, "Bool": True}

        return defaults[config_type.given_name]  # type: ignore
    elif config_type.kind == ConfigTypeKind.ARRAY:
        return []
    elif config_type.kind == ConfigTypeKind.MAP:
        return {}
    elif config_type.kind == ConfigTypeKind.ENUM:
        return "|".join(sorted(map(lambda v: v.config_value, config_type.enum_values)))  # type: ignore
    else:
        check.failed(
            "Do not know how to scaffold {type_name}".format(type_name=config_type.given_name)
        )
