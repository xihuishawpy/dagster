import keyword
import os
import re
from glob import glob
from typing import Any, Dict, List, Optional, Tuple

import pkg_resources
import yaml

import dagster._check as check
import dagster.seven as seven
from dagster.core.errors import DagsterInvalidDefinitionError, DagsterInvariantViolationError
from dagster.core.storage.tags import check_reserved_tags
from dagster.utils import frozentags
from dagster.utils.yaml_utils import merge_yaml_strings, merge_yamls

DEFAULT_OUTPUT = "result"
DEFAULT_GROUP_NAME = "default"  # asset group_name used when none is provided
DEFAULT_IO_MANAGER_KEY = "io_manager"

DISALLOWED_NAMES = set(
    [
        "context",
        "conf",
        "config",
        "meta",
        "arg_dict",
        "dict",
        "input_arg_dict",
        "output_arg_dict",
        "int",
        "str",
        "float",
        "bool",
        "input",
        "output",
        "type",
    ]
    + list(keyword.kwlist)  # just disallow all python keywords
)

VALID_NAME_REGEX_STR = r"^[A-Za-z0-9_]+$"
VALID_NAME_REGEX = re.compile(VALID_NAME_REGEX_STR)


class NoValueSentinel:
    """Sentinel value to distinguish unset from None"""


def has_valid_name_chars(name):
    return bool(VALID_NAME_REGEX.match(name))


def check_valid_name(name: str):
    check.str_param(name, "name")
    if name in DISALLOWED_NAMES:
        raise DagsterInvalidDefinitionError(
            f'"{name}" is not a valid name in Dagster. It conflicts with a Dagster or python reserved keyword.'
        )

    if not has_valid_name_chars(name):
        raise DagsterInvalidDefinitionError(
            f'"{name}" is not a valid name in Dagster. Names must be in regex {VALID_NAME_REGEX_STR}.'
        )

    check.invariant(is_valid_name(name))
    return name


def is_valid_name(name):
    check.str_param(name, "name")

    return name not in DISALLOWED_NAMES and has_valid_name_chars(name)


def _kv_str(key, value):
    return '{key}="{value}"'.format(key=key, value=repr(value))


def struct_to_string(name, **kwargs):
    # Sort the kwargs to ensure consistent representations across Python versions
    props_str = ", ".join([_kv_str(key, value) for key, value in sorted(kwargs.items())])
    return "{name}({props_str})".format(name=name, props_str=props_str)


def validate_tags(tags: Optional[Dict[str, Any]], allow_reserved_tags=True) -> Dict[str, str]:
    valid_tags = {}
    for key, value in check.opt_dict_param(tags, "tags", key_type=str).items():
        if not isinstance(value, str):
            valid = False
            err_reason = f'Could not JSON encode value "{value}"'
            try:
                str_val = seven.json.dumps(value)
                err_reason = 'JSON encoding "{json}" of value "{val}" is not equivalent to original value'.format(
                    json=str_val, val=value
                )

                valid = seven.json.loads(str_val) == value
            except Exception:
                pass

            if not valid:
                raise DagsterInvalidDefinitionError(
                    'Invalid value for tag "{key}", {err_reason}. Tag values must be strings '
                    "or meet the constraint that json.loads(json.dumps(value)) == value.".format(
                        key=key, err_reason=err_reason
                    )
                )

            valid_tags[key] = str_val
        else:
            valid_tags[key] = value

    if not allow_reserved_tags:
        check_reserved_tags(valid_tags)

    return frozentags(valid_tags)


def validate_group_name(group_name: Optional[str]) -> str:
    """Ensures a string name is valid and returns a default if no name provided."""
    return check_valid_name(group_name) if group_name else DEFAULT_GROUP_NAME


def config_from_files(config_files: List[str]) -> Dict[str, Any]:
    """Constructs run config from YAML files.

    Args:
        config_files (List[str]): List of paths or glob patterns for yaml files
            to load and parse as the run config.

    Returns:
        Dict[str, Any]: A run config dictionary constructed from provided YAML files.

    Raises:
        FileNotFoundError: When a config file produces no results
        DagsterInvariantViolationError: When one of the YAML files is invalid and has a parse
            error.
    """
    config_files = check.opt_list_param(config_files, "config_files")

    filenames = []
    for file_glob in config_files or []:
        globbed_files = glob(file_glob)
        if not globbed_files:
            raise DagsterInvariantViolationError(
                'File or glob pattern "{file_glob}" for "config_files" '
                "produced no results.".format(file_glob=file_glob)
            )

        filenames += [os.path.realpath(globbed_file) for globbed_file in globbed_files]

    try:
        run_config = merge_yamls(filenames)
    except yaml.YAMLError as err:
        raise DagsterInvariantViolationError(
            f"Encountered error attempting to parse yaml. Parsing files {filenames} "
            f"loaded by file/patterns {config_files}."
        ) from err

    return run_config


def config_from_yaml_strings(yaml_strings: List[str]) -> Dict[str, Any]:
    """Static constructor for run configs from YAML strings.

    Args:
        yaml_strings (List[str]): List of yaml strings to parse as the run config.

    Returns:
        Dict[Str, Any]: A run config dictionary constructed from the provided yaml strings

    Raises:
        DagsterInvariantViolationError: When one of the YAML documents is invalid and has a
            parse error.
    """
    yaml_strings = check.list_param(yaml_strings, "yaml_strings", of_type=str)

    try:
        run_config = merge_yaml_strings(yaml_strings)
    except yaml.YAMLError as err:
        raise DagsterInvariantViolationError(
            f"Encountered error attempting to parse yaml. Parsing YAMLs {yaml_strings} "
        ) from err

    return run_config


def config_from_pkg_resources(pkg_resource_defs: List[Tuple[str, str]]) -> Dict[str, Any]:
    """Load a run config from a package resource, using :py:func:`pkg_resources.resource_string`.

    Example:

    .. code-block:: python

        config_from_pkg_resources(
            pkg_resource_defs=[
                ('dagster_examples.airline_demo.environments', 'local_base.yaml'),
                ('dagster_examples.airline_demo.environments', 'local_warehouse.yaml'),
            ],
        )


    Args:
        pkg_resource_defs (List[(str, str)]): List of pkg_resource modules/files to
            load as the run config.

    Returns:
        Dict[Str, Any]: A run config dictionary constructed from the provided yaml strings

    Raises:
        DagsterInvariantViolationError: When one of the YAML documents is invalid and has a
            parse error.
    """
    pkg_resource_defs = check.list_param(pkg_resource_defs, "pkg_resource_defs", of_type=tuple)

    try:
        yaml_strings = [
            pkg_resources.resource_string(*pkg_resource_def).decode("utf-8")
            for pkg_resource_def in pkg_resource_defs
        ]
    except (ModuleNotFoundError, FileNotFoundError, UnicodeDecodeError) as err:
        raise DagsterInvariantViolationError(
            "Encountered error attempting to parse yaml. Loading YAMLs from "
            f"package resources {pkg_resource_defs}."
        ) from err

    return config_from_yaml_strings(yaml_strings=yaml_strings)
