import subprocess
from distutils import spawn
from typing import Iterable, List, Optional

import click


def check_output(cmd: List[str], dry_run: bool = True, cwd: Optional[str] = None) -> Optional[str]:
    if not dry_run:
        return subprocess.check_output(cmd, text=True, stderr=subprocess.STDOUT, cwd=cwd)
    click.echo(
        click.style("Dry run; not running.", fg="red")
        + f' Would run: {" ".join(cmd)}'
    )

    return None


def which_(exe: str) -> Optional[str]:
    """Uses distutils to look for an executable, mimicking unix which"""
    # https://github.com/PyCQA/pylint/issues/73
    return spawn.find_executable(exe)


def all_equal(iterable: Iterable[object]) -> bool:
    return len(set(iterable)) == 1
