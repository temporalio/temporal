#!/usr/bin/env python3
"""
Dump help output for all tdbg commands with feature flag on/off into a tree-structured directory.

- Reads commands from tools/tdbg/commandsgen/commands.yml
- For each command path (e.g., "tdbg workflow show"):
  - Creates a nested directory structure under output root mirroring the command path:
      help_tree/tdbg/workflow/show/
  - Writes two files with the help output:
      on.txt  (ENABLE_TDBG_V2=true)
      off.txt (ENABLE_TDBG_V2=false)

By default, this script:
- Runs in the repository root discovered relative to this file.
- Invokes: go run cmd/tools/tdbg/main.go <subpath> --help
- Captures both stdout and stderr into the files for diffing.
- Sets NO_COLOR=1 to attempt to disable color output for consistent diffs.

Usage:
  python3 tools/tdbg/commandsgen/dump_help_tree.py
  python3 tools/tdbg/commandsgen/dump_help_tree.py --clean --verbose
  python3 tools/tdbg/commandsgen/dump_help_tree.py --out tools/tdbg/commandsgen/help_tree
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Tuple


DEFAULT_COMMANDS_YAML = Path("tools/tdbg/commandsgen/commands.yml")
DEFAULT_MAIN_GO = Path("cmd/tools/tdbg/main.go")
DEFAULT_OUT_DIR = Path("tools/tdbg/commandsgen/help_tree")
RUN_TIMEOUT_SECS = 60  # per invocation


def find_repo_root(start: Path) -> Path:
    """
    Attempt to discover the repo root by looking for cmd/tools/tdbg/main.go upwards.
    Falls back to the first "temporal" ancestor directory, else the starting directory.
    """
    cur = start.resolve()
    # Search upwards for a directory containing DEFAULT_MAIN_GO
    for parent in [cur, *cur.parents]:
        if (parent / DEFAULT_MAIN_GO).is_file():
            return parent

    # Fallback: find the nearest ancestor named "temporal"
    for parent in [cur, *cur.parents]:
        if parent.name == "temporal":
            return parent

    # Worst-case fallback: use the starting directory
    return cur


def parse_command_names(commands_yml_path: Path) -> List[str]:
    """
    Parse the commands.yml and extract the 'name' values for commands.
    Only picks up top-level command entries under 'commands:' and ignores nested 'options' names.

    This uses a simple indentation-based heuristic:
    - Must be within the 'commands:' section.
    - Must match lines of the form: '  - name: <value>' (exactly two leading spaces).
    """
    content = commands_yml_path.read_text(encoding="utf-8").splitlines()
    inside_commands = False
    names: List[str] = []

    name_line_re = re.compile(r"^  -\s+name:\s+(.+?)\s*$")

    for line in content:
        if not inside_commands:
            # Enter the commands section
            if re.match(r"^commands:\s*$", line):
                inside_commands = True
            continue

        # End of commands section when we encounter a top-level key (no indentation)
        if re.match(r"^[A-Za-z0-9_-]+:\s*$", line):
            break

        # Match only two-space-indented "name" lines to avoid options-level matches
        m = name_line_re.match(line)
        if m:
            value = m.group(1).strip()
            if value:
                names.append(value)

    # Sanity: keep only those that start with 'tdbg'
    names = [n for n in names if n.startswith("tdbg")]
    return names


def run_tdbg_help(
    repo_root: Path,
    subpath_parts: List[str],
    enable_v2: bool,
    main_go_rel: Path,
    timeout_secs: int = RUN_TIMEOUT_SECS,
) -> Tuple[int, str]:
    """
    Execute the tdbg CLI help for a given subpath with v2 enabled or disabled.

    subpath_parts: the command tokens after 'tdbg', e.g. ['workflow', 'show']
    Returns (exit_code, combined_output)
    """
    cmd = ["go", "run", str(main_go_rel)]
    cmd.extend(subpath_parts)
    cmd.append("--help")

    env = os.environ.copy()
    env["ENABLE_TDBG_V2"] = "true" if enable_v2 else "false"
    # Try to disable color for stable diffs
    env["NO_COLOR"] = env.get("NO_COLOR", "1")

    try:
        proc = subprocess.run(
            cmd,
            cwd=str(repo_root),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout_secs,
        )
        return proc.returncode, proc.stdout
    except subprocess.TimeoutExpired as e:
        combined = (e.stdout or "") + (e.stderr or "")
        combined += f"\n[Timed out after {timeout_secs}s]\n"
        return 124, combined
    except FileNotFoundError as e:
        return 127, f"{e}\n"


def command_to_dir(out_root: Path, command_name: str) -> Path:
    """
    Convert a command name like 'tdbg workflow show' into a directory path:
      out_root/tdbg/workflow/show
    """
    parts = command_name.split()
    return out_root.joinpath(*parts)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8", newline="\n")


def dump_all(
    repo_root: Path,
    out_root: Path,
    commands: Iterable[str],
    main_go_rel: Path,
    verbose: bool = False,
) -> None:
    for name in commands:
        # Determine the target directory
        cmd_dir = command_to_dir(out_root, name)
        # The subpath excludes the root 'tdbg'
        parts = name.split()[1:]  # [] for root

        if verbose:
            rel_dir = cmd_dir.relative_to(repo_root)
            print(f"[+] {name} -> {rel_dir}")

        # V2 on
        rc_on, out_on = run_tdbg_help(repo_root, parts, enable_v2=True, main_go_rel=main_go_rel)
        write_text(cmd_dir / "on.txt", out_on)

        # V2 off
        rc_off, out_off = run_tdbg_help(repo_root, parts, enable_v2=False, main_go_rel=main_go_rel)
        write_text(cmd_dir / "off.txt", out_off)

        if verbose:
            print(f"    wrote on.txt (rc={rc_on}), off.txt (rc={rc_off})")


def main(argv: List[str]) -> int:
    parser = argparse.ArgumentParser(description="Dump help output for tdbg commands (v2 on/off) as a tree.")
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUT_DIR})",
    )
    parser.add_argument(
        "--commands-yaml",
        type=Path,
        default=DEFAULT_COMMANDS_YAML,
        help=f"Path to commands.yml (default: {DEFAULT_COMMANDS_YAML})",
    )
    parser.add_argument(
        "--main-go",
        type=Path,
        default=DEFAULT_MAIN_GO,
        help=f"Relative path (from repo root) to main.go (default: {DEFAULT_MAIN_GO})",
    )
    parser.add_argument("--clean", action="store_true", help="Remove output directory before dumping.")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output.")
    args = parser.parse_args(argv)

    # Resolve repo root relative to this script location
    script_path = Path(__file__).resolve()
    repo_root = find_repo_root(script_path.parent)
    if args.verbose:
        print(f"Repo root: {repo_root}")

    commands_yml = (repo_root / args.commands_yaml).resolve()
    if not commands_yml.is_file():
        print(f"commands.yml not found at: {commands_yml}", file=sys.stderr)
        return 2

    main_go_path = (repo_root / args.main_go).resolve()
    if not main_go_path.is_file():
        print(f"main.go not found at: {main_go_path}", file=sys.stderr)
        return 2

    out_root = (repo_root / args.out).resolve()
    if args.clean and out_root.exists():
        if args.verbose:
            print(f"Cleaning output directory: {out_root}")
        shutil.rmtree(out_root)

    if args.verbose:
        print(f"Parsing commands from: {commands_yml}")

    commands = parse_command_names(commands_yml)
    if args.verbose:
        print(f"Found {len(commands)} commands.")
        for c in commands:
            print(f" - {c}")

    dump_all(
        repo_root=repo_root,
        out_root=out_root,
        commands=commands,
        main_go_rel=args.main_go,
        verbose=args.verbose,
    )

    if args.verbose:
        print(f"Done. Output in: {out_root}")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
