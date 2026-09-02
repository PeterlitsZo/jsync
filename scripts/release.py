#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import tomllib

JS_PACKAGE_NAME = "jsync_js"
RUST_PACKAGE_NAME = "jsync_rs"
VERSION_RE = re.compile(r"^\d+\.\d+\.\d+(?:-[0-9A-Za-z]+(?:\.[0-9A-Za-z]+)*)?$")


class ReleaseError(RuntimeError):
    pass


@dataclass(frozen=True)
class Args:
    command: str
    version: str | None


@dataclass(frozen=True)
class Paths:
    root: Path
    js_workspace: Path
    js_package: Path
    js_package_json: Path
    rust_workspace: Path
    rust_manifest: Path
    rust_package_manifest: Path


def main() -> int:
    try:
        args = parse_args()
        paths = find_paths()

        if args.command == "check":
            check(paths, args.version)
        elif args.command == "package":
            package(paths, args.version)
        elif args.command == "publish":
            if args.version is None:
                raise ReleaseError("publish requires --version")
            publish(paths, args.version)
        else:
            raise ReleaseError(f"unknown command: {args.command}")
    except ReleaseError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    return 0


def parse_args() -> Args:
    parser = argparse.ArgumentParser(
        description="Release jsync_js to npm and jsync_rs to crates.io."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    check_parser = subparsers.add_parser("check", help="run release checks")
    _ = check_parser.add_argument("--version", help="expected release version")

    package_parser = subparsers.add_parser("package", help="create local packages")
    _ = package_parser.add_argument("--version", help="expected release version")

    publish_parser = subparsers.add_parser(
        "publish", help="publish jsync_js and jsync_rs"
    )
    _ = publish_parser.add_argument("--version", required=True, help="release version")

    namespace = parser.parse_args()
    command = cast("object", namespace.command)
    version = cast("object", namespace.version)

    if not isinstance(command, str):
        raise ReleaseError(f"invalid command: {command!r}")
    if version is not None and not isinstance(version, str):
        raise ReleaseError(f"invalid version: {version!r}")
    if version is not None:
        check_version_format(version)

    return Args(command=command, version=version)


def find_paths() -> Paths:
    root = find_repo_root(Path.cwd())
    return Paths(
        root=root,
        js_workspace=root / "jsync_js",
        js_package=root / "jsync_js" / "packages" / "jsync",
        js_package_json=root / "jsync_js" / "packages" / "jsync" / "package.json",
        rust_workspace=root / "jsync_rs",
        rust_manifest=root / "jsync_rs" / "Cargo.toml",
        rust_package_manifest=root / "jsync_rs" / "crates" / "jsync_rs" / "Cargo.toml",
    )


def find_repo_root(start: Path) -> Path:
    for path in (start, *start.parents):
        if (
            (path / "jsync_js").is_dir()
            and (path / "jsync_rs").is_dir()
            and (path / "AGENTS.md").is_file()
        ):
            return path
    raise ReleaseError("could not find jsync repository root")


def check(paths: Paths, expected_version: str | None) -> None:
    ensure_tools()
    check_versions(paths, expected_version)
    check_js(paths)
    check_rust(paths)


def package(paths: Paths, expected_version: str | None) -> None:
    ensure_clean_worktree(paths)
    check(paths, expected_version)
    run(["npm", "pack"], paths.js_package)
    run(
        [
            "cargo",
            "package",
            "--manifest-path",
            str(paths.rust_package_manifest),
            "--locked",
        ],
        paths.root,
    )
    crate_path = (
        f"{paths.rust_workspace}/target/package/"
        f"{RUST_PACKAGE_NAME}-{rust_version(paths)}.crate"
    )
    print(f"cargo crate: {crate_path}")


def publish(paths: Paths, version: str) -> None:
    ensure_clean_worktree(paths)
    check(paths, version)
    print_release_summary(paths)
    confirm_publish(version)
    run(
        [
            "cargo",
            "publish",
            "--manifest-path",
            str(paths.rust_package_manifest),
            "--locked",
        ],
        paths.root,
    )
    run(["npm", "publish"], paths.js_package)


def ensure_tools() -> None:
    missing = [
        tool for tool in ("git", "pnpm", "npm", "cargo") if shutil.which(tool) is None
    ]
    if missing:
        raise ReleaseError(f"missing required tools: {', '.join(missing)}")


def ensure_clean_worktree(paths: Paths) -> None:
    status = output(["git", "status", "--porcelain"], paths.root)
    if status.strip():
        raise ReleaseError("worktree is not clean; commit or stash changes first")


def check_versions(paths: Paths, expected_version: str | None) -> None:
    js = read_js_package(paths)
    rust = read_rust_package(paths)

    if js.get("name") != JS_PACKAGE_NAME:
        actual = js.get("name")
        message = (
            f"{paths.js_package_json} has name={actual!r}, expected {JS_PACKAGE_NAME!r}"
        )
        raise ReleaseError(message)
    if rust.get("name") != RUST_PACKAGE_NAME:
        actual = rust.get("name")
        message = (
            f"{paths.rust_package_manifest} has name={actual!r}, "
            f"expected {RUST_PACKAGE_NAME!r}"
        )
        raise ReleaseError(message)
    if js.get("private") is True:
        raise ReleaseError(f"{paths.js_package_json} still has private=true")

    js_current = js_version(paths)
    rust_current = rust_version(paths)
    check_version_format(js_current)
    check_version_format(rust_current)
    if js_current != rust_current:
        raise ReleaseError(
            f"JS version {js_current} does not match Rust version {rust_current}"
        )
    if expected_version is not None and js_current != expected_version:
        raise ReleaseError(
            f"current version is {js_current}, expected {expected_version}"
        )


def check_js(paths: Paths) -> None:
    run(
        ["pnpm", "--dir", str(paths.js_workspace), "install", "--frozen-lockfile"],
        paths.root,
    )
    pnpm_run(paths, "typecheck")
    pnpm_run(paths, "test")
    pnpm_run(paths, "build")


def pnpm_run(paths: Paths, script: str) -> None:
    run(
        [
            "pnpm",
            "--dir",
            str(paths.js_workspace),
            "--filter",
            JS_PACKAGE_NAME,
            "run",
            script,
        ],
        paths.root,
    )


def check_rust(paths: Paths) -> None:
    run(
        ["cargo", "fmt", "--manifest-path", str(paths.rust_manifest), "--check"],
        paths.root,
    )
    run(
        ["cargo", "check", "--manifest-path", str(paths.rust_manifest), "--locked"],
        paths.root,
    )
    run(
        ["cargo", "test", "--manifest-path", str(paths.rust_manifest), "--locked"],
        paths.root,
    )


def print_release_summary(paths: Paths) -> None:
    commit = output(["git", "rev-parse", "--short", "HEAD"], paths.root).strip()
    print()
    print("Release summary:")
    print(f"- npm: {JS_PACKAGE_NAME}@{js_version(paths)}")
    print(f"- crates.io: {RUST_PACKAGE_NAME}@{rust_version(paths)}")
    print(f"- git commit: {commit}")


def confirm_publish(version: str) -> None:
    expected = f"publish {version}"
    entered = input(f"Type {expected!r} to publish: ")
    if entered != expected:
        raise ReleaseError("publish confirmation did not match")


def js_version(paths: Paths) -> str:
    version = read_js_package(paths).get("version")
    if isinstance(version, str) and version:
        return version
    raise ReleaseError(f"{paths.js_package_json} is missing version")


def rust_version(paths: Paths) -> str:
    package = read_rust_package(paths)
    version = package.get("version")
    if isinstance(version, str):
        return version

    if (
        isinstance(version, dict)
        and cast("Mapping[str, object]", version).get("workspace") is True
    ):
        workspace = cast("object", read_toml(paths.rust_manifest).get("workspace"))
        if isinstance(workspace, dict):
            workspace_table = cast("Mapping[str, object]", workspace)
            workspace_package = workspace_table.get("package")
            if isinstance(workspace_package, dict):
                workspace_package_table = cast(
                    "Mapping[str, object]", workspace_package
                )
                workspace_version = workspace_package_table.get("version")
                if isinstance(workspace_version, str) and workspace_version:
                    return workspace_version

    raise ReleaseError(f"{paths.rust_package_manifest} is missing version")


def read_js_package(paths: Paths) -> Mapping[str, object]:
    return read_json(paths.js_package_json)


def read_rust_package(paths: Paths) -> Mapping[str, object]:
    package = read_toml(paths.rust_package_manifest).get("package")
    if isinstance(package, dict):
        return cast("Mapping[str, object]", package)
    raise ReleaseError(f"{paths.rust_package_manifest} is missing [package]")


def read_json(path: Path) -> Mapping[str, object]:
    data = cast("object", json.loads(path.read_text(encoding="utf-8")))
    if isinstance(data, dict):
        return cast("Mapping[str, object]", data)
    raise ReleaseError(f"{path} is not a JSON object")


def read_toml(path: Path) -> Mapping[str, object]:
    data = cast("object", tomllib.loads(path.read_text(encoding="utf-8")))
    if isinstance(data, dict):
        return cast("Mapping[str, object]", data)
    raise ReleaseError(f"{path} is not a TOML table")


def check_version_format(version: str) -> None:
    if not VERSION_RE.fullmatch(version):
        raise ReleaseError(
            f"invalid version {version!r}; expected MAJOR.MINOR.PATCH or prerelease"
        )


def run(command: Sequence[str], cwd: Path) -> None:
    print(f"+ ({cwd}) {' '.join(command)}", flush=True)
    result = subprocess.run(list(command), cwd=cwd, check=False)
    if result.returncode != 0:
        raise ReleaseError(
            f"command failed with exit code {result.returncode}: {' '.join(command)}"
        )


def output(command: Sequence[str], cwd: Path) -> str:
    print(f"+ ({cwd}) {' '.join(command)}", flush=True)
    result = subprocess.run(
        list(command),
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        message = (
            f"command failed with exit code {result.returncode}: {' '.join(command)}"
        )
        if result.stderr.strip():
            message = f"{message}\n{result.stderr.strip()}"
        raise ReleaseError(message)
    return result.stdout


if __name__ == "__main__":
    raise SystemExit(main())
