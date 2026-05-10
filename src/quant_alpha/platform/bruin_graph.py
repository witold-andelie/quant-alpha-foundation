"""Bruin-style asset graph: lineage tracking and local execution."""
from __future__ import annotations

import subprocess
import sys
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

import yaml


class AssetStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class AssetNode:
    name: str
    asset_type: str          # python | duckdb.table | bigquery.table
    connection: str
    description: str
    depends: list[str] = field(default_factory=list)
    owner: str = ""
    tags: list[str] = field(default_factory=list)
    columns: list[dict[str, Any]] = field(default_factory=list)
    custom_checks: list[dict[str, Any]] = field(default_factory=list)
    run_file: str | None = None   # python runner script
    sql_file: str | None = None   # SQL asset file
    status: AssetStatus = AssetStatus.PENDING
    duration_s: float = 0.0
    error: str | None = None


class AssetGraph:
    """In-memory directed acyclic graph of Bruin assets with topological runner."""

    def __init__(self, bruin_root: Path) -> None:
        self.root = bruin_root
        self.nodes: dict[str, AssetNode] = {}
        self._load_all_assets()

    def _load_all_assets(self) -> None:
        pipelines = self.root / "pipelines"
        if not pipelines.exists():
            return
        for asset_file in sorted(pipelines.rglob("*.asset.yml")):
            node = self._parse_asset_yml(asset_file)
            self.nodes[node.name] = node
        for sql_file in sorted(pipelines.rglob("*.sql")):
            node = self._parse_sql_asset(sql_file)
            if node:
                self.nodes[node.name] = node

    def _parse_asset_yml(self, path: Path) -> AssetNode:
        data = yaml.safe_load(path.read_text())
        run_cfg = data.get("run", {})
        return AssetNode(
            name=data["name"],
            asset_type=data.get("type", "python"),
            connection=data.get("connection", "duckdb_local"),
            description=data.get("description", "").strip(),
            depends=data.get("depends", []),
            owner=data.get("owner", ""),
            tags=data.get("tags", []),
            columns=data.get("columns", []),
            custom_checks=data.get("custom_checks", []),
            run_file=str(path.parent / run_cfg["file"]) if run_cfg.get("file") else None,
        )

    def _parse_sql_asset(self, path: Path) -> AssetNode | None:
        """Parse @asset frontmatter comment block inside a SQL file."""
        text = path.read_text()
        if "@asset" not in text:
            return None
        start = text.find("/* @asset")
        end = text.find("*/", start)
        if start == -1 or end == -1:
            return None
        yaml_block = text[start + len("/* @asset"):end].strip()
        try:
            data = yaml.safe_load(yaml_block)
        except yaml.YAMLError:
            return None
        if not data or "name" not in data:
            return None
        return AssetNode(
            name=data["name"],
            asset_type=data.get("type", "duckdb.table"),
            connection=data.get("connection", "duckdb_local"),
            description=data.get("description", "").strip(),
            depends=data.get("depends", []),
            owner=data.get("owner", ""),
            tags=data.get("tags", []),
            columns=data.get("columns", []),
            sql_file=str(path),
        )

    # ------------------------------------------------------------------
    # Lineage queries
    # ------------------------------------------------------------------

    def upstream(self, name: str, depth: int = 99) -> list[str]:
        """Return all upstream dependencies (breadth-first)."""
        if name not in self.nodes:
            return []
        visited: list[str] = []
        queue = list(self.nodes[name].depends)
        while queue and depth > 0:
            depth -= 1
            nxt: list[str] = []
            for dep in queue:
                if dep not in visited:
                    visited.append(dep)
                    nxt.extend(self.nodes[dep].depends if dep in self.nodes else [])
            queue = nxt
        return visited

    def downstream(self, name: str) -> list[str]:
        """Return all assets that directly or indirectly depend on `name`."""
        result = []
        for node_name, node in self.nodes.items():
            if name in self.upstream(node_name):
                result.append(node_name)
        return result

    def topological_order(self) -> list[str]:
        """Kahn's algorithm — returns names in execution order."""
        in_degree: dict[str, int] = {n: 0 for n in self.nodes}
        for node in self.nodes.values():
            for dep in node.depends:
                if dep in in_degree:
                    in_degree[node.name] += 1

        queue = [n for n, d in in_degree.items() if d == 0]
        order: list[str] = []
        while queue:
            n = queue.pop(0)
            order.append(n)
            for downstream_node in self.nodes.values():
                if n in downstream_node.depends:
                    in_degree[downstream_node.name] -= 1
                    if in_degree[downstream_node.name] == 0:
                        queue.append(downstream_node.name)
        return order

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(
        self,
        targets: list[str] | None = None,
        env: dict[str, str] | None = None,
        dry_run: bool = False,
    ) -> dict[str, AssetStatus]:
        """Run assets in topological order. If targets given, run only them + upstream."""
        import os

        run_env = {**os.environ, **(env or {})}
        order = self.topological_order()

        if targets:
            needed: set[str] = set()
            for t in targets:
                needed.add(t)
                needed.update(self.upstream(t))
            order = [n for n in order if n in needed]

        for name in order:
            node = self.nodes[name]
            if dry_run:
                print(f"[dry-run] {name}  ({node.asset_type})")
                node.status = AssetStatus.SKIPPED
                continue

            deps_ok = all(
                self.nodes[d].status == AssetStatus.SUCCESS
                for d in node.depends
                if d in self.nodes
            )
            if not deps_ok:
                node.status = AssetStatus.SKIPPED
                print(f"  SKIP  {name}  (upstream failed)")
                continue

            node.status = AssetStatus.RUNNING
            print(f"  RUN   {name}  ({node.asset_type}) ...", end=" ", flush=True)
            t0 = time.monotonic()
            try:
                self._execute_node(node, run_env)
                node.duration_s = time.monotonic() - t0
                node.status = AssetStatus.SUCCESS
                print(f"OK  ({node.duration_s:.1f}s)")
            except Exception as exc:
                node.duration_s = time.monotonic() - t0
                node.status = AssetStatus.FAILED
                node.error = str(exc)
                print(f"FAIL  ({node.duration_s:.1f}s)")
                print(f"        → {exc}", file=sys.stderr)

        return {n: self.nodes[n].status for n in order}

    def _execute_node(self, node: AssetNode, env: dict[str, str]) -> None:
        if node.run_file:
            run_path = Path(node.run_file).resolve()
            if not str(run_path).startswith(str(self.root.resolve())):
                raise RuntimeError(f"run_file escapes project root: {node.run_file!r}")
            result = subprocess.run(
                [sys.executable, str(run_path)],
                env=env,
                capture_output=True,
                text=True,
                timeout=300,
            )
            if result.returncode != 0:
                raise RuntimeError(result.stderr.strip() or "non-zero exit")
        elif node.sql_file:
            # SQL assets: just validate the file exists/parses (actual run via dbt/DuckDB)
            Path(node.sql_file).read_text()   # raises if missing
        else:
            raise RuntimeError(f"No run_file or sql_file for asset '{node.name}'")

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def lineage_report(self) -> str:
        lines = ["Asset Lineage Graph", "=" * 50]
        for name in self.topological_order():
            node = self.nodes[name]
            deps = " ← " + ", ".join(node.depends) if node.depends else ""
            lines.append(f"  {name}{deps}")
            lines.append(f"    type={node.asset_type}  owner={node.owner}  tags={node.tags}")
        return "\n".join(lines)

    def status_report(self) -> str:
        lines = ["Asset Status", "=" * 50]
        icons = {
            AssetStatus.SUCCESS: "✓",
            AssetStatus.FAILED: "✗",
            AssetStatus.SKIPPED: "−",
            AssetStatus.PENDING: "·",
            AssetStatus.RUNNING: "▶",
        }
        for name in self.topological_order():
            node = self.nodes[name]
            icon = icons[node.status]
            dur = f"{node.duration_s:.1f}s" if node.duration_s else ""
            err = f"  ERROR: {node.error}" if node.error else ""
            lines.append(f"  {icon} {name}  {dur}{err}")
        return "\n".join(lines)
