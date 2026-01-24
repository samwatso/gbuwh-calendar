#!/usr/bin/env python3
"""
D1 database wrapper using Wrangler CLI.

Provides a simple interface for executing SQL against Cloudflare D1
using `npx wrangler d1 execute` in non-interactive mode.

Environment variables:
    D1_DB_NAME: Name of the D1 database (required)
    DRY_RUN: Set to "1" to log SQL without executing (optional)

Usage:
    from d1 import D1Client

    client = D1Client()
    client.execute_sql("INSERT INTO events ...")
    results = client.query_json("SELECT * FROM events")
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


class D1Error(Exception):
    """Error from D1 execution."""
    pass


class D1Client:
    """Wrapper around Wrangler D1 CLI commands."""

    def __init__(self, db_name: str | None = None, dry_run: bool | None = None):
        """
        Initialize D1 client.

        Args:
            db_name: D1 database name. Defaults to D1_DB_NAME env var.
            dry_run: If True, log SQL but don't execute. Defaults to DRY_RUN env var.
        """
        self.db_name = db_name or os.environ.get("D1_DB_NAME")
        if not self.db_name:
            raise D1Error("D1_DB_NAME environment variable is required")

        if dry_run is None:
            dry_run = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")
        self.dry_run = dry_run

        if self.dry_run:
            logger.info("DRY_RUN mode enabled - SQL will be logged but not executed")

    def _run_wrangler(
        self,
        args: list[str],
        check: bool = True,
        capture_output: bool = True,
    ) -> subprocess.CompletedProcess:
        """Run a wrangler command."""
        cmd = ["npx", "wrangler", "d1", "execute", self.db_name, "--remote", "--yes"] + args

        logger.debug(f"Running: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=capture_output,
                text=True,
                check=False,  # We handle errors ourselves
            )

            if check and result.returncode != 0:
                error_msg = result.stderr or result.stdout or "Unknown error"
                logger.error(f"Wrangler command failed: {error_msg}")
                raise D1Error(f"Wrangler command failed: {error_msg}")

            return result

        except FileNotFoundError:
            raise D1Error("npx/wrangler not found. Ensure Node.js and Wrangler are installed.")

    def execute_sql(self, sql: str) -> None:
        """
        Execute a SQL statement (no result expected).

        Args:
            sql: SQL statement to execute
        """
        if self.dry_run:
            logger.info(f"[DRY_RUN] Would execute SQL:\n{sql[:500]}...")
            return

        logger.info(f"Executing SQL command ({len(sql)} chars)")
        self._run_wrangler(["--command", sql])
        logger.info("SQL command executed successfully")

    def execute_file(self, path: str) -> None:
        """
        Execute SQL from a file.

        Args:
            path: Path to SQL file
        """
        if self.dry_run:
            with open(path, "r") as f:
                sql = f.read()
            logger.info(f"[DRY_RUN] Would execute SQL file {path}:\n{sql[:500]}...")
            return

        logger.info(f"Executing SQL file: {path}")
        self._run_wrangler(["--file", path])
        logger.info(f"SQL file executed successfully: {path}")

    def query_json(self, sql: str) -> list[dict[str, Any]]:
        """
        Execute a SQL query and return results as JSON.

        Args:
            sql: SQL SELECT statement

        Returns:
            List of row dictionaries
        """
        if self.dry_run:
            logger.info(f"[DRY_RUN] Would query:\n{sql}")
            return []

        logger.info(f"Executing query ({len(sql)} chars)")
        result = self._run_wrangler(["--json", "--command", sql])

        try:
            # Wrangler --json outputs the result as JSON
            # The structure is typically: [{"results": [...], "success": true, ...}]
            data = json.loads(result.stdout)

            # Handle different possible output formats
            if isinstance(data, list) and len(data) > 0:
                # Standard format: array with result objects
                first = data[0]
                if isinstance(first, dict) and "results" in first:
                    return first["results"]
                return data
            elif isinstance(data, dict):
                if "results" in data:
                    return data["results"]
                return [data]

            return data

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response: {e}")
            logger.error(f"Raw output: {result.stdout[:500]}")
            raise D1Error(f"Failed to parse query results: {e}")


def main() -> int:
    """Simple CLI for testing."""
    import argparse

    parser = argparse.ArgumentParser(description="D1 CLI wrapper")
    parser.add_argument("--command", "-c", type=str, help="SQL command to execute")
    parser.add_argument("--file", "-f", type=str, help="SQL file to execute")
    parser.add_argument("--query", "-q", type=str, help="SQL query (returns JSON)")
    args = parser.parse_args()

    try:
        client = D1Client()

        if args.query:
            results = client.query_json(args.query)
            print(json.dumps(results, indent=2))
        elif args.command:
            client.execute_sql(args.command)
            print("Command executed successfully")
        elif args.file:
            client.execute_file(args.file)
            print("File executed successfully")
        else:
            parser.print_help()
            return 1

        return 0

    except D1Error as e:
        logger.error(str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
