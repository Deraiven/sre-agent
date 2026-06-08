#!/usr/bin/env python3
"""Seed the services table from data/prep/services_seed.jsonl."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from pathlib import Path


DEFAULT_DATABASE_URL = "postgresql://sre_agent:sre_agent@localhost:5432/sre_agent"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL))
    parser.add_argument("--services-seed", default="data/prep/services_seed.jsonl")
    return parser.parse_args()


def sql_literal(value: object) -> str:
    if value is None:
        return "null"
    if isinstance(value, list):
        items = ",".join(str(item).replace("\\", "\\\\").replace('"', '\\"') for item in value)
        return f"'{{{items}}}'"
    text = str(value)
    return "'" + text.replace("'", "''") + "'"


def build_sql(rows: list[dict]) -> str:
    statements = [
        "begin;",
    ]
    for row in rows:
        statements.append(
            """
insert into services (
  service_id,
  description,
  owner,
  github_repo,
  production_branch,
  newrelic_app_name,
  newrelic_app_id,
  newrelic_entity_guid,
  k8s_cluster,
  k8s_namespace,
  k8s_workload_kind,
  k8s_workload_name,
  k8s_label_selector,
  tags
) values (
  {service_id},
  {description},
  {owner},
  {github_repo},
  {production_branch},
  {newrelic_app_name},
  {newrelic_app_id},
  {newrelic_entity_guid},
  {k8s_cluster},
  {k8s_namespace},
  {k8s_workload_kind},
  {k8s_workload_name},
  {k8s_label_selector},
  {tags}
)
on conflict (service_id) do update set
  description = excluded.description,
  owner = excluded.owner,
  github_repo = excluded.github_repo,
  production_branch = excluded.production_branch,
  newrelic_app_name = excluded.newrelic_app_name,
  newrelic_app_id = excluded.newrelic_app_id,
  newrelic_entity_guid = excluded.newrelic_entity_guid,
  k8s_cluster = excluded.k8s_cluster,
  k8s_namespace = excluded.k8s_namespace,
  k8s_workload_kind = excluded.k8s_workload_kind,
  k8s_workload_name = excluded.k8s_workload_name,
  k8s_label_selector = excluded.k8s_label_selector,
  tags = excluded.tags,
  updated_at = now();
""".format(**{key: sql_literal(row.get(key)) for key in [
                "service_id",
                "description",
                "owner",
                "github_repo",
                "production_branch",
                "newrelic_app_name",
                "newrelic_app_id",
                "newrelic_entity_guid",
                "k8s_cluster",
                "k8s_namespace",
                "k8s_workload_kind",
                "k8s_workload_name",
                "k8s_label_selector",
                "tags",
            ]})
        )
    statements.append("commit;")
    return "\n".join(statements)


def main() -> None:
    args = parse_args()
    seed_path = Path(args.services_seed)
    rows = [json.loads(line) for line in seed_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    sql = build_sql(rows)
    subprocess.run(["psql", args.database_url, "-v", "ON_ERROR_STOP=1"], input=sql, text=True, check=True)
    print(f"Seeded {len(rows)} services")


if __name__ == "__main__":
    main()
