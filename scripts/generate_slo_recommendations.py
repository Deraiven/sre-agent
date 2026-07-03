#!/usr/bin/env python3
"""Generate reviewable SLO recommendations from stored metric windows."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sre_agent.slo_recommendations import DEFAULT_SLO_RECOMMENDATION_VERSION, generate_slo_recommendations


DEFAULT_DATABASE_URL = "postgresql://sre_agent:sre_agent@localhost:5432/sre_agent"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL))
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--window-size", default="15m")
    parser.add_argument("--recommendation-version", default=DEFAULT_SLO_RECOMMENDATION_VERSION)
    parser.add_argument("--baseline-version", default="baseline-v1")
    parser.add_argument("--service", action="append", help="Generate only for this service_id. Can be repeated.")
    parser.add_argument("--replace", action="store_true", help="Delete existing pending rows for this recommendation version first.")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = generate_slo_recommendations(
        args.database_url,
        days=args.days,
        window_size=args.window_size,
        recommendation_version=args.recommendation_version,
        baseline_version=args.baseline_version,
        service_ids=args.service,
        replace=args.replace,
        dry_run=args.dry_run,
    )
    if args.dry_run and len(result.get("recommendations") or []) > 10:
        result = {**result, "recommendations": result["recommendations"][:10], "truncated": True}
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
