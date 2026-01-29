"""
CLI entry point for the ML data pipeline.

Usage:
    python -m ml_pipeline convert              # local Parquet only
    python -m ml_pipeline run                  # convert + upload + track
    python -m ml_pipeline run --sources stock_loan,earnings
    python -m ml_pipeline query "SELECT * FROM stock_loan LIMIT 10"
    python -m ml_pipeline status               # check connectivity
    python -m ml_pipeline top-shorts           # quick analytics
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)


def cmd_convert(args):
    from ml_pipeline.pipeline import MLPipeline

    pipe = MLPipeline()
    sources = args.sources.split(",") if args.sources else None
    result = pipe.convert_only(sources)
    for src, path in result.items():
        print(f"  {src:20s} → {path}")


def cmd_run(args):
    from ml_pipeline.pipeline import MLPipeline

    pipe = MLPipeline()
    sources = args.sources.split(",") if args.sources else None
    result = pipe.run(sources=sources, upload=not args.no_upload, track=not args.no_track)
    print(json.dumps(
        {k: str(v) if not isinstance(v, (dict, str)) else v for k, v in result.items()},
        indent=2,
    ))


def cmd_query(args):
    from ml_pipeline.query import QueryEngine

    engine = QueryEngine()
    engine.register_sources()
    df = engine.sql(args.sql)
    print(df.to_string(index=False))


def cmd_status(args):
    from ml_pipeline.pipeline import MLPipeline

    pipe = MLPipeline()
    status = pipe.status()
    print(json.dumps(status, indent=2, default=str))


def cmd_top_shorts(args):
    from ml_pipeline.query import QueryEngine

    engine = QueryEngine()
    df = engine.top_short_borrow(n=args.n)
    print(df.to_string(index=False))


def cmd_ticker(args):
    from ml_pipeline.query import QueryEngine

    engine = QueryEngine()
    df = engine.ticker_history(args.symbol)
    print(df.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(
        prog="ml_pipeline",
        description="ML Data Pipeline — convert, upload, query trading data",
    )
    sub = parser.add_subparsers(dest="command")

    # convert
    p = sub.add_parser("convert", help="Convert raw data to local Parquet")
    p.add_argument("--sources", default=None, help="Comma-separated source names")

    # run
    p = sub.add_parser("run", help="Full pipeline: convert → upload → track metadata")
    p.add_argument("--sources", default=None, help="Comma-separated source names")
    p.add_argument("--no-upload", action="store_true", help="Skip cloud upload")
    p.add_argument("--no-track", action="store_true", help="Skip Supabase metadata")

    # query
    p = sub.add_parser("query", help="Run SQL on Parquet data")
    p.add_argument("sql", help="SQL query string")

    # status
    sub.add_parser("status", help="Check pipeline status and connectivity")

    # top-shorts
    p = sub.add_parser("top-shorts", help="Top N highest fee-rate shorts")
    p.add_argument("-n", type=int, default=50)

    # ticker
    p = sub.add_parser("ticker", help="Full history for a ticker")
    p.add_argument("symbol", help="Ticker symbol (e.g. AAPL)")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    commands = {
        "convert": cmd_convert,
        "run": cmd_run,
        "query": cmd_query,
        "status": cmd_status,
        "top-shorts": cmd_top_shorts,
        "ticker": cmd_ticker,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
