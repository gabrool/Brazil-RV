# Engineering guidelines

This project is hardware constrained. Speed, memory use, disk use, and code simplicity are first-class requirements.

## Defaults

- Use chunked parsers for large files.
- Store parsed tables as Parquet.
- Prefer `polars`, `pyarrow`, and `duckdb` for large tabular work.
- Use `pandas` only for small data or when it is clearly simpler.
- Avoid reading full historical datasets when a partitioned scan is enough.
- Partition large outputs by source, dataset, year, and date where useful.
- Make ingestion steps resumable and idempotent.
- Keep test fixtures small.
- Do not commit downloaded data.

## Lean-code rule

Every line of code should justify itself.

Avoid speculative abstractions, legacy compatibility that has not been requested, duplicate utilities, notebook-only production logic, and silent error handling that hides data problems.

Start with the smallest correct implementation that preserves point-in-time safety, manifests, schemas, tests, and reproducibility.

## Dependency discipline

Dependencies must earn their place. Do not add heavy ML, orchestration, or database dependencies during B3 ingestion unless the task directly requires them.
