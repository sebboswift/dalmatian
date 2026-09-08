from __future__ import annotations

import argparse
import math
import shutil
from pathlib import Path

from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Generate a reproducible, scan-sized Parquet benchmark dataset"
    )
    parser.add_argument("target", type=Path)
    parser.add_argument("--target-gib", type=float, default=5.0)
    parser.add_argument(
        "--seed-rows",
        type=int,
        default=100_000,
        help="Rows per copied Parquet file; keep files small enough for constrained executors.",
    )
    args = parser.parse_args()
    target = args.target.resolve()
    if target == Path(target.anchor) or len(target.parts) < 3:
        raise SystemExit("refusing unsafe target path")

    seed = target.with_name(f".{target.name}-seed")
    shutil.rmtree(seed, ignore_errors=True)
    shutil.rmtree(target, ignore_errors=True)
    seed.mkdir(parents=True)
    target.mkdir(parents=True)

    spark = (
        SparkSession.builder.master("local[4]")
        .appName("dalmatian-benchmark-data")
        .getOrCreate()
    )
    try:
        frame = spark.range(0, args.seed_rows, 1, 4).selectExpr(
            "id",
            "concat(sha2(cast(id as string), 256), sha2(concat('b', cast(id as string)), 256)) "
            "AS payload",
        )
        frame.coalesce(1).write.mode("overwrite").parquet(str(seed))
    finally:
        spark.stop()

    part = next(seed.glob("part-*.parquet"))
    part_bytes = part.stat().st_size
    target_bytes = math.ceil(args.target_gib * 1024**3)
    copies = math.ceil(target_bytes / part_bytes)
    for index in range(copies):
        shutil.copyfile(part, target / f"part-{index:05d}.parquet")
    shutil.rmtree(seed)

    actual_bytes = sum(path.stat().st_size for path in target.glob("*.parquet"))
    print(f"files={copies}")
    print(f"rows={copies * args.seed_rows}")
    print(f"bytes={actual_bytes}")
    print(f"gib={actual_bytes / 1024**3:.3f}")


if __name__ == "__main__":
    main()
