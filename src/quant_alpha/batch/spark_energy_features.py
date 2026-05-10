from __future__ import annotations

from pathlib import Path


def build_spark_session(app_name: str = "second-foundation-energy-batch"):
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def compute_energy_features(
    input_path: str,
    output_path: str,
    output_partitions: int = 1,
) -> None:
    from pyspark.sql import Window, functions as F

    spark = build_spark_session()
    try:
        frame = spark.read.parquet(input_path)
        w_market = Window.partitionBy("market").orderBy("timestamp")
        w_24 = w_market.rowsBetween(-23, 0)
        w_168 = w_market.rowsBetween(-167, 0)

        # Guard log() against zero/negative values; lag can be null on first row
        safe_log = lambda col: F.when(F.col(col) > 0, F.log(F.col(col)))
        lag_spot = F.lag("spot_price").over(w_market)

        enriched = (
            frame.withColumn(
                "spot_return_1h",
                F.when(lag_spot > 0, safe_log("spot_price") - F.log(lag_spot)),
            )
            .withColumn("rolling_spot_mean_24h", F.avg("spot_price").over(w_24))
            .withColumn("rolling_spot_std_24h", F.stddev_pop("spot_price").over(w_24))
            .withColumn("rolling_residual_mean_168h", F.avg("residual_load").over(w_168))
            .withColumn("residual_load_shock", F.col("residual_load") - F.col("rolling_residual_mean_168h"))
            .withColumn("imbalance_premium", F.col("imbalance_price") - F.col("spot_price"))
            .withColumn(
                "scarcity_flag",
                F.when(F.col("residual_load_shock") > 5, F.lit(1)).otherwise(F.lit(0)),
            )
        )

        (
            enriched.coalesce(output_partitions)
            .write.mode("overwrite")
            .parquet(output_path)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[3]
    compute_energy_features(
        str(root / "data/raw/power_market.parquet"),
        str(root / "data/processed/power_market_spark_features.parquet"),
    )
