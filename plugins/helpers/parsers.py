from .mappings import PSRTYPE_MAPPINGS
from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
    FloatType,
    BooleanType,
    TimestampType,
)
from pyspark.sql.types import DoubleType, IntegerType, StringType, DataType
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _parse_resolution_to_timedelta(resolution_column: str) -> str:
    resolutions = {
        "PT60M": "INTERVAL 1 HOUR",
        "P1Y": "INTERVAL 12 MONTH",
        "PT15M": "INTERVAL 15 MINUTES",
        "PT30M": "INTERVAL 30 MINUTES",
        "P1D": "INTERVAL 1 DAY",
        "P7D": "INTERVAL 7 DAY",
        "P1M": "INTERVAL 1 MONTH",
    }
    delta = resolutions.get(resolution_column)
    if delta is None:
        raise NotImplementedError(
            f"Sorry, I don't know what to do with the "
            "resolution '{resolution_column}', because there was no "
            "documentation to be found of this format. "
            "Everything is hard coded. Please open an "
            "issue."
        )
    return delta


# parsing datetime
def parse_datetimeindex(spark, df_ts, df_nonts, tz=None):
    start = df_nonts.select(
        F.to_utc_timestamp(F.col("time_Period_timeInterval_start"), "+07:00")
    ).collect()
    end = df_nonts.select(
        F.to_utc_timestamp(F.col("time_Period_timeInterval_end"), "+07:00")
    ).collect()
    # if tz is not None:
    #     start = df_nonts.select(
    #         F.to_utc_timestamp(F.col("time_Period_timeInterval_start"), tz)
    #     ).collect[0][0]
    #     end = df_nonts.select(
    #         F.to_utc_timestamp(F.col("time_Period_timeInterval_end"), tz)
    #     ).collect()[0][0]
    # print(start)
    # print(end)

    # ambil resolution dan parse
    resolution_col = df_ts.select(F.col("Period_resolution")).collect()[0][0]
    delta = _parse_resolution_to_timedelta(resolution_col)

    # generate date index
    # date_index = spark.createDataFrame([{'date':1}]).select(F.explode(F.sequence(F.lit(start),F.lit(end),F.expr(delta))).alias("ts_index"))
    date_index = spark.sql(
        f"SELECT sequence(to_timestamp('{start}'), to_timestamp('{end}'), {delta}) as ts_index"
    ).withColumn("ts_index", F.explode(F.col("ts_index")))
    # if tz is not None:
    #     #case kalo di parse_timeindex: weekly granularity bakal nambah index element karena ada Daylight Saving Time. Harus di kurangin
    #     #sementara skip dulu
    #     pass
    # generate row number

    date_index = (
        date_index.select("ts_index")
        .distinct()
        .withColumn(
            "position",
            F.expr(
                "ROW_NUMBER() OVER (PARTITION BY '1' ORDER BY ts_index) AS position"
            ),
        )
    )
    # alternatif pake built in function spark
    # w = Window.partitionBy(F.lit(1)).orderBy("ts_index")
    # date_index = (
    #     date_index.select("ts_index")
    #     .distinct()
    #     .withColumn("position", F.row_number().over(w))
    # )
    return date_index


# parsing generation timeseries function
def parse_generation_timeseries(
    spark,
    period_row,
    df_periods,
    df_psrtype,
    df_metric,
    per_plant: bool = False,
    include_eic: bool = False,
):
    # ------------------
    # get name of psrtype
    psrtype = df_psrtype[period_row][0]
    if psrtype is None:
        psrtype = None
    else:
        psrtype_name = PSRTYPE_MAPPINGS[psrtype]
        name = [psrtype_name]

    # check consumption ato aggregated dari nilai inBiddingZone
    # kalo inBiddingZone is none, berarti adanya outbidding zone alias metric = consumption atawa consumption element. kalo inBidding zone is not none, berarti metric = aggregated atawa generation element
    metric_check = df_metric[period_row][0]
    if metric_check is None:
        metric = "Actual Consumption"
    else:
        metric = "Actual Aggregated"
    name.append(metric)

    # skip per plant case
    if per_plant:
        plantname = ""
        if include_eic:
            pass

    # giving the columns set a name (berguna kalo per plantnya kepake aja sih)
    if len(name) == 1:
        name = name[0]
    else:
        name = " - ".join(name)

    # getting the columns for a row of per type generation data and setting up dataframe for the column
    # -----------------
    # getting quantities
    df_periodrow = df_periods[period_row][0]
    datas = [(int(point.position), float(point.quantity)) for point in df_periodrow]
    datas = spark.createDataFrame(datas, ["position", name])

    return datas
