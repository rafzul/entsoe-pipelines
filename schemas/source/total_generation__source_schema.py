StructType(
    [
        StructField(
            "GL_MarketDocument",
            StructType(
                [
                    StructField("xmlns", StringType(), True),
                    StructField(
                        "TimeSeries",
                        ArrayType(
                            StructType(
                                [
                                    StructField(
                                        "MktPSRType",
                                        StructType(
                                            [StructField("psrType", StringType(), True)]
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        "Period",
                                        StructType(
                                            [
                                                StructField(
                                                    "Point",
                                                    ArrayType(
                                                        StructType(
                                                            [
                                                                StructField(
                                                                    "position",
                                                                    IntegerType(),
                                                                    True,
                                                                ),
                                                                StructField(
                                                                    "quantity",
                                                                    DoubleType(),
                                                                    True,
                                                                ),
                                                            ]
                                                        ),
                                                        True,
                                                    ),
                                                    True,
                                                ),
                                                StructField(
                                                    "resolution", StringType(), True
                                                ),
                                                StructField(
                                                    "timeInterval",
                                                    StructType(
                                                        [
                                                            StructField(
                                                                "end",
                                                                TimestampType(),
                                                                True,
                                                            ),
                                                            StructField(
                                                                "start",
                                                                TimestampType(),
                                                                True,
                                                            ),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    StructField("businessType", StringType(), True),
                                    StructField("curveType", StringType(), True),
                                    StructField(
                                        "inBiddingZone_Domain_mRID",
                                        StructType(
                                            [
                                                StructField("text", StringType(), True),
                                                StructField(
                                                    "codingScheme", StringType(), True
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    StructField("mRID", StringType(), True),
                                    StructField(
                                        "objectAggregation", StringType(), True
                                    ),
                                    StructField(
                                        "outBiddingZone_Domain_mRID",
                                        StructType(
                                            [
                                                StructField("text", StringType(), True),
                                                StructField(
                                                    "codingScheme", StringType(), True
                                                ),
                                            ]
                                        ),
                                        True,
                                    ),
                                    StructField(
                                        "quantity_Measure_Unit_name", StringType(), True
                                    ),
                                ]
                            ),
                            True,
                        ),
                        True,
                    ),
                    StructField("createdDateTime", TimestampType(), True),
                    StructField("mRID", StringType(), True),
                    StructField("process_processType", StringType(), True),
                    StructField(
                        "receiver_MarketParticipant_mRID",
                        StructType(
                            [
                                StructField("text", StringType(), True),
                                StructField("codingScheme", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "receiver_MarketParticipant_marketRole_type", StringType(), True
                    ),
                    StructField("revisionNumber", IntegerType(), True),
                    StructField(
                        "sender_MarketParticipant_mRID",
                        StructType(
                            [
                                StructField("text", StringType(), True),
                                StructField("codingScheme", StringType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField(
                        "sender_MarketParticipant_marketRole_type", StringType(), True
                    ),
                    StructField(
                        "time_Period_timeInterval",
                        StructType(
                            [
                                StructField("end", TimestampType(), True),
                                StructField("start", TimestampType(), True),
                            ]
                        ),
                        True,
                    ),
                    StructField("type", StringType(), True),
                ]
            ),
            True,
        )
    ]
)
