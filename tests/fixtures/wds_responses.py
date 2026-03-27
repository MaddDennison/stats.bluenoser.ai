"""Sample StatsCan WDS API responses for offline testing."""

CUBE_METADATA_RESPONSE = [
    {
        "status": "SUCCESS",
        "object": {
            "productId": 18100004,
            "cubeTitleEn": "Consumer Price Index, monthly, not seasonally adjusted",
            "cubeTitleFr": "Indice des prix à la consommation, mensuel, non désaisonnalisé",
            "dimension": [
                {
                    "dimensionPositionId": 1,
                    "dimensionNameEn": "Geography",
                    "member": [
                        {"memberId": 2, "memberNameEn": "Canada", "parentMemberId": None, "terminated": 0},
                        {"memberId": 7, "memberNameEn": "Nova Scotia", "parentMemberId": None, "terminated": 0},
                        {"memberId": 8, "memberNameEn": "Halifax, Nova Scotia", "parentMemberId": None, "terminated": 0},
                    ],
                },
                {
                    "dimensionPositionId": 2,
                    "dimensionNameEn": "Products and product groups",
                    "member": [
                        {"memberId": 2, "memberNameEn": "All-items", "parentMemberId": None, "terminated": 0},
                        {"memberId": 3, "memberNameEn": "Food", "parentMemberId": None, "terminated": 0},
                    ],
                },
            ],
        },
    }
]

VECTORS_LATEST_N_RESPONSE = [
    {
        "status": "SUCCESS",
        "object": {
            "responseStatusCode": 0,
            "productId": 18100004,
            "coordinate": "7.2.0.0.0.0.0.0.0.0",
            "vectorId": 41691513,
            "vectorDataPoint": [
                {
                    "refPer": "2026-02-01",
                    "refPerRaw": "2026-02-01",
                    "value": 169.2,
                    "decimals": 1,
                    "scalarFactorCode": 0,
                    "symbolCode": 0,
                    "statusCode": 0,
                    "releaseTime": "2026-03-16T08:30",
                },
                {
                    "refPer": "2026-01-01",
                    "refPerRaw": "2026-01-01",
                    "value": 168.5,
                    "decimals": 1,
                    "scalarFactorCode": 0,
                    "symbolCode": 0,
                    "statusCode": 0,
                    "releaseTime": "2026-02-17T08:30",
                },
                {
                    "refPer": "2025-02-01",
                    "refPerRaw": "2025-02-01",
                    "value": 166.4,
                    "decimals": 1,
                    "scalarFactorCode": 0,
                    "symbolCode": 0,
                    "statusCode": 0,
                    "releaseTime": "2025-03-18T08:30",
                },
            ],
        },
    },
    {
        "status": "SUCCESS",
        "object": {
            "responseStatusCode": 0,
            "productId": 18100004,
            "coordinate": "2.2.0.0.0.0.0.0.0.0",
            "vectorId": 41690973,
            "vectorDataPoint": [
                {
                    "refPer": "2026-02-01",
                    "value": 165.9,
                    "decimals": 1,
                    "statusCode": 0,
                    "symbolCode": 0,
                    "releaseTime": "2026-03-16T08:30",
                },
                {
                    "refPer": "2026-01-01",
                    "value": 165.0,
                    "decimals": 1,
                    "statusCode": 0,
                    "symbolCode": 0,
                    "releaseTime": "2026-02-17T08:30",
                },
            ],
        },
    },
]

# Response with a suppressed value (statusCode 6 = too unreliable)
VECTORS_WITH_SUPPRESSED = [
    {
        "status": "SUCCESS",
        "object": {
            "vectorId": 99999999,
            "productId": 18100004,
            "coordinate": "7.99.0.0.0.0.0.0.0.0",
            "vectorDataPoint": [
                {
                    "refPer": "2026-02-01",
                    "value": 0,
                    "decimals": 1,
                    "statusCode": 6,
                    "symbolCode": 0,
                    "releaseTime": "2026-03-16T08:30",
                },
            ],
        },
    }
]

CHANGED_CUBE_LIST_RESPONSE = [
    {
        "status": "SUCCESS",
        "object": {
            "productId": 18100004,
            "releaseTime": "2026-03-16T08:30",
        },
    },
    {
        "status": "SUCCESS",
        "object": {
            "productId": 36100434,
            "releaseTime": "2026-03-16T08:30",
        },
    },
    {
        "status": "SUCCESS",
        "object": {
            "productId": 99999999,
            "releaseTime": "2026-03-16T08:30",
        },
    },
]

SERIES_INFO_RESPONSE = [
    {
        "status": "SUCCESS",
        "object": {
            "vectorId": 41691513,
            "productId": 18100004,
            "coordinate": "7.2.0.0.0.0.0.0.0.0",
            "SeriesTitleEn": "Nova Scotia;All-items",
        },
    }
]
