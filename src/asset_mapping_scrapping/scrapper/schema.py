import pandera as pa
import numpy as np
import datetime

base_schema: dict = {
    "id": pa.Column(object, required=False),
    "latitude": pa.Column(
        float,
        [
            pa.Check.in_range(
                -90,
                90,
                name="latitude_range",
                title="Latitude range check",
                error="Latitudes in EPSG:4326 coordinate system should be between -90 and 90 degrees.",
            ),
            pa.Check(lambda s: s.isna().sum() / len(s) < 0.25),
        ],
        nullable=True,
        required=False,
        coerce=True,
    ),
    "longitude": pa.Column(
        float,
        [
            pa.Check.in_range(
                -180,
                180,
                name="longitude_range",
                title="Longitude range check",
                error="Longitudes in EPSG:4326 coordinate system should be between -180 and 180 degrees.",
            ),
            pa.Check(lambda s: s.isna().sum() / len(s) < 0.25),
        ],
        nullable=True,
        required=False,
        coerce=True,
    ),
    "address": pa.Column(
        str,
        pa.Check(
            lambda s: s.isna().sum() / len(s) < 0.25,
            name="missing_address",
            title="Percentage of missing address",
            error="More than 25% of values in column address are missing",
            raise_warning=True,
        ),
        nullable=True,
        required=False,
    ),
    "country": pa.Column(
        str,
        pa.Check(
            lambda s: s.isna().sum() / len(s) < 0.25,
            name="missing_country",
            title="Percentage of missing country",
            error="More than 25% of values in column country are missing",
            raise_warning=True,
        ),
        nullable=True,
        required=False,
    ),
    "state": pa.Column(
        str,
        pa.Check(
            lambda s: s.isna().sum() / len(s) < 0.25,
            name="missing_state",
            title="Percentage of missing state",
            error="More than 25% of values in column state are missing",
            raise_warning=True,
        ),
        nullable=True,
        required=False,
    ),
    "city": pa.Column(
        str,
        pa.Check(
            lambda s: s.isna().sum() / len(s) < 0.25,
            name="missing_city",
            title="Percentage of missing city",
            error="More than 25% of values in column city are missing",
            raise_warning=True,
        ),
        nullable=True,
        required=False,
    ),
    "asset_name": pa.Column(str, nullable=False, required=True),
    "type": pa.Column(str, nullable=False, required=False),
    "subtype": pa.Column(str, nullable=False, required=False),
    "status": pa.Column(
        str,
        pa.Check(
            lambda s: s.isna().sum() / len(s) < 0.25,
            name="missing_status",
            title="Percentage of missing status",
            error="More than 25% of values in column status are missing",
            raise_warning=True,
        ),
        nullable=True,
        required=False,
    ),
    "operator": pa.Column(str, nullable=True, required=False),
    "acquisition_date": pa.Column(pa.DateTime, nullable=True, required=False),
    "company_name": pa.Column(str, nullable=True, required=True),
}

base_checks: list = [
    pa.Check(
        lambda df: ("latitude" in df.columns and "longitude" in df.columns)
        or np.all(
            [col in df.columns for col in ["address", "country", "state", "city"]]
        ),
        name="location_data",
        title="Location columns availability",
        error="Not all columns required to locate the asset are available. Please provide either (latitude, longitude) or (address, city, state, country)",
    ),  # check that there is essential data about location
    pa.Check(
        lambda df: np.all(df.isna().sum() / len(df) < 0.25),
        name="total_missing_values",
        title="Total missing values",
        error="Some columns have more than 25% missing values. Please check that this is not a collection issue",
        raise_warning=True,
    ),  # check the proportion of missing values
    pa.Check(
        lambda df: ("type" in df.columns or "subtype" in df.columns),
        name="type_data",
        title="Type identification data",
        error="No column was provided to describe the type of the asset. Please provide a type or subtype column",
    ),
]

real_estate_schema = pa.DataFrameSchema(
    {
        **base_schema,
        "sector": pa.Column(
            str, pa.Check.equal_to("Real Estate"), nullable=False, required=True
        ),
        "area": pa.Column(
            float,
            [
                pa.Check(
                    lambda s: s.isna().sum() / len(s) < 0.25,
                    name="missing_area",
                    title="Percentage of missing area",
                    error="More than 25% of values in column area are missing",
                    raise_warning=True,
                ),
                pa.Check.greater_than(0),
            ],
            nullable=True,
            required=False,
            coerce=True,
        ),
        "unit": pa.Column(str, nullable=True, required=False),
        "ownership": pa.Column(
            float,
            pa.Check.in_range(
                0,
                100,
                name="ownership_pct_range",
                title="Ownership percentage check",
                error="Ownership percentage should be between float 0 and 100.",
            ),
            nullable=True,
            required=False,
        ),
        "pct_occupied": pa.Column(
            float,
            pa.Check.in_range(
                0,
                100,
                name="pct_occupied_range",
                title="Occupation rate check",
                error="Occupation rate should be between float 0 and 100.",
            ),
            nullable=True,
            required=False,
        ),
    },
    checks=base_checks
    + [
        pa.Check(
            lambda df: ("unit" in df.columns and "area" in df.columns)
            or not ("unit" in df.columns or "area" in df.columns),
            name="area_and_unit",
            title="Area and unit columns",
            error="Area column needs to be provided with a unit column.",
        ),  # area is not provided without unit
    ],
    strict=True,
)

mining_schema = pa.DataFrameSchema(
    {
        **base_schema,
        "sector": pa.Column(
            str, pa.Check.equal_to("Mining"), nullable=False, required=True
        ),
        "commodity": pa.Column(
            str, nullable=True
        ),  # l check if value strictly contains letters
        "product": pa.Column(str, nullable=True),
        "unit": pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        "year": pa.Column(
            float,
            pa.Check.in_range(1800, datetime.date.today().year),
            nullable=True,
            coerce=True,
        ),
        "capacity": pa.Column(float, nullable=True, coerce=True),
        "extracted_amount": pa.Column(float, nullable=True, coerce=True),
        "reserves": pa.Column(float, nullable=True, coerce=True),
    },
    checks=base_checks,
    strict=True,
)

production_schema: pa.DataFrameSchema = pa.DataFrameSchema(
    {
        **base_schema,
        "sector": pa.Column(
            str,
            pa.Check.isin(["Construction", "Metals", "Oil and Gas"]),
            nullable=False,
            required=True,
        ),
        "production": pa.Column(float, nullable=True, coerce=True),
        "commodity": pa.Column(
            str, nullable=True
        ),  # l check if value strictly contains letters
        "product": pa.Column(str, nullable=True),
        "unit": pa.Column(
            str,
            nullable=True,
            coerce=True,
        ),
        "year": pa.Column(
            float,
            pa.Check.in_range(1800, datetime.date.today().year),
            nullable=True,
            coerce=True,
        ),
        "capacity": pa.Column(float, nullable=True, coerce=True),
        "value": pa.Column(float, nullable=True, coerce=True),
    },
    checks=base_checks,
)

sector_schema_mapping = {
    "Real Estate": real_estate_schema,
    "Mining": mining_schema,
    "Construction": production_schema,
}
