from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Literal, Optional
from uuid import UUID

from pydantic.v1 import Field, SecretBytes, SecretStr, conint
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DateType,
    DayTimeIntervalType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from sparkdantic.v1 import SparkModel


def test_base_type_fields():
    class BaseTypeModel(SparkModel):
        a: int
        b: float
        c: str
        d: bool
        e: bytes
        f: Decimal
        y: date
        cc: datetime
        gg: timedelta
        hh: DoubleType
        ii: SecretBytes
        jj: SecretStr
        x: str = Field(alias='_x')
        uuid: UUID

    expected_schema = StructType(
        [
            StructField('a', IntegerType(), False),
            StructField('b', DoubleType(), False),
            StructField('c', StringType(), False),
            StructField('d', BooleanType(), False),
            StructField('e', BinaryType(), False),
            StructField('f', DecimalType(10, 0), False),
            StructField('y', DateType(), False),
            StructField('cc', TimestampType(), False),
            StructField('gg', DayTimeIntervalType(0, 3), False),
            StructField('hh', DoubleType(), False),
            StructField('ii', BinaryType(), False),
            StructField('jj', StringType(), False),
            StructField('_x', StringType(), False),
            StructField('uuid', StringType(), False),
        ]
    )

    actual_schema = BaseTypeModel.model_spark_schema(by_alias=True)
    assert actual_schema == expected_schema


def test_literal_fields():
    class ParentModel(SparkModel):
        a: Literal['yes', 'no']

    class ChildModel(ParentModel):
        b: Optional[Literal['yes', 'no']]

    expected_schema = StructType(
        [
            StructField('a', StringType(), False),
            StructField('b', StringType(), True),
        ]
    )

    actual_schema = ChildModel.model_spark_schema()
    assert actual_schema == expected_schema


def test_annotated_fields():
    class AnnotatedModel(SparkModel):
        optional_field: Optional[conint(lt=1, gt=10)]  # type: ignore
        required_field: conint(lt=1, gt=10)  # type: ignore

    expected_schema = StructType(
        [
            StructField('optional_field', IntegerType(), True),
            StructField('required_field', IntegerType(), False),
        ]
    )

    actual_schema = AnnotatedModel.model_spark_schema()
    assert actual_schema == expected_schema


def test_decimal_fields():
    class DecimalModel(SparkModel):
        a: Decimal
        b: Decimal = Field(decimal_places=2)
        c: Decimal = Field(decimal_places=2, max_digits=5)

    expected_schema = StructType(
        [
            StructField('a', DecimalType(10, 0), False),
            StructField('b', DecimalType(10, 2), False),
            StructField('c', DecimalType(5, 2), False),
        ]
    )

    actual_schema = DecimalModel.model_spark_schema()
    assert actual_schema == expected_schema
