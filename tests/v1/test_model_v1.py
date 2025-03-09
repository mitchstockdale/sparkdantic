from typing import Optional

from pydantic.v1 import BaseModel, Field
from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType

from sparkdantic.v1 import SparkModel, create_spark_schema


def test_spark_schema_is_created_for_basemodel():
    class ClassicPydanticModel(BaseModel):
        a: int

    expected_schema = StructType(
        [
            StructField('a', IntegerType(), False),
        ]
    )

    actual_schema = create_spark_schema(ClassicPydanticModel)
    assert actual_schema == expected_schema


def test_models_with_mixed_multiple_inheritance():
    class ParentModel(BaseModel):
        a: int

    class AnotherParentModel(BaseModel):
        b: str

    class InheritedModel(SparkModel, ParentModel, AnotherParentModel):
        c: Optional[str]

    expected_schema = StructType(
        [
            StructField('b', StringType(), False),
            StructField('a', IntegerType(), False),
            StructField('c', StringType(), True),
        ]
    )

    actual_schema = InheritedModel.model_spark_schema()
    assert actual_schema == expected_schema


def test_safe_casting():
    class SafeCastingModel(SparkModel):
        a: int
        b: Optional[int]
        c: Optional[int] = None
        d: Optional[int] = Field(spark_type=StringType)
        e: str = Field(spark_type=IntegerType)
        f: str

    expected_schema = StructType(
        [
            StructField('a', LongType(), False),
            StructField('b', LongType(), True),
            StructField('c', LongType(), True),
            StructField('d', StringType(), True),
            StructField('e', IntegerType(), False),
            StructField('f', StringType(), False),
        ]
    )

    actual_schema = SafeCastingModel.model_spark_schema(safe_casting=True)
    assert actual_schema == expected_schema
