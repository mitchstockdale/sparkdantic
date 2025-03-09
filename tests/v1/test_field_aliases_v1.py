import pytest
from pydantic.v1 import Field
from pyspark.sql.types import IntegerType, StructField, StructType

from sparkdantic.v1 import SparkModel


class NestedAliasModel(SparkModel):
    z: int = Field(alias='_z')


class AliasModel(SparkModel):
    a: int = Field(alias='_a')
    b: int
    _c: int  # Omitted from the model schema
    d: NestedAliasModel = Field(alias='_e')


def test_spark_schema_contains_aliases_by_default():
    expected_schema = StructType(
        [
            StructField('_a', IntegerType(), False),
            StructField('b', IntegerType(), False),
            StructField('_e', StructType([StructField('_z', IntegerType(), False)]), False),
        ]
    )
    actual_schema = AliasModel.model_spark_schema()
    assert actual_schema == expected_schema


def test_spark_schema_contains_field_names_when_not_using_aliases():
    expected_schema = StructType(
        [
            StructField('a', IntegerType(), False),
            StructField('b', IntegerType(), False),
            StructField('d', StructType([StructField('z', IntegerType(), False)]), False),
        ]
    )
    actual_schema = AliasModel.model_spark_schema(by_alias=False)
    assert actual_schema == expected_schema


@pytest.mark.parametrize(
    'by_alias',
    [True, False],
)
def test_spark_model_schema_json_has_same_field_names_to_model_json_schema(by_alias):
    spark_schema = AliasModel.model_spark_schema(by_alias=by_alias)
    json_schema = AliasModel.schema(by_alias=by_alias)

    assert sorted(spark_schema.fieldNames()) == sorted(json_schema['properties'].keys())
