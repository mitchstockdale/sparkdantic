import inspect
from copy import deepcopy
from datetime import date, datetime, timedelta
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Dict,
    Literal,
    Optional,
    Type,
    Union,
    get_args,
    get_origin,
)
from uuid import UUID

from pydantic.v1 import BaseModel, Field, SecretBytes, SecretStr

from sparkdantic import utils
from sparkdantic._type_utils import (
    convert_from_pydantic_types,
    get_enum_mixin_type,
    get_union_type_arg,
    is_optional,
    is_spark_datatype,
)
from sparkdantic.exceptions import TypeConversionError

if utils.have_pyspark:
    utils.require_pyspark_version_in_range()
    from pyspark.sql.types import DataType, StructType

if TYPE_CHECKING:
    from pyspark.sql.types import DataType, StructType

BaseModelOrSparkModel = Union[BaseModel, 'SparkModel']

_type_mapping = MappingProxyType(
    {
        int: 'integer',
        float: 'double',
        str: 'string',
        SecretStr: 'string',
        bool: 'boolean',
        bytes: 'binary',
        SecretBytes: 'binary',
        list: 'array',
        dict: 'map',
        datetime: 'timestamp',
        date: 'date',
        Decimal: 'decimal',
        timedelta: 'interval day to second',
        UUID: 'string',
    }
)


def SparkField(*args, spark_type: Optional[Union[Type['DataType'], str]] = None, **kwargs) -> Field:
    if spark_type is not None:
        kwargs['spark_type'] = spark_type
    return Field(*args, **kwargs)


class SparkModel(BaseModel):
    """A Pydantic V1 BaseModel subclass with model to PySpark schema conversion.

    Methods:
        model_spark_schema: Generates a PySpark schema from the model fields.
        model_json_spark_schema: Generates a PySpark JSON compatible schema from the model fields.
    """

    @classmethod
    def model_spark_schema(
        cls,
        safe_casting: bool = False,
        by_alias: bool = True,
    ) -> 'StructType':
        """Generates a PySpark schema from the model fields. This operates similarly to
        `pydantic.BaseModel.schema()`.

        Args:
            safe_casting (bool): Indicates whether to use safe casting for integer types.
            by_alias (bool): Indicates whether to use attribute aliases or not.

        Returns:
            pyspark.sql.types.StructType: The generated PySpark schema.
        """
        return create_spark_schema(cls, safe_casting, by_alias)

    @classmethod
    def model_json_spark_schema(
        cls,
        safe_casting: bool = False,
        by_alias: bool = True,
    ) -> Dict[str, Any]:
        """Generates a PySpark JSON compatible schema from the model fields. This operates similarly to
        `pydantic.BaseModel.schema()`.

        Args:
            safe_casting (bool): Indicates whether to use safe casting for integer types.
            by_alias (bool): Indicates whether to use attribute aliases or not.

        Returns:
            Dict[str, Any]: The generated PySpark JSON schema.
        """
        return create_json_spark_schema(cls, safe_casting, by_alias)

    class Config:
        arbitrary_types_allowed = True
        use_enum_values = True


def create_json_spark_schema(
    model: Type[BaseModelOrSparkModel],
    safe_casting: bool = False,
    by_alias: bool = True,
) -> Dict[str, Any]:
    """Generates a PySpark JSON compatible schema from the model fields. This operates similarly to
    `pydantic.BaseModel.schema()`.

    Args:
        model (pydantic.BaseModel or SparkModel): The pydantic model to generate the schema from.
        safe_casting (bool): Indicates whether to use safe casting for integer types.
        by_alias (bool): Indicates whether to use attribute aliases (`serialization_alias` or `alias`) or not.

    Returns:
        Dict[str, Any]: The generated PySpark JSON schema
    """
    if not _is_base_model(model):
        raise TypeError('`model` must be of type `SparkModel` or `pydantic.BaseModel`')

    fields = []
    for name, field in model.__fields__.items():
        if by_alias:
            name = field.alias or name

        field_info_extra = field.field_info.extra
        override = field_info_extra.get('spark_type')
        field_type = get_union_type_arg(field.annotation)

        spark_type: Union[str, Dict[str, Any]]

        try:
            if _is_base_model(field_type):
                spark_type = create_json_spark_schema(field_type, safe_casting, by_alias)
            elif override is not None:
                if isinstance(override, str):
                    spark_type = override
                elif utils.have_pyspark and is_spark_datatype(override):
                    spark_type = override.typeName()
                else:
                    msg = '`spark_type` override should be a `str` type name (e.g. long)'
                    if utils.have_pyspark:
                        msg += ' or `pyspark.sql.types.DataType` (e.g. LongType)'
                    msg += f', but got {override}'
                    raise TypeError(msg)
            elif isinstance(field_type, str):
                spark_type = field_type
            elif utils.have_pyspark and is_spark_datatype(field_type):
                spark_type = field_type.typeName()
            else:
                spark_type = _from_python_type(field_type, [field.field_info], safe_casting)
        except Exception as raised_error:
            raise TypeConversionError(
                f'Error converting field `{name}` to PySpark type'
            ) from raised_error

        nullable = is_optional(field.annotation)
        struct_field = {
            'name': name,
            'type': spark_type,
            'nullable': nullable,
            'metadata': {},
        }
        fields.append(struct_field)
    return {
        'type': 'struct',
        'fields': fields,
    }


def create_spark_schema(
    model: Type[BaseModelOrSparkModel],
    safe_casting: bool = False,
    by_alias: bool = True,
) -> 'StructType':
    """Generates a PySpark schema from the model fields.

    Args:
        model (pydantic.BaseModel or SparkModel): The pydantic model to generate the schema from.
        safe_casting (bool): Indicates whether to use safe casting for integer types.
        by_alias (bool): Indicates whether to use attribute aliases or not.

    Returns:
        pyspark.sql.types.StructType: The generated PySpark schema.
    """
    utils.require_pyspark()
    json_schema = create_json_spark_schema(model, safe_casting, by_alias)
    return StructType.fromJson(json_schema)


def _get_spark_type(t: Type) -> str:
    """Returns the corresponding PySpark data type for a given Python type.

    Args:
        t (Type): The Python type to convert to a PySpark data type.

    Returns:
        DataType: The corresponding PySpark data type.

    Raises:
        TypeError: If the type is not recognized in the type map.
    """
    spark_type = _type_mapping.get(t)
    if spark_type is None:
        raise TypeError(f'Type {t} not recognized')
    return spark_type


def _from_python_type(
    type_: Type,
    metadata: list[Any],
    safe_casting: bool = False,
) -> Union[str, Dict[str, Any]]:
    """Converts a Python type to a corresponding PySpark data type.

    Args:
        type_ (Type): The python type to convert to a PySpark data type.
        metadata (list): The metadata for the field.
        safe_casting (bool): Indicates whether to use safe casting for integer types.

    Returns:
        Union[str, Dict[str, Any]]: The corresponding PySpark data type (dict for complex types).
    """
    py_type = get_union_type_arg(type_)

    if _is_base_model(py_type):
        return create_json_spark_schema(py_type, safe_casting)

    args = get_args(py_type)
    origin = get_origin(py_type)

    if origin is None and py_type in (list, dict):
        raise TypeError(f'Type argument(s) missing from {py_type.__name__}')

    # Convert complex types
    if origin is list:
        element_type = _from_python_type(args[0], [])
        contains_null = is_optional(args[0])
        return {
            'type': 'array',
            'elementType': element_type,
            'containsNull': contains_null,
        }

    if origin is dict:
        key_type = _from_python_type(args[0], [])
        value_type = _from_python_type(args[1], [])
        value_contains_null = is_optional(args[1])
        return {
            'type': 'map',
            'keyType': key_type,
            'valueType': value_type,
            'valueContainsNull': value_contains_null,
        }

    if origin is Literal:
        # PySpark doesn't have an equivalent type for Literal. To allow Literal usage with a model we check all the
        # types of values within Literal. If they are all the same, use that type as our new type.
        literal_arg_types = set(map(lambda a: type(a), args))
        if len(literal_arg_types) > 1:
            raise TypeError(
                'Multiple types detected in `Literal` type. Only one consistent arg type is supported.'
            )
        py_type = literal_arg_types.pop()

    if origin is Annotated:
        # first arg of annotated type is the type, second is metadata that we don't do anything with (yet)
        py_type = args[0]

    if issubclass(py_type, Enum):
        py_type = get_enum_mixin_type(py_type)

    py_type = convert_from_pydantic_types(py_type)

    spark_type = _get_spark_type(py_type)

    if safe_casting is True and spark_type == 'integer':
        spark_type = 'long'

    if spark_type == 'decimal':
        meta = None if len(metadata) < 1 else deepcopy(metadata).pop()
        if meta is not None:
            max_digits = meta.max_digits or 10
            decimal_places = meta.decimal_places or 0
        else:
            max_digits = 10
            decimal_places = 0
        return f'decimal({max_digits}, {decimal_places})'

    return spark_type


def _is_base_model(cls: Type) -> bool:
    """Checks if a class is a pydantic.BaseModel.

    Args:
        cls: The class to check.

    Returns:
        bool: True if it is a subclass, otherwise False.
    """
    try:
        return inspect.isclass(cls) and issubclass(cls, BaseModel)
    except TypeError:
        return False
