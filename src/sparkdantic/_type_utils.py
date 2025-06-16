import inspect
import sys
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Type, Union, get_args, get_origin

from sparkdantic import utils

if utils.have_pyspark:
    utils.require_pyspark_version_in_range()
    from pyspark.sql.types import DataType

if TYPE_CHECKING:
    from pyspark.sql.types import DataType

if sys.version_info > (3, 10):
    from types import UnionType  # pragma: no cover
else:
    UnionType = Union  # pragma: no cover

if sys.version_info > (3, 11):
    from enum import EnumType  # pragma: no cover
else:
    EnumType = Type[Enum]  # pragma: no cover

MixinType = Union[Type[int], Type[str]]


def get_enum_mixin_type(t: EnumType) -> MixinType:
    """Returns the mixin type of an Enum.

    Args:
        t (EnumType): The Enum to get the mixin type from.

    Returns:
        MixinType: The type mixed with the Enum.

    Raises:
        TypeError: If the mixin type is not supported (int and str are supported).
    """
    if issubclass(t, int):
        return int
    elif issubclass(t, str):
        return str
    else:
        raise TypeError(f'Enum {t} is not supported. Only int and str mixins are supported.')


def is_optional(t: Type) -> bool:
    """Determines if a type is optional.

    Args:
        t (Type): The Python type to check for nullability.

    Returns:
        bool: a boolean indicating nullability.
    """
    return get_origin(t) in (Union, UnionType) and type(None) in get_args(t)


def get_union_type_arg(t: Type) -> Type:
    """Returns the inner type from a Union or the type itself if it's not a Union.

    Args:
        t (Type): The Union type to get the inner type from.

    Returns:
        Type: The inner type or the original type.
    """
    return get_args(t)[0] if get_origin(t) in (Union, UnionType) else t


def convert_from_pydantic_types(t: Type) -> Type:
    """Converts pydantic.v1.types to python types.

    These typically include constrained types e.g. conint.

    Args:
        t (Type): The pydantic.v1 type to convert.

    Returns:
        Type: The converted Python type.
    """
    if issubclass(t, bool):
        return bool
    if issubclass(t, int):
        return int
    if issubclass(t, float):
        return float
    if issubclass(t, str):
        return str
    if issubclass(t, bytes):
        return bytes
    if issubclass(t, Decimal):
        return Decimal
    return t


def is_spark_datatype(t: Type) -> bool:
    """Determines if a type is a PySpark DataType.

    Args:
        t (Type): The Python type to check.

    Returns:
        bool: a boolean indicating if the type is a PySpark DataType.
    """
    return inspect.isclass(t) and issubclass(t, DataType)
