__version__ = '1.5.0'
__author__ = 'Mitchell Lisle'
__email__ = 'm.lisle90@gmail.com'

import pydantic

if pydantic.VERSION.startswith('1.'):
    from .v1.model import SparkField, SparkModel, create_json_spark_schema, create_spark_schema
else:
    from .model import SparkField, SparkModel, create_json_spark_schema, create_spark_schema

__all__ = [
    'SparkField',
    'SparkModel',
    'create_spark_schema',
    'create_json_spark_schema',
]
