# NOTE: Use this or something similar
from distutils.version import LooseVersion

MINIMUM_PYSPARK_VERSION = '3.3.0'

try:
    import pyspark
except ImportError as raised_error:
    have_pyspark = False
    pyspark_import_error = raised_error
else:
    have_pyspark = True
    pyspark_import_error = None  # type: ignore


def require_minimum_pyspark_version() -> None:
    """Raise ImportError if minimum version of PySpark is not installed"""
    if not have_pyspark:
        raise pyspark_import_error
    else:
        if LooseVersion(pyspark.__version__) < LooseVersion(MINIMUM_PYSPARK_VERSION):
            raise ImportError(
                f'PySpark version {MINIMUM_PYSPARK_VERSION} or newer is required, but found {pyspark.__version__}'
            )
