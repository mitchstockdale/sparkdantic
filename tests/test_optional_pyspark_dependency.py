import pydantic
import pytest

from sparkdantic import model, utils


@pytest.fixture
def no_pyspark(monkeypatch):
    for module in [utils, model]:
        monkeypatch.setattr(module, 'have_pyspark', False)
        monkeypatch.setattr(module, 'pyspark_import_error', ImportError('No module named pyspark'))


def test_create_spark_schema_raises_import_error_when_no_pyspark(no_pyspark):
    class NoPySparkModel(pydantic.BaseModel):
        pass

    with pytest.raises(ImportError) as exc:
        model.create_spark_schema(NoPySparkModel)
    assert 'No module named pyspark' == str(exc.value)


def test_model_spark_schema_raises_import_error_when_no_pyspark(no_pyspark):
    class NoPySparkModel(model.SparkModel):
        pass

    with pytest.raises(ImportError) as exc:
        NoPySparkModel.model_spark_schema()
    assert 'No module named pyspark' == str(exc.value)


def test_no_pyspark_raises_import_error(no_pyspark):
    with pytest.raises(ImportError) as exc:
        utils.require_minimum_pyspark_version()
    assert 'No module named pyspark' == str(exc.value)


def test_lower_pyspark_version_raises_import_error(
    monkeypatch,
):
    low_version = '3.2.0'
    monkeypatch.setattr(utils.pyspark, '__version__', low_version)
    monkeypatch.setattr(
        utils,
        'pyspark_import_error',
        ImportError(f'PySpark version 3.3.0 or newer is required, but found {low_version}'),
    )

    with pytest.raises(ImportError) as exc:
        utils.require_minimum_pyspark_version()
    assert 'PySpark version 3.3.0 or newer is required, but found 3.2.0' == str(exc.value)


@pytest.mark.parametrize(
    'version',
    [
        '3.3.0',
        '3.3.1',
        '3.4.0',
        '4.0.0',
    ],
)
def test_at_least_minimum_pyspark_version_does_not_raise_error(
    version,
    monkeypatch,
):
    monkeypatch.setattr(utils, 'have_pyspark', True)
    monkeypatch.setattr(utils.pyspark, '__version__', version)

    utils.require_minimum_pyspark_version()


# FIXME: This test works standalone, but not when run as a part of the test suite
# def test_no_pyspark_stores_import_error(monkeypatch):
#     import sys
#     monkeypatch.setitem(sys.modules, "pyspark", None)
#     from sparkdantic import model

#     assert model._have_pyspark is False
#     assert isinstance(model._pyspark_import_error, ImportError)
