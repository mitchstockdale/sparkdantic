import nox

nox.options.reuse_existing_virtualenvs = True

PYDANTIC_VERSIONS = ['1.10.17', '2.1.1', '2.10.6']


def install(session: nox.Session, extras: str = '') -> None:
    session.install('pytest')

    if extras:
        session.install(f'.[{extras}]')
    else:
        session.install('.')


@nox.session()
@nox.parametrize('pydantic_version', PYDANTIC_VERSIONS)
def tests(session: nox.Session, pydantic_version: str) -> None:
    """
    Run the test suite against specific dependencies.
    """
    install(session, 'pyspark')
    session.install(f'pydantic=={pydantic_version}')

    if pydantic_version.startswith('1.'):
        session.run('pytest', *session.posargs, 'tests/v1')
    else:
        # Pydantic v2, it should also contain the v1 API, so run all tests
        session.run('pytest', *session.posargs)
