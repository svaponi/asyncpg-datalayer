import os
from contextlib import contextmanager
from unittest import mock

import dotenv


# see https://docs.python.org/3/library/contextlib.html#contextlib.contextmanager
# see also https://adamj.eu/tech/2020/10/13/how-to-mock-environment-variables-with-pytest/
@contextmanager
def mock_environ(
    dotenv_file: str = None,
    **kwargs,
):
    env = {}
    if dotenv_file:
        env.update(dotenv.dotenv_values(dotenv.find_dotenv(dotenv_file)))
    if kwargs:
        env.update(kwargs)
    with mock.patch.dict(os.environ, env):
        yield
