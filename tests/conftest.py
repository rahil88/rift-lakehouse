import pytest

from tests.factories import FakeClock


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()
