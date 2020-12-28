import pytest


def test():
    with pytest.raises(Exception) as expected:
        import presto
    assert "pip install trino" in str(expected.value)
