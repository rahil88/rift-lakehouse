"""Smoke test: every pipeline package imports. Keeps `make test` meaningful from Phase 0."""

import importlib

import pytest


@pytest.mark.parametrize("package", ["collector", "consumer", "quality"])
def test_package_imports(package):
    module = importlib.import_module(package)
    assert module.__doc__, f"{package} should describe its role in a module docstring"
