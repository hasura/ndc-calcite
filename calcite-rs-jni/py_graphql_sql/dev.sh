#!/bin/bash

case $1 in
    "test")
        poetry run pytest
        ;;
    "format")
        poetry run black .
        poetry run isort .
        ;;
    "lint")
        poetry run pylint py_graphql_sql/ tests/
        ;;
    "typecheck")
        poetry run mypy py_graphql_sql/
        ;;
    "check")
        echo "Running all checks..."
        poetry run black . --check
        poetry run isort . --check
        poetry run pylint py_graphql_sql/ tests/
        poetry run mypy py_graphql_sql/
        poetry run pytest --cov=py_graphql_sql --cov-report=term-missing
        ;;
    *)
        echo "Usage: ./dev.sh [test|format|lint|typecheck|check]"
        exit 1
        ;;
esac
