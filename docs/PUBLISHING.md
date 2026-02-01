# Publishing & Release Guide

This guide mirrors the Week 4 "DX & Publishing" milestone from the roadmap. It assumes you have uv installed locally (https://docs.astral.sh/uv/).

## 1. Build the distribution artifacts
```bash
uv build
```
Outputs land in `dist/` as both an sdist (`.tar.gz`) and wheel (`.whl`). Inspect them locally if needed:
```bash
ls dist
uv pip install dist/aqualisys-*.whl --target /tmp/aqualisys-wheel-check
```

## 2. Smoke-test from a clean environment
```bash
python -m venv /tmp/aqualisys-smoke
source /tmp/aqualisys-smoke/bin/activate
pip install --upgrade pip
pip install dist/aqualisys-*.whl
python - <<'PY'
from aqualisys import DataQualityChecker, NotNullRule
print("Loaded", DataQualityChecker, NotNullRule)
PY
```

## 3. Publish to TestPyPI first
```bash
uv publish --repository testpypi --token "$TEST_PYPI_API_TOKEN"
```
Retrieve the token from https://test.pypi.org/manage/account/token/ and store it in the `TEST_PYPI_API_TOKEN` environment variable (or GitHub secret of the same name). After publishing, install from TestPyPI to verify:
```bash
pip install --index-url https://test.pypi.org/simple --no-deps aqualisys
```

## 4. Promote to PyPI
Once TestPyPI validation passes:
```bash
uv publish --token "$PYPI_API_TOKEN"
```

## 5. GitHub Actions workflow
Pushing a tag like `v0.2.0` triggers `.github/workflows/release.yml`, which:
1. Checks out the repo.
2. Sets up Python 3.12 and uv.
3. Runs `uv build`.
4. Uploads `dist/` as a workflow artifact.

If `TEST_PYPI_API_TOKEN` or `PYPI_API_TOKEN` secrets exist, the workflow also attempts to publish. Keep those secrets scoped to org/repo maintainers.
