# Release Checklist

Use this checklist for each package release.

## 1. Update Version

Choose one:

```bash
poetry version patch
# or
poetry version minor
# or
poetry version major
```

## 2. Validate Before Publish

Run the full test suite:

```bash
poetry run all-tests
```

Run package build and metadata checks:

```bash
poetry run package-check
```

## 3. Publish

Recommended first pass to TestPyPI:

```bash
poetry publish -r testpypi
```

Then publish to PyPI:

```bash
poetry publish -r pypi
```

## 4. Tag Source

Replace `X.Y.Z` with the new version:

```bash
git add pyproject.toml poetry.lock
git commit -m "release: vX.Y.Z"
git tag vX.Y.Z
git push origin main --tags
```
