"""Builds dist/catalog_etl.zip, which the DAG passes to Spark through py_files."""
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def build():
    target = ROOT / "dist" / "catalog_etl.zip"
    target.parent.mkdir(exist_ok=True)
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted((ROOT / "catalog_etl").glob("*.py")):
            archive.write(path, path.relative_to(ROOT))
    return target


if __name__ == "__main__":
    print(build())
