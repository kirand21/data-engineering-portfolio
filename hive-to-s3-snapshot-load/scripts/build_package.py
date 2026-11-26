"""Builds dist/price_load.zip, which the DAG passes to Spark through py_files."""
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def build():
    target = ROOT / "dist" / "price_load.zip"
    target.parent.mkdir(exist_ok=True)
    with zipfile.ZipFile(target, "w", zipfile.ZIP_DEFLATED) as archive:
        for path in sorted((ROOT / "price_load").glob("*.py")):
            archive.write(path, path.relative_to(ROOT))
    return target


if __name__ == "__main__":
    print(build())
