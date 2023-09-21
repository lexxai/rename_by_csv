import pyinstaller_versionfile
from configparser import ConfigParser
import sys
from pathlib import Path


parser = ConfigParser()
# read config file
description = ""
ver = "0.0.1"
config_setup = Path("..\setup.cfg")
if config_setup.is_file():
    parser.read(config_setup)
    ver = parser["metadata"].get("version", "0.0.1")
    description = parser["metadata"].get("description", "").strip('"')
    name = parser["metadata"].get("name", "").strip('"')
else:
    config_setup = Path("../rename_by_csv/pyproject.toml")
    if config_setup.is_file():
        parser.read(config_setup)
        ver = parser["tool.poetry"].get("version", "0.0.1").strip('"')
        description = parser["tool.poetry"].get("description", "").strip('"')
        name = parser["tool.poetry"].get("name", "").strip('"')

if len(sys.argv) > 1:
    filename = Path(sys.argv[1])
    if filename.is_file():
        new_fn = filename.with_stem(f"{filename.stem}_{ver}")
        filename.rename(new_fn)
else:
    pyinstaller_versionfile.create_versionfile(
        output_file="../rename_by_csv/versionfile.txt",
        version=f"{ver}.0",
        company_name="lexxai",
        file_description=description,
        internal_name=name,
        legal_copyright="https://github.com/lexxai",
        original_filename=f"{name}_{ver}.exe",
        product_name=name,
    )
    print(f"Done: versionfile.txt in parent folder. version='{ver}.0'")
