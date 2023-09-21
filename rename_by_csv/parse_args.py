import argparse
import os
import sys
from pefile import PE

# from pathlib import Path
from aiopath import AsyncPath as Path

if sys.version_info >= (3, 8):
    from importlib.metadata import version
else:
    from importlib_metadata import version

OPENCV = True


def get_version_pe():
    if getattr(sys, "frozen", False):
        pe = PE(sys.executable)
        if "VS_FIXEDFILEINFO" not in pe.__dict__:
            print("ERROR: Oops, has no version info. Can't continue.")
            return None
        if not pe.VS_FIXEDFILEINFO:
            print("ERROR: VS_FIXEDFILEINFO field not set for. Can't continue.")
            return None
        verinfo = pe.VS_FIXEDFILEINFO[0]
        # print(verinfo)
        filever = (
            verinfo.FileVersionMS >> 16,
            verinfo.FileVersionMS & 0xFFFF,
            verinfo.FileVersionLS >> 16,
            # verinfo.FileVersionLS & 0xFFFF,
        )
        return "{}.{}.{}".format(*filever)


def get_version():
    try:
        version_str = version(__package__)
        # print(f"{version_str=}")
    except Exception:
        version_str = get_version_pe()
        if version_str is None:
            version_str = "undefined"
    pack = __package__ if __package__ else Path(sys.executable).name
    return f"Version: '{version_str}', package: {pack}"


def app_arg():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "-V",
        "--version",
        action="version",
        version=get_version(),
        help="show version of app",
    )
    ap.add_argument(
        "--work",
        help="Directory for work. Is prefix for all other directories that is not absolute, default ''",
        default="",
        type=Path,
    )
    ap.add_argument("--input", help="Path to input folder", required=True, type=Path)
    ap.add_argument(
        "--input_csv", help="Path to input_csv file", required=True, type=Path
    )
    ap.add_argument(
        "--output",
        help="Path for output folder, default 'output'",
        default="output",
        type=Path,
    )
    ap.add_argument(
        "--verbose",
        help="verbose output",
        action="store_true",
    )
    args = vars(ap.parse_args())
    # print(f"{args=}")
    return args
