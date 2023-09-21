import asyncio
import logging
import platform

# import csv

# from pathlib import Path
# from shutil import copy2

import asyncio
from aiopath import AsyncPath
from aioshutil import copy2
import aiocsv as csv

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
import tqdm.asyncio

try:
    from rename_by_csv.parse_args import app_arg
except ImportError:
    from parse_args import app_arg

logger: logging


async def get_folder_data(input_folder: AsyncPath) -> dict[str, AsyncPath]:
    result = {}
    if await input_folder.is_dir():
        # result = [item.stem for item in input_folder.glob("*.*")]
        async for input_file in input_folder.glob("*.*"):
            if await input_file.is_file():
                input_file_stem = input_file.stem
                result[input_file_stem] = input_file
    return result


async def get_csv_data(
    input_file: AsyncPath, key_index: int = 0, delimiter=",", encoding="utf-8-sig"
) -> tuple[list[str], dict[str, list[str]]]:
    input_data: dict[str, list[str]] = {}
    input_header: list[str] = []
    if await input_file.is_file():
        async with input_file.open(newline="", encoding=encoding) as csvfile:
            reader = csv.AsyncReader(csvfile, delimiter=delimiter)
            try:
                while not (input_header := await anext(reader)):
                    logger.info(f"reread csv header: {input_header}")
                key: str = ""
                try:
                    async for row in reader:
                        if row:
                            key = AsyncPath(row[key_index]).stem
                            input_data[key] = row
                except IndexError:
                    logger.error(f"{input_file} key index error: {key_index}")
            except StopIteration:
                logger.error(f"Is empty '{input_file}' ?")
    else:
        logger.error(f"File {input_file} is not found")
    return input_header, input_data


async def do_copy(src_path: AsyncPath, output_path: AsyncPath):
    try:
        await copy2(src_path, output_path)
        return src_path.stem
    except OSError as os_error:
        logger.error(f"ERROR: {os_error}")


async def csv_operation(
    input_path: AsyncPath, input_csv_path: AsyncPath, output: AsyncPath
):
    input_files: dict[str, AsyncPath] = await get_folder_data(input_path)
    input_header, input_data = await get_csv_data(input_csv_path)
    # prepare report statistic data
    input_files_len = len(input_files)
    input_records = len(input_data)
    report_txt = f"Files on input folder: {input_files_len}. Records on csv file: {input_records}"
    logger.info(report_txt)
    # save result to csv file
    if input_data:
        await output.mkdir(exist_ok=True, parents=True)
        futures = []
        for key, row in input_data.items():
            filename_src = key
            filename_dst = row[1]
            src_path = input_files.get(filename_src)
            if src_path:
                if await src_path.is_file():
                    new_path = src_path.with_stem(filename_dst)
                    output_path = output.joinpath(new_path.name)
                    logger.debug(f"copy2({src_path}, {output_path})")
                    future = do_copy(src_path, output_path)
                    futures.append(future)
        with logging_redirect_tqdm():
            result = [
                await future for future in tqdm.asyncio.tqdm.as_completed(futures)
            ]
            logger.info(f"{result=}")

    else:
        logger.error("No output data. Nothing to save.")


def check_absolute_path(p: AsyncPath, work: AsyncPath) -> AsyncPath:
    return p if p.is_absolute() else work.joinpath(p)


async def main_async():
    global logger
    args = app_arg()
    logging.basicConfig(
        level=logging.DEBUG if args.get("verbose") else logging.INFO,
        format="%(asctime)s  %(message)s",
    )
    logger = logging.getLogger(__name__)
    work_path = args.get("work")
    input_path = check_absolute_path(args.get("input"), work_path)
    input_csv_path = check_absolute_path(args.get("input_csv"), work_path)
    output_path = check_absolute_path(args.get("output"), work_path)
    await csv_operation(input_path, input_csv_path, output_path)
    logger.info("DONE")


if __name__ == "__main__":
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    try:
        asyncio.run(main_async())
    except Exception as err:
        print(err)
