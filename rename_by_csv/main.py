import logging
import csv

from pathlib import Path
from shutil import copy2
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
import concurrent.futures


try:
    from rename_by_csv.parse_args import app_arg
except ImportError:
    from parse_args import app_arg

logger: logging


def get_folder_data(input_folder: Path) -> dict[str, Path]:
    result = {}
    if input_folder.is_dir():
        # result = [item.stem for item in input_folder.glob("*.*")]
        for input_file in input_folder.glob("*.*"):
            if input_file.is_file():
                input_file_stem = input_file.stem
                result[input_file_stem] = input_file
    return result


def get_csv_data(
    input_file: Path, key_index: int = 0, delimiter=",", encoding="utf-8-sig"
) -> tuple[list[str], dict[str, list[str]]]:
    input_data: dict[str, list[str]] = {}
    input_header: list[str] = []
    if input_file.is_file():
        with input_file.open(newline="", encoding=encoding) as csvfile:
            reader = csv.reader(csvfile, delimiter=delimiter)
            try:
                while not (input_header := next(reader)):
                    logger.info(f"reread csv header: {input_header}")
                key: str = ""
                try:
                    for row in reader:
                        if row:
                            key = Path(row[key_index]).stem
                            input_data[key] = row
                except IndexError:
                    logger.error(f"{input_file} key index error: {key_index}")
            except StopIteration:
                logger.error(f"Is empty '{input_file}' ?")
    else:
        logger.error(f"File {input_file} is not found")
    return input_header, input_data


def do_copy(src_path: Path, output_path: Path):
    try:
        copy2(src_path, output_path)
        return src_path.stem
    except OSError as os_error:
        logger.error(f"ERROR: {os_error}")


def csv_operation(
    input_path: Path,
    input_csv_path: Path,
    output: Path,
    csv_key_idx_src: int = 0,
    csv_key_idx_dst: int = 1,
):
    input_files: dict[str, Path] = get_folder_data(input_path)
    input_header, input_data = get_csv_data(input_csv_path, key_index=csv_key_idx_src)
    # prepare report statistic data
    input_files_len = len(input_files)
    input_records = len(input_data)
    report_txt = f"Files on input folder: {input_files_len}. Records on csv file: {input_records}"
    logger.info(report_txt)
    # copy files to destination with new name
    if input_data:
        output.mkdir(exist_ok=True, parents=True)
        futures = []
        max_workers = None  # automatically  min(32, os.cpu_count() + 4).
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            for key, row in input_data.items():
                filename_src = key
                filename_dst = Path(row[csv_key_idx_dst]).stem
                src_path = input_files.get(filename_src)
                logger.debug(f"{src_path=},{filename_src=}, {filename_dst=}")
                if src_path:
                    if src_path.is_file():
                        new_path = src_path.with_stem(filename_dst)
                        output_path = output.joinpath(new_path.name)
                        # logger.debug(f"create task copy2({src_path}, {output_path})")
                        # create task
                        # do_copy(src_path, output_path)
                        future = pool.submit(do_copy, src_path, output_path)
                        futures.append(future)
            if futures:
                with logging_redirect_tqdm():
                    # Wait result
                    logger.info(
                        f"Wait all copy instances result , total:  {len(futures)}"
                    )
                    result = [
                        future.result()
                        for future in tqdm(
                            concurrent.futures.as_completed(futures), total=len(futures)
                        )
                    ]
                    result = list(filter(lambda x: x is not None, result))
                    result_len = len(result)
                    if result_len != len(futures):
                        r_set = set(result)
                        i_set = set(input_data.keys())
                        diff_set = i_set.difference(r_set)
                        logger.error(f"ERROR. Some files was not copied: {diff_set}")
                    # logger.debug(result)
            else:
                logger.error("ERROR: No data for copy. Nothing to do.")
    else:
        logger.error("ERROR: No output data. Nothing to save.")


def check_absolute_path(p: Path, work: Path) -> Path:
    return p if p.is_absolute() else work.joinpath(p)


def main():
    global logger
    args = app_arg()
    logging.basicConfig(
        level=logging.DEBUG if args.get("verbose") else logging.INFO,
        format="%(asctime)s %(threadName)s  %(message)s",
    )
    logger = logging.getLogger(__name__)
    work_path = args.get("work")
    csv_key_idx_src = args.get("csv_key_idx_src", 0)
    csv_key_idx_dst = args.get("csv_key_idx_dst", 1)
    input_path = check_absolute_path(args.get("input"), work_path)
    input_csv_path = check_absolute_path(args.get("input_csv"), work_path)
    output_path = check_absolute_path(args.get("output"), work_path)
    csv_operation(
        input_path, input_csv_path, output_path, csv_key_idx_src, csv_key_idx_dst
    )
    logger.info("FINISHED")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(err)
