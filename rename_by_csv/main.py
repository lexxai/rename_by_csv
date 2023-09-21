import logging
import csv
from pathlib import Path
from shutil import copy2
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm


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


def csv_operation(input_path: Path, input_csv_path: Path, output: Path):
    input_files: dict[str, Path] = get_folder_data(input_path)
    input_header, input_data = get_csv_data(input_csv_path)
    # prepare report statistic data
    input_files_len = len(input_files)
    input_records = len(input_data)
    report_txt = f"Files on input folder: {input_files_len}. Records on csv file: {input_records}"
    logger.info(report_txt)
    # save result to csv file
    if input_data:
        output.mkdir(exist_ok=True, parents=True)

        try:
            with logging_redirect_tqdm():
                for key, row in tqdm(input_data.items()):
                    filename_src = key
                    filename_dst = row[1]
                    src_path = input_files.get(filename_src)
                    if src_path:
                        if src_path.is_file():
                            new_path = src_path.with_stem(filename_dst)
                            output_path = output.joinpath(new_path.name)
                            logger.debug(f"copy2({src_path}, {output_path})")
                            copy2(src_path, output_path)
        except OSError as err:
            logger.error(f"ERROR {err}")

    else:
        logger.error("No output data. Nothing to save.")


def check_absolute_path(p: Path, work: Path) -> Path:
    return p if p.is_absolute() else work.joinpath(p)


def main():
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
    csv_operation(input_path, input_csv_path, output_path)
    logger.info("DONE")


if __name__ == "__main__":
    try:
        main()
    except Exception as err:
        print(err)
