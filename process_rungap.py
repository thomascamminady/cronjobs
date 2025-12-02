#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = [
#   "fit2parquets",
#   "fire",
#   "polars",
# ]
# ///
# pyright: reportMissingModuleSource=false
# pyright: reportMissingImports=false

import datetime
import glob
import logging
import os

import fire
import polars as pl
from fit2parquets.parser import Parser


def parse(rungap_source: str, rungap_destination, logging_level: int) -> bool:
    destination_root = os.path.join(rungap_destination, "export")
    has_new_file_been_parsed = False
    parser = Parser()
    files = glob.glob(f"{rungap_source}/*.fit")
    files.sort(reverse=True)
    counter = 0
    for index, file in enumerate(files):
        filename = os.path.basename(file)
        destination = os.path.join(destination_root, filename)
        if not os.path.exists(destination) or len(os.listdir(destination)) == 0:
            try:
                parser.fit2parquets(
                    file,
                    write_to_folder_in_which_fit_file_lives=False,
                    alternate_folder_path=destination,
                )
                logging.info(
                    f"Parsed {index + 1}/{len(files)}: {filename} to {destination}"
                )
                has_new_file_been_parsed = True
                counter += 1
            except Exception as e:
                logging.info(f"Error parsing {filename}: {e}")
    if not has_new_file_been_parsed:
        logging.info("No new files to parse")
    else:
        logging.info(f"Parsed {counter} new files")
    return has_new_file_been_parsed


def merge(rungap_source: str, rungap_destination: str, logging_level: int):
    destination_root = os.path.join(rungap_destination, "export")
    destination_merged = os.path.join(rungap_destination, "merged")
    mesgs = {
        os.path.basename(f).replace(".parquet", "")
        for f in glob.glob(destination_root + "/**/*.parquet")
    }

    for mesg in mesgs:
        df = pl.concat(
            (
                pl.scan_parquet(f).with_columns(file=pl.lit(f.split("/")[-2]))
                for f in glob.glob(destination_root + f"/**/{mesg}.parquet")
            ),
            how="diagonal_relaxed",
        ).collect()
        logging.info(f"Merging {mesg}: Shape {df.shape}")
        df.write_parquet(destination_merged + f"/{mesg}.parquet")


def main(
    rungap_source: str = "/Users/thomascamminady/Library/Mobile Documents/iCloud~com~rungap~RunGap/Documents/Export",
    rungap_destination: str = "/Users/thomascamminady/Data/rungap",
    logging_level: int = logging.DEBUG,
):
    logging.basicConfig(level=logging_level)
    current_timestamp = datetime.datetime.now().isoformat()
    logging.info(f"Process started at {current_timestamp}")
    new_file = parse(rungap_source, rungap_destination, logging_level)
    if new_file:
        logging.info("Merging files...")
        merge(rungap_source, rungap_destination, logging_level)
        logging.info("Merging done.")
    else:
        logging.info("No new files to merge.")


if __name__ == "__main__":
    fire.Fire(main)
