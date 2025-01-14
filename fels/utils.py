# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function
import atexit
import gzip
import logging
import os
import shutil
import sqlite3
from pathlib import Path
from loguru import logger

import ubelt


# Set the default output dir to the XDG or System cache dir
# i.e. ~/.cache/fels $XDG_DATA_HOME/fels %APPDATA%/fels or ~/Library/Caches/fels

FELS_DEFAULT_OUTPUTDIR = os.environ.get('FELS_DEFAULT_OUTPUTDIR', '')
if not FELS_DEFAULT_OUTPUTDIR:
    FELS_DEFAULT_OUTPUTDIR = ubelt.get_app_cache_dir('fels')

GLOBAL_SQLITE_CONNECTIONS = {}


def download_metadata_file(url, outputdir, program, max_age=None):
    """

    :param url:
    :param outputdir:
    :param program:
    :param max_age:
    :return:
    """
    if outputdir is None:
        outputdir = FELS_DEFAULT_OUTPUTDIR
    zipped_index_path = os.path.join(outputdir, 'index_' + program + '.csv.gz')

    if not os.path.isfile(zipped_index_path):
        if not os.path.exists(os.path.dirname(zipped_index_path)):
            os.makedirs(os.path.dirname(zipped_index_path))
        print('Downloading Metadata file...')
        print('url = {!r}'.format(url))
        print('outputdir = {!r}'.format(outputdir))
        ubelt.download(url, fpath=zipped_index_path, chunksize=int(2 ** 22))
    index_path_parquet = os.path.join(outputdir, 'index_' + program + '_parquet')
    index_path_csv = os.path.join(outputdir, 'index_' + program + '.csv')
    index_path_csv_chunks = os.path.join(outputdir, 'index_chunks_' + program)

    if not os.path.isfile(Path(index_path_csv)):

        with gzip.open(zipped_index_path) as gzip_index, open(index_path_csv, 'wb') as f:
            logger.info(f"Unzipping Metadata file to: {gzip_index}")
            shutil.copyfileobj(gzip_index, f)

    if not os.path.isfile(Path(index_path_csv_chunks).joinpath("index_part_00")):
        logger.info(f"Splitting Metadata CSV file into pieces to {index_path_csv_chunks}")
        Path(index_path_csv_chunks).mkdir(parents=True, exist_ok=True)
        os.system(f"split -d -l 3500000 {index_path_csv} {index_path_csv_chunks}/index_part_")

    if not os.path.isfile(Path(index_path_parquet).joinpath("_SUCCESS")):
        logger.info('creating parquet index using spark. see http:/localhost:4040 for the progress. After executing the spark context will be closed')
        from pyspark.sql import SparkSession
        paralelism = 6
        logger.info(f"using parallelism of {paralelism}")

        spark = SparkSession.builder \
          .master(f"local[{paralelism}]") \
          .appName("convert to parquet") \
          .getOrCreate()

        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType, TimestampType

        # setting the schema for spark to read the CSV
        schema = StructType() \
            .add("GRANULE_ID", StringType(), True) \
            .add("PRODUCT_ID", StringType(), True) \
            .add("DATATAKE_IDENTIFIER", StringType(), True) \
            .add("MGRS_TILE", StringType(), True) \
            .add("SENSING_TIME", TimestampType(), True) \
            .add("TOTAL_SIZE", IntegerType(), True) \
            .add("CLOUD_COVER", DoubleType(), True) \
            .add("GEOMETRIC_QUALITY_FLAG", BooleanType(), True) \
            .add("GENERATION_TIME", DateType(), True) \
            .add("NORTH_LAT", DoubleType(), True) \
            .add("SOUTH_LAT", DoubleType(), True) \
            .add("WEST_LON", DoubleType(), True) \
            .add("EAST_LON", DoubleType(), True) \
            .add("BASE_URL", StringType(), True) \


        spark_df = spark.read.format("csv").option("header", False).schema(schema).load(index_path_csv_chunks)
        spark_df.write.mode('overwrite').parquet(index_path_parquet)
        spark.stop()
    return index_path_parquet


def sort_url_list(cc_values, all_acqdates, all_urls):
    """Sort the url list by increasing cc_values and acqdate."""
    cc_values = sorted(cc_values)
    all_acqdates = sorted(all_acqdates, reverse=True)
    all_urls = [x for (y, z, x) in sorted(zip(cc_values, all_acqdates, all_urls))]
    urls = []
    for url in all_urls:
        urls.append('http://storage.googleapis.com/' + url.replace('gs://', ''))
    return urls


def ensure_sqlite_csv_conn(collection_file, fields, table_create_cmd,
                           tablename='unnamed_table1', index_cols=[],
                           overwrite=False):
    """
    Returns a connection to a cache of a csv file
    """
    sql_fpath = collection_file + '.v001.sqlite'
    overwrite = False
    if os.path.exists(sql_fpath):
        sql_stat = os.stat(sql_fpath)
        col_stat = os.stat(collection_file)
        # CSV file has a newer modified time, we have to update
        if col_stat.st_mtime > sql_stat.st_mtime:
            overwrite = True
    else:
        overwrite = True

    stamp_dpath = ubelt.ensuredir((os.path.dirname(collection_file), '.stamps'))
    base_name = os.path.basename(collection_file)

    stamp = ubelt.CacheStamp(base_name, dpath=stamp_dpath, depends=[
        fields, table_create_cmd, tablename], verbose=3
    )
    if stamp.expired():
        overwrite = True

    if overwrite:
        # Update the SQL cache if the CSV file was modified.
        print('Computing (or recomputing) an sql cache')

        ubelt.delete(sql_fpath, verbose=3)
        print('Initial connection to sql_fpath = {!r}'.format(sql_fpath))
        conn = sqlite3.connect(sql_fpath)
        cur = conn.cursor()
        try:
            print('(SQL) >')
            print(table_create_cmd)
            cur.execute(table_create_cmd)

            keypart = ','.join(fields)
            valpart = ','.join('?' * len(fields))
            insert_statement = ubelt.codeblock(
                '''
                INSERT INTO {tablename}({keypart})
                VALUES({valpart})
                ''').format(keypart=keypart, valpart=valpart,
                            tablename=tablename)

            if index_cols:
                index_cols_str = ', '.join(index_cols)
                indexname = 'noname_index'
                # TODO: Can we make an efficient date index with sqlite?
                create_index_cmd = ubelt.codeblock(
                    '''
                    CREATE INDEX {indexname} ON {tablename} ({index_cols_str});
                    ''').format(
                        index_cols_str=index_cols_str, tablename=tablename,
                        indexname=indexname)
                print('(SQL) >')
                print(create_index_cmd)
                _ = cur.execute(create_index_cmd)

            import tqdm
            print('convert to sqlite collection_file = {!r}'.format(collection_file))
            with open(collection_file, 'r') as csvfile:

                # Read the total number of bytes in the CSV file
                csvfile.seek(0, 2)
                total_nbytes = csvfile.tell()

                # Read the header information
                csvfile.seek(0)
                header = csvfile.readline()
                header_nbytes = csvfile.tell()

                # Approximate the number of lines in the file
                # Measure the bytes in the first N lines and take the average
                num_lines_to_measure = 100
                csvfile.seek(0, 2)
                content_nbytes = total_nbytes - header_nbytes
                csvfile.seek(header_nbytes)
                for _ in range(num_lines_to_measure):
                    csvfile.readline()
                first_content_bytes = csvfile.tell() - header_nbytes
                appprox_bytes_per_line = first_content_bytes / num_lines_to_measure
                approx_num_rows = int(content_nbytes / appprox_bytes_per_line)

                # Select the indexes of the columns we want
                csv_fields = header.strip().split(',')
                field_to_idx = {field: idx for idx, field in enumerate(csv_fields)}
                col_indexes = [field_to_idx[k] for k in fields]

                prog = tqdm.tqdm(
                    iter(csvfile),
                    desc='insert csv rows into sqlite cache',
                    total=approx_num_rows, mininterval=1, maxinterval=15,
                    position=0, leave=True,
                )
                # Note: Manual iteration is 1.5x faster than DictReader
                n = 0
                batch_vals = []
                for line in prog:
                    cols = line[:-1].split(',')
                    # Select the values to insert into the SQLite database
                    vals = [cols[idx] for idx in col_indexes]
                    # cur.execute(insert_statement, vals)
                    batch_vals.append(vals)
                    n += 1
                    if n == 50000:
                        cur.executemany(insert_statement, batch_vals)
                        conn.commit()
                        batch_vals = []
                        n = 0

                cur.executemany(insert_statement, batch_vals)
                conn.commit()

        except Exception:
            raise
        else:
            GLOBAL_SQLITE_CONNECTIONS[sql_fpath] = conn
            stamp.renew()
        finally:
            cur.close()

    # cache SQLite connections
    if sql_fpath in GLOBAL_SQLITE_CONNECTIONS:
        conn = GLOBAL_SQLITE_CONNECTIONS[sql_fpath]
    else:
        conn = sqlite3.connect(sql_fpath)
        GLOBAL_SQLITE_CONNECTIONS[sql_fpath] = conn

    return conn


@atexit.register
def _close_global_conns():
    for conn in GLOBAL_SQLITE_CONNECTIONS.values():
        conn.close()
    GLOBAL_SQLITE_CONNECTIONS.clear()
