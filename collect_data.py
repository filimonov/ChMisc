#!/usr/bin/env python3
import logging
from chmisc.tagutils import TagUtils
from chmisc.chpod import ChPod
from chmisc.xml2csv import XMLConfigToCSVConverter
from os.path import exists, dirname, join as path_join
from os import makedirs
from shutil import rmtree


logging.basicConfig(
    format='[%(asctime)s][%(thread)d - %(threadName)s][%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level='INFO'
)

logger = logging.getLogger('c_s_f')

tutils = TagUtils('./ch_repos_tags.csv')

output_dir = path_join(dirname(__file__), 'output2')

table_names = (
    'tables',
    'aggregate_function_combinators',
    'build_options',
    'collations',
    'data_type_families',
    'formats',
    'functions',
    'table_engines',
    'table_functions',
    'settings',
    'settings_changes',
    'merge_tree_settings',
    'licenses',
    'asynchronous_metrics',
    'events',
    'metrics',
    'server_settings',
    'privileges',
    'columns',
    'errors'
)

image = tutils.images[0]

for image in tutils.images:
    c_path = path_join(output_dir, image.split(':')[1])
    if exists(c_path):
        logger.info(f'Skipping {image} - output directory exists')
    else:
        try:
            logger.info(f'Starting podman for image {image}')

            ch = ChPod(image)

            version = ch.get_version()
            if version is None:
                logger.error(f'Failed to get version for {image}')
                continue

            logger.info(f'Version reported by container {version}')

            makedirs(c_path)

            converter = XMLConfigToCSVConverter()

            configs = ch.get_preprocessed_configs()

            for file_name, xml_raw in configs.items():
                out_file_name = path_join(c_path, f'config_{file_name}.tsv')
                logger.info(f'Dumping the config file: {file_name} into {out_file_name}')
                converter.dump_xml(version, xml_raw, out_file_name)

            ch.query('SYSTEM FLUSH LOGS')

            extra_params = {}
            if ch.is_version_newer_than("20.9.2.20"):
                extra_params['system_events_show_zero_values'] = '1'
            else:
                extra_params['log_queries'] = '1'

            for table in table_names:
                logger.info(f'Collecting table system.{table} data for {image}')
                status, data = ch.query(f'SELECT version() AS ch_version, *, rowNumberInAllBlocks() as order FROM system.{table} FORMAT TabSeparatedWithNamesAndTypes', extra_params=extra_params)
                if status:
                    out_file_name = path_join(c_path, f'system_{table}.csv')
                    with open(out_file_name, 'w') as f:
                        f.writelines(data)
                else:
                    logger.warning(f'Failed to extract data from system.{table} for {image}: {data}')

            logger.info(f'Finished work for {image}')
        except Exception as e:
            logger.error(f'Failure to collect data for {image}: {e}')

            rmtree(c_path, ignore_errors=True)

logger.info('Good bye, cruel world! My work is done.')
