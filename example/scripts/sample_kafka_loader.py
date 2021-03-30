# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

"""
This is a example script demonstrating how to load data into Neo4j and
Elasticsearch without using an Airflow DAG.

It contains several jobs:
- `run_csv_job`: runs a job that extracts table data from a CSV, loads (writes)
  this into a different local directory as a csv, then publishes this data to
  neo4j.
- `run_table_column_job`: does the same thing as `run_csv_job`, but with a csv
  containing column data.
- `create_last_updated_job`: creates a job that gets the current time, dumps it
  into a predefined model schema, and publishes this to neo4j.
- `create_es_publisher_sample_job`: creates a job that extracts data from neo4j
  and pubishes it into elasticsearch.

For other available extractors, please take a look at
https://github.com/amundsen-io/amundsendatabuilder#list-of-extractors
"""

import logging
import os
import sys
import uuid

from elasticsearch import Elasticsearch
from pyhocon import ConfigFactory
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from pathlib import Path

from databuilder.extractor.schema_reg_extractor import SchemaRegExtractor
from databuilder.extractor.es_last_updated_extractor import EsLastUpdatedExtractor
from databuilder.extractor.neo4j_search_data_extractor import Neo4jSearchDataExtractor
from databuilder.job.job import DefaultJob
from databuilder.loader.file_system_elasticsearch_json_loader import FSElasticsearchJSONLoader
from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
from databuilder.publisher.elasticsearch_constants import (
    DASHBOARD_ELASTICSEARCH_INDEX_MAPPING, USER_ELASTICSEARCH_INDEX_MAPPING,
)
from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
from databuilder.publisher import neo4j_csv_publisher
from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import ChainedTransformer, NoopTransformer
from databuilder.transformer.dict_to_model import MODEL_CLASS, DictToModel
from databuilder.transformer.generic_transformer import (
    CALLBACK_FUNCTION, FIELD_NAME, GenericTransformer,
)

es_host = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_HOST', 'localhost')
neo_host = os.getenv('CREDENTIALS_NEO4J_PROXY_HOST', 'localhost')

es_port = os.getenv('CREDENTIALS_ELASTICSEARCH_PROXY_PORT', 9200)
neo_port = os.getenv('CREDENTIALS_NEO4J_PROXY_PORT', 7687)
if len(sys.argv) > 1:
    es_host = sys.argv[1]
if len(sys.argv) > 2:
    neo_host = sys.argv[2]

es = Elasticsearch([
    {'host': es_host, 'port': es_port},
])

Base = declarative_base()

NEO4J_ENDPOINT = 'bolt://127.0.0.1:7687'
NEO4j_USERNAME = 'neo4j'
NEO4j_PASSWORD = 'test'

LOGGER = logging.getLogger(__name__)


def run_csv_job():
    tmp_folder = '/var/tmp/amundsen/product-view'
    node_files_folder = Path(tmp_folder, 'nodes')
    relationship_files_folder = Path(tmp_folder, 'relationships')

    kafka_extractor = SchemaRegExtractor()
    csv_loader = FsNeo4jCSVLoader()

    task = DefaultTask(extractor=kafka_extractor,
                       loader=csv_loader)

    job_config = ConfigFactory.from_dict({
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.NODE_DIR_PATH}': node_files_folder,
        f'loader.filesystem_csv_neo4j.{FsNeo4jCSVLoader.RELATION_DIR_PATH}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NODE_FILES_DIR}': node_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.RELATION_FILES_DIR}': relationship_files_folder,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_END_POINT_KEY}': NEO4J_ENDPOINT,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_USER}': NEO4j_USERNAME,
        f'publisher.neo4j.{neo4j_csv_publisher.NEO4J_PASSWORD}': NEO4j_PASSWORD,
        f'publisher.neo4j.{neo4j_csv_publisher.JOB_PUBLISH_TAG}': str(int(datetime.utcnow().timestamp()))
    })

    job = DefaultJob(conf=job_config,
               task=task,
               publisher=Neo4jCsvPublisher())
    
    return job


if __name__ == "__main__":
    job = run_csv_job()
    job.launch()
