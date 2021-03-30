# Copyright Contributors to the Amundsen project.
# SPDX-License-Identifier: Apache-2.0

import importlib
from typing import (
    Any, Dict, Iterator, Union,
)
from pyhocon import ConfigTree
import requests
import json
from databuilder.extractor.base_extractor import Extractor
from databuilder.models.table_metadata import ColumnMetadata, TableMetadata


class SchemaRegExtractor(Extractor):

    def init(self, conf: ConfigTree) -> None:
         self.subjects = requests.get("http://localhost:8081/subjects").json()
         self._extract_iter: Union[None, Iterator] = None
              

    def extract(self) -> Union[TableMetadata, None]:

        if not self._extract_iter:
            self._extract_iter = self._extract_topic_data()
        try:
            result = next(self._extract_iter)
            return result
        except StopIteration:
            return None
        
    def _extract_topic_data(self) -> Iterator[TableMetadata]:
        
        for subject in self.subjects:
            ## not handling versions because cba
            s = requests.get(f"http://localhost:8081/subjects/{subject}/versions/1").json()
            schema = json.loads(s['schema'])
            fields, i = [], 0
            
            ## make this recursive
            
            for key in schema['fields']:
                fields.append(ColumnMetadata(
                    key['name'].replace("-","_"),
                    'a comment',
                    str(key['type']),
                    i
                ))
                i += 1
            
            
            ## need a kafka meta data model
            yield TableMetadata(
                'kafka2',
                'gold',
                'test_schema',
                subject.replace("-","_"),
                'description',
                fields,
                True,
                ["a tag", "second tag"]
            )
            

    def get_scope(self) -> str:
        return 'extractor.generic'
