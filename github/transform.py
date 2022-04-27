#  Copyright (c) 2021.  Atlas of Living Australia
#   All Rights Reserved.
#
#   The contents of this file are subject to the Mozilla Public
#   License Version 1.1 (the "License"); you may not use this file
#   except in compliance with the License. You may obtain a copy of
#   the License at http://www.mozilla.org/MPL/
#
#   Software distributed under the License is distributed on an "AS  IS" basis,
#   WITHOUT WARRANTY OF ANY KIND, either express or
#   implied. See the License for the specific language governing
#   rights and limitations under the License.
import csv
import io
from typing import Dict

import attr
import marshmallow
import requests

from dwc.schema import ExtendedTaxonSchema
from processing.dataset import Port, Dataset, Record, Index, Keys
from processing.node import ProcessingContext
from processing.source import Source

def _stripnewline(s: str) -> str:
    return None if s is None else s.replace('\n', ' ').replace('\r', ' ').strip()

@attr.s
class GithubListSource(Source):
    """Read a species list from github as a CSV file"""
    dialect: str = attr.ib()
    encoding: str = attr.ib(default='utf-8', kw_only=True)

    @classmethod
    def create(cls, id:str, dialect="ala", **kwargs):
        schema = ExtendedTaxonSchema()
        output = Port.port(schema)
        error = Port.error_port(schema)
        return GithubListSource(id, output, error, dialect, **kwargs)

    def execute(self, context: ProcessingContext):
        output = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        fieldmap = { (field.data_key if field.data_key is not None else field.name).lower(): (field.data_key if field.data_key is not None else field.name) for field in self.output.schema.fields.values()}
        fieldmap1 = { field.name.lower(): (field.data_key if field.data_key is not None else field.name) for field in self.output.schema.fields.values()}
        fieldmap.update(fieldmap1)
        url = context.get_default('sourceUrl')
        idstem = 'ALA_' + context.get_default('datasetID').upper()
        list = requests.get(url).text
        with io.StringIO(list) as ifile:
            reader = csv.DictReader(ifile, dialect=self.dialect)
            line = 1
            for row in reader:
                try:
                    if len(row) > 0:
                        row = { fieldmap[k.lower()] : _stripnewline(v) for (k, v) in row.items() if k.lower() in fieldmap }
                        row['taxonID'] = idstem + "_" + str(line)
                        value = Record(line, self.output.schema.load(row), None)
                        if self.predicate is None or self.predicate(value):
                            output.add(value)
                            self.count(self.ACCEPTED_COUNT, value, context)
                except marshmallow.ValidationError as err:
                    err.data['_line'] = line
                    err.data['_messages'] = err.messages
                    error = Record(line, err.data, err.messages)
                    errors.add(error)
                    self.count(self.ERROR_COUNT, error, context)
                self.count(self.PROCESSED_COUNT, None, context)
                line += 1
        context.save(self.output, output)
        context.save(self.error, errors)
