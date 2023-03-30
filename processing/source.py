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

import sys
import csv
from os import path
from typing import Dict, Callable

import attr
import marshmallow
import openpyxl

from processing.dataset import Port, Dataset, Record
from processing.node import Node, ProcessingContext
from processing.transform import Predicate

csv.field_size_limit(sys.maxsize)


@attr.s
class Source(Node):
    output: Port = attr.ib()
    error: Port = attr.ib()
    predicate: Callable[[Record], bool] = attr.ib(default=None, kw_only=True)

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        if self.predicate is not None and isinstance(self.predicate, Predicate):
            inputs.update(self.predicate.outputs())
        return inputs

    def outputs(self) -> Dict[str, Port]:
        outputs = super().outputs()
        outputs['output'] = self.output
        return outputs

    def errors(self) -> Dict[str, Port]:
        errors = super().errors()
        errors['error'] = self.error
        return errors

    def vertex_color(self, context: ProcessingContext):
        return 'lightcyan'


@attr.s(auto_attribs=True)
class NullSource(Source):
    """A source that generates nothing"""

    @classmethod
    def create(cls, id: str, schema: marshmallow.Schema, **kwargs):
        """Create a null source

        :param id: The identifier of the node
        :param schema: The notional schema to use
        :param kwargs: Any keyword arguments
        :return:
        """
        source = Port.port(schema)
        error = Port.error_port(schema)
        return NullSource(id, source, error, **kwargs)

    def execute(self, context: ProcessingContext):
        dataset = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        context.save(self.output, dataset)
        context.save(self.error, errors)


@attr.s(auto_attribs=True)
class CsvSource(Source):
    file: path
    dialect: str
    encoding: str = attr.ib(default='utf-8', kw_only=True)
    comment: str = attr.ib(default='#', kw_only=True)
    search_output: bool = attr.ib(default=False, kw_only=True)

    @classmethod
    def create(cls, id: str, file: path, dialect: str, schema: marshmallow.Schema, **kwargs):
        source = Port.port(schema)
        error = Port.error_port(schema)
        return CsvSource(id, source, error, file, dialect, **kwargs)

    def decomment(self, fp):
        for row in fp:
            if self.comment is not None and row.startswith(self.comment):
                continue
            if row:
                yield row

    def execute(self, context: ProcessingContext):
        dataset = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        filename = context.locate_input_file(self.file, self.search_output)
        with open(filename, "r", encoding=self.encoding) as ifile:
            reader = csv.DictReader(self.decomment(ifile), dialect=self.dialect)
            line = 1
            for row in reader:
                try:
                    value = Record(line, self.output.schema.load(row), None)
                    if self.predicate is None or self.predicate(value):
                        dataset.add(value)
                        self.count(self.ACCEPTED_COUNT, value, context)
                except marshmallow.ValidationError as err:
                    err.data['_line'] = line
                    err.data['_messages'] = err.messages
                    error = Record(line, err.data, err.messages)
                    errors.add(error)
                    self.count(self.ERROR_COUNT, error, context)
                self.count(self.PROCESSED_COUNT, None, context)
                line += 1
        context.save(self.output, dataset)
        context.save(self.error, errors)


@attr.s(auto_attribs=True)
class ExcelSource(Source):
    file: path
    sheet: str

    @classmethod
    def create(cls, id: str, file: path, sheet: str, schema: marshmallow.Schema, **kwargs):
        source = Port.port(schema)
        error = Port.error_port(schema)
        return ExcelSource(id, source, error, file, sheet, **kwargs)

    def execute(self, context: ProcessingContext):
        dataset = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        wb = openpyxl.load_workbook(context.locate_input_file(self.file), read_only=False, keep_vba=False,
                                    data_only=True, keep_links=False)
        sheetname = self.sheet if self.sheet else wb.get_sheet_names()[0]
        sheet = wb[sheetname]
        rows = sheet.values
        columns = next(rows)
        line = 0
        try:
            while True:
                row = next(rows)
                row = {columns[j]: (row[j] if row[j] else '') for j in range(len(row))}
                try:
                    value = Record(line, self.output.schema.load(row), None)
                    if self.predicate is None or self.predicate(value):
                        dataset.add(value)
                        self.count(self.ACCEPTED_COUNT, value, context)
                except marshmallow.ValidationError as err:
                    err.data['_line'] = line
                    err.data['_messages'] = err.messages
                    error = Record(line, err.data, err.messages)
                    errors.add(error)
                    self.count(self.ERROR_COUNT, error, context)
                line += 1
                self.count(self.PROCESSED_COUNT, row, context)
        except StopIteration:
            pass
        context.save(self.output, dataset)
        context.save(self.error, errors)
