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
import os
from collections import OrderedDict
from typing import List, Dict, Set

import attr

from processing.dataset import Port, Record
from processing.node import Node, ProcessingContext


@attr.s()
class Sink(Node):
    input: Port = attr.ib()
    fieldnames: List[str] = attr.ib(default=None, kw_only=True)
    fieldkeys: Dict[str, str] = attr.ib(default=None, kw_only=True)
    required_fields: Set[str] = attr.ib(default=None, kw_only=True)
    reduce: bool = attr.ib(default=False, kw_only=True)

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        fields = self.input.schema.fields
        if self.fieldnames is None:
            self.fieldnames = [field.name for field in fields.values()]  # Keep order
        if self.fieldkeys is None:
            self.fieldkeys = {field.name: (field.data_key if field.data_key is not None else field.name) for field in
                      fields.values()}
        if self.required_fields is None:
            self.required_fields = set([ field.name for field in fields.values() if field.metadata.get('export', False) ])

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['input'] = self.input
        return inputs

    def build_data(self, record: Record, fieldnames: List[str], line: int = None):
        """
        Build the output data from a record.
        If errors are allowed, via no_errors=False, then this will suitably log the error and keep on going
        so that error sinks can squeeze something out.

        :param record: The record to format

        :return: The resulting formatted dictionary
        """
        fields = self.input.schema.fields
        data = OrderedDict()
        if line is not None:
            data['#'] = str(line)
        for name in fieldnames:
            if name in fields:
                value = record.data.get(name)
                dk = self.fieldkeys.get(name, name)
                if dk is None:
                    self.logger.warning("Null key for %s in record %d", v, record.line)
                    continue
                try:
                    ser = '' if value is None else fields[name]._serialize(value, name, record.data)
                except Exception as err:
                    if self.no_errors:
                        raise err
                    self.logger.debug("Exception %s formatting %s:'%s':%s for record %d", err, name, value, type(value),
                                      record.line)
                    ser = str(value)
                data[dk] = ser
        return data

    def reduced_fields(self, context: ProcessingContext) -> List[str]:
        """
        Get the actual fields that need to be written to the sink.
        <p>
        If the reduced parameter is true, only columns that have actual values will be written.

        :param context: The current context
        :return: The reduced field list
        """
        if not self.reduce:
            return self.fieldnames
        input = context.acquire(self.input)
        if len(input.rows) == 0:
            return self.fieldnames
        seen = self.required_fields.copy()
        for record in input.rows:
            for (key, value) in record.data.items():
                if value is not None:
                    seen.add(key)
        return list(filter(lambda name: name in seen, self.fieldnames))


    def fileName(self):
        """
        Get the file name associated with this sink, if any.
        This is generally used by meta-docments and is a relative file path.

        :return: The file name. By default, returns "unknown" so that things like metafiles can be tested
        """
        return "unknown"

    def vertex_color(self, context: ProcessingContext):
        return 'lightyellow' if self.tags.get('generated', False) else 'lightblue'

@attr.s(auto_attribs=True)
class NullSink(Node):
    """Sink that ignores a list of ports. Useful for discarding placeholder results"""
    _inputs: List[Port]

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        index = 0
        for input in self._inputs:
            inputs[str(index)] = input
            index += 1
        return inputs

    @classmethod
    def create(cls, id: str, *args, **kwargs):
        return NullSink(id, args, **kwargs)

    @classmethod
    def create_in_context(cls, id: str, input: Port, context: ProcessingContext, **kwargs):
        return NullSink(id, [input], **kwargs)

    def execute(self, context: ProcessingContext):
        for port in self.inputs().values():
            self.logger.debug("Port %s routed to null sink", port.id)

@attr.s
class CsvSink(Sink):
    file: os.path = attr.ib()
    dialect: str = attr.ib()
    work: bool = attr.ib(default=False)

    @classmethod
    def create(cls, id: str, input: Port, file: os.path, dialect: str, work: bool = False, **kwargs):
        return CsvSink(id, input, file, dialect, work, **kwargs)

    @classmethod
    def create_in_context(cls, id: str, input: Port, context: ProcessingContext, **kwargs):
        file = id + ".csv"
        return CsvSink(id, input, file, 'ala', True, **kwargs)

    def execute(self, context: ProcessingContext):
         dataset = context.acquire(self.input)
         fields = self.reduced_fields(context)
         keys = list(map(lambda name: self.fieldkeys.get(name, name), fields))
         with open(context.locate_output_file(self.file, self.work), "w") as ofile:
            writer = csv.DictWriter(ofile, keys, dialect=self.dialect)
            writer.writeheader()
            for row in dataset.rows:
                data = self.build_data(row, fields)
                try:
                    writer.writerow(data)
                    self.count(self.PROCESSED_COUNT, row, context)
                    self.count(self.ACCEPTED_COUNT, row, context)
                except Exception as err:
                    self.logger.error("Unable to write row %d: %s for %s", row.line, str(err), str(row.data))
                    self.count(self.ERROR_COUNT, row, context)

    def fileName(self):
        """
        Get the file associated with this sink.

        :return: The relative file path
        """
        return self.file

@attr.s
class LogSink(Sink):
    limit: int = attr.ib(default=None, kw_only=True)

    @classmethod
    def create(cls, id: str, input: Port, **kwargs):
        return LogSink(id, input, **kwargs)

    @classmethod
    def create_in_context(cls, id: str, input: Port, context: ProcessingContext, **kwargs):
        return cls.create(id, input, **kwargs)

    def execute(self, context: ProcessingContext):
        dataset = context.acquire(self.input)
        fields = self.reduced_fields(context)
        self.logger.info(', '.join(fields))
        line = 0
        for row in dataset.rows:
            line += 1
            if self.limit is not None and line > self.limit:
                break
            data = self.build_data(row, fields, line)
            try:
                self.logger.info(', '.join([str(data.get(self.fieldkeys.get(k, None), None)) for k in fields]))
                self.count(self.PROCESSED_COUNT, row, context)
            except Exception as err:
                self.logger.error("Unable to write row %d: %s for %s", row.line, str(err), str(row.data))
                self.count(self.ERROR_COUNT, row, context)
