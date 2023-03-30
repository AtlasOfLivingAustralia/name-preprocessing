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

import datetime
import re
import string
import uuid
from collections import OrderedDict
from collections.abc import Callable
from copy import deepcopy
from inspect import signature
from re import Pattern
from typing import List, Dict, Tuple, Set, Any

import attr
import marshmallow
from marshmallow import Schema

from processing import fields
from processing.dataset import Port, Dataset, Record, Keys, Index, IndexType
from processing.fields import String
from processing.node import Node, ProcessingContext, ProcessingException

HREF_MARKUP = re.compile(r'\s*<a [^>]*href\s*=\s*"([^"]*)"[^>]*>')
STRIP_MARKUP = re.compile(r'(<!--.*-->|<[^>]*>)')
NORMALISE_SPACES = re.compile(r'\s+')


def choose(*choices):
    """Choose the first available defined, non-empty value from a list"""
    for choice in choices:
        if choice is not None and (not isinstance(choice, str) or len(choice) > 0):
            return choice
    return None

def normalise_spaces(s: str):
    if s is None:
        return None
    s = s.strip()
    if s is None or len(s) == 0:
        return None
    s = NORMALISE_SPACES.sub(' ', s)
    return s

def strip_markup(s: str):
    """
    Remove HTML or XML markup from a string and turn it into plain text.

    :param s: The string to strip

    :return: A string with markup removed
    """
    if s is None:
        return None
    s = s.strip()
    if s is None or len(s) == 0:
        return None
    s = STRIP_MARKUP.sub('', s)
    s = s.replace('&lt;', '<')
    s = s.replace('&gt;', '>')
    s = s.replace('&amp;', '&')
    return normalise_spaces(s)

def extract_href(s: str) -> str:
    if s is None:
        return None
    match = HREF_MARKUP.match(s)
    if match:
        return match.group(1)
    return s

def _get_or_default(record: Record, context: ProcessingContext, field: str, key: str):
    val = record.data.get(field)
    if val is None:
        val = context.get_default(key)
    return val

class _TriggerSchema(Schema):
    triggered = fields.Boolean()

@attr.s
class Predicate(Node):
    """A callable node. These nodes can be used to acquire data and then filter based on that data"""
    trigger: Port = attr.ib(kw_only=True)

    @trigger.default
    def default_trigger(self):
        return Port.port(_TriggerSchema())

    def outputs(self) -> Dict[str, Port]:
        outputs = super().outputs()
        outputs['trigger'] = self.trigger
        return outputs

    def report(self, context: ProcessingContext):
        self.logger.info("Ready")

    def __call__(self, *args, **kwargs):
        record: Record = args[0]
        return self.test(record)

    def test(self, record: Record) -> bool:
        raise NotImplementedError

    def commit(self, context: ProcessingContext):
        context.save(self.trigger, Dataset.for_port(self.trigger))
        super().commit(context)

    def vertex_color(self, context: ProcessingContext):
        return 'lightgreen'

@attr.s
class Transform(Node):
    """Abstract transformation class"""
    error: Port = attr.ib(init=False, kw_only=True)

    def __attrs_post_init__(self):
        """
        Construct a default error port from the schema of the first input or output,
        which is assumed to be dreiving the transform.
        """
        source = self.inputs().get('input')
        if source is None:
            source = self.outputs().get('output')
        if source is None:
            raise ValueError("Transform " + self.id + " has no input and no output")
        self.error = Port.error_port(source.schema)
        super().__attrs_post_init__()


    def errors(self) -> Dict[str, Port]:
        errors = super().errors()
        errors['error'] = self.error
        return errors

    def build_additional(self, context: ProcessingContext):
        """
        Build additional context to pass to the composition methods.
        This method is run once at the start of the execute method and
        then the results passed on to each composition step.

        :returns None by default, can be overridden to supply extra data
        """
        return None

    def handle_exception(self, err: Exception, record: Record, errors: Dataset, context: ProcessingContext):
       if self.fail_on_exception:
           raise err
       self.count(self.ERROR_COUNT, record, context)
       errors.add(Record.error(record, err))

@attr.s
class ThroughTransform(Transform):
    REJECTED_COUNT = "rejected"
    """
    A simple abstract in-transform-out transform with an error port
    """
    input: Port = attr.ib()
    output: Port = attr.ib()
    reject: Port = attr.ib()

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['input'] = self.input
        return inputs

    def outputs(self) -> Dict[str, Port]:
        outputs = super().outputs()
        outputs['output'] = self.output
        if self.reject is not None:
            outputs['reject'] = self.reject
        return outputs

    def execute(self, context: ProcessingContext):
        super().execute(context)
        data = context.acquire(self.input)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        rejects = Dataset.for_port(self.reject) if self.reject is not None else None
        additional = self.build_additional(context)
        for row in data.rows:
            try:
                transformed = self.compose(row, context, additional)
                if transformed is not None:
                    self.count(self.ACCEPTED_COUNT, row, context)
                    result.add(transformed)
                else:
                    if rejects is not None:
                        self.count(self.REJECTED_COUNT, row, context)
                        rejects.add(row)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                errors.add(Record.error(row, err))
                self.count(self.ERROR_COUNT, row, context)
            self.count(self.PROCESSED_COUNT, row, context)
        context.save(self.output, result)
        context.save(self.error, errors)
        if self.reject is not None:
            context.save(self.reject, rejects)

    def compose(self, record: Record, context: ProcessingContext, additional) -> Record:
        """
        Transform a record

        :param record: The record
        :param context: The processing context
        :param additional: Any additional

        :return: The transformed record, or None for an ingored record
        """
        raise NotImplementedError

@attr.s
class FilterTransform(ThroughTransform):
    """
    A row-filter which selects records from an incoming dataset.
    A predicate that takes a record and returns a boolean is used to decide how to
    """
    predicate: Callable = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, pred: Callable, **kwargs):
        """
        Construct a filter

        :param id: The filter id
        :param input: The input dataset
        :param pred: The selection predicate
        :keyword record_rejects: If set to true, recod

        :return: A filter with the appropriate information
        """
        output = Port(input.schema)
        reject = Port(input.schema) if kwargs.pop('record_rejects', False) else None
        return FilterTransform(id, input, output, reject, pred, **kwargs)

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        if isinstance(self.predicate, Predicate):
            inputs.update(self.predicate.outputs())
        return inputs

    def compose(self, record: Record, context: ProcessingContext, additional) -> Record:
        """
        :return: Return the record if the predicate is true, otherwise None
        """
        return record if self.predicate(record) else None

@attr.s
class ProjectTransform(ThroughTransform):
    """
    A class that projects an input row onto an output schema.
    """

    def __attrs_post_init__(self):
        super().__attrs_post_init__()
        source = set(self.input.field_set())
        dest = set(self.output.field_set())
        self.copy_attrs = dest.intersection(source)
        self.append_attrs = dest.difference(source)

    @classmethod
    def create(cls, id: str, input: Port, output: Schema, **kwargs):
        output_port = Port.port(output)
        return ProjectTransform(id, input, output_port, None, **kwargs)

    @classmethod
    def create_from(cls, id: str, input: Port, *args, **kwargs):
        names = set(args)
        reschema = OrderedDict()
        for (name, field) in input.schema.fields.items():
            if name in names:
                mf = deepcopy(field)
                reschema[name] = mf
        output = Port.port(Port.schema_from_dict(reschema, ordered=True)())
        return ProjectTransform(id, input, output, None, **kwargs)

    def compose(self, record: Record, context: ProcessingContext, additional) -> Record:
        projected = dict()
        for cp in self.copy_attrs:
            if cp in record.data:
                projected[cp] = record.data[cp]
        for ap in self.append_attrs:
            projected[ap] = None
        return Record(record.line, projected, record.issues)

@attr.s
class LookupTransform(Transform):
    """
    A simplified lookup where the input is joined with the lookup, based on key fields
    """
    UNMATCHED_COUNT = "unmatched"

    input: Port = attr.ib()
    lookup: Port = attr.ib()
    output: Port = attr.ib()
    unmatched: Port = attr.ib()
    input_keys: Keys = attr.ib()
    lookup_keys: Keys = attr.ib()
    input_map: Dict[str, str] = attr.ib()
    lookup_map: Dict[str, str] = attr.ib()
    lookup_type: IndexType = attr.ib(default=IndexType.UNIQUE, kw_only=True)
    reject: bool = attr.ib(default=False, kw_only=True)
    merge: bool = attr.ib(default=True, kw_only=True)
    overwrite: bool = attr.ib(default=False, kw_only=True)

    @classmethod
    def create(cls, id: str, input: Port, lookup: Port, input_keys, lookup_keys, **kwargs):
        """
        Construct a lookup transform that generates as ouput a joined port with the provided key

        Which columns are included/excluded from the result are specified by the
        input/lookup_map, _include and _exclude and lookup_prefix paremeters.
        Anything explicitly mapped takes precedence over inclusion (which is a name-name map)
        Exclusion only applies if there is no explicit inclusion
        If the lookup_prefix is supplied, then that is a default addition to lookup columns.

        :param id The transform id
        :param input: The input port
        :param lookup: The lookup port
        :param input_keys: The (source) keys for the input record
        :param lookup_keys: The (target) keys for the lookup
        :keyword input_map: The columns to take (and rename) from the input
        :keyword lookup_map: The columns to take (and rename) from the input
        :keyword input_include: The columns to include from the input
        :keyword lookup_include: The columns to include from the input
        :keyword input_exclude: The columns to exclude from the input
        :keyword lookup_exclude: The columns to exclude from the input
        :keyword lookup_prefix: The prefix to append to lookup columns
        :keyword reject: Reject unmatched input records (False by default)
        :keyword merge: Merge schemas (True by default)
        :keyword overwrite: Overwrite input values with lookup values if they have the same name
        :keyword lookup_type: The type of lookup to use
        :keyword record_unmatched: Provide an output for unmatched records

        :return: A lookup transform
        """
        input_map = kwargs.pop('input_map', None)
        input_include = kwargs.pop('input_include', None)
        input_exclude = kwargs.pop('input_exclude', None)
        lookup_map = kwargs.pop('lookup_map', None)
        lookup_include = kwargs.pop('lookup_include', None)
        lookup_exclude = kwargs.pop('lookup_exclude', None)
        lookup_prefix =  kwargs.pop('lookup_prefix', None)
        if kwargs.get('merge', True):
            (input_map, input_schema) = cls._build_map(input.schema, input_map, input_include, input_exclude, None)
            (lookup_map, lookup_schema) = cls._build_map(lookup.schema, lookup_map, lookup_include, lookup_exclude, lookup_prefix)
            output = Port.merged(input_schema, lookup_schema)
        else:
            (input_map, input_schema) = cls._build_map(input.schema, input_map, input_include, input_exclude, None)
            lookup_map = None
            output = Port.port(input_schema)
        unmatched = kwargs.pop('record_unmatched', False)
        unmatched = Port.port(input_schema) if unmatched else None
        input_keys = Keys.make_keys(input.schema, input_keys)
        lookup_keys = Keys.make_keys(lookup.schema, lookup_keys)
        return LookupTransform(id, input, lookup, output, unmatched, input_keys, lookup_keys, input_map, lookup_map, **kwargs)

    @classmethod
    def _build_map(cls, schema: Schema, map: Dict[str, str], includes: List[str], excludes: List[str], prefix: str):
        if map is None and includes is None and excludes is None and prefix is None:
            return (None, schema)
        mapping = {}
        remap = (lambda x: x) if prefix is None else (lambda x: prefix + x)
        if map is None and includes is None and excludes is None:
            mapping.update({name: remap(name) for (name, field) in schema.fields.items()})
        if excludes is not None:
            mapping.update({name: remap(name) for (name, field) in schema.fields.items() if name not in excludes})
        if includes is not None:
            mapping.update({name: remap(name) for name in includes})
        if map is not None:
            mapping.update(map)
        reschema = OrderedDict()
        for (name, field) in schema.fields.items():
            if name in mapping:
                mn = mapping[name]
                mf = deepcopy(field)
                mf.name = mn
                reschema[mn] = mf
        return (mapping, Port.schema_from_dict(reschema, ordered=True)())

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['input'] = self.input
        inputs['lookup'] = self.lookup
        return inputs

    def outputs(self) -> Dict[str, Port]:
        outputs = super().outputs()
        outputs['output'] = self.output
        if self.unmatched is not None:
            outputs['unmatched'] = self.unmatched
        return outputs

    def execute(self, context: ProcessingContext):
        super().execute(context)
        data = context.acquire(self.input)
        table = context.acquire(self.lookup)
        index = Index.create(table, self.lookup_keys, self.lookup_type)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        missing = Dataset.for_port(self.unmatched) if self.unmatched is not None else None
        additional = self.build_additional(context)
        for row in data.rows:
            try:
                link = index.find(row, self.input_keys)
                if link is None:
                    self.count(self.UNMATCHED_COUNT, row, context)
                    if missing is not None:
                        missing.add(row)
                if link is not None or not self.reject:
                    composed = self.compose(row, link, context, additional)
                    if composed is not None:
                        result.add(composed)
                        self.count(self.ACCEPTED_COUNT, composed, context)
            except Exception as err:
                if self.fail_on_exception:
                    self.logger.error("Exception raised in " + self.id + " for " + str(err))
                    raise err
                errors.add(Record.error(row, err))
                self.count(self.ERROR_COUNT, row, context)
            self.count(self.PROCESSED_COUNT, row, context)
        context.save(self.output, result)
        context.save(self.error, errors)
        if missing is not None:
            context.save(self.unmatched, missing)

    def compose(self, record: Record, link: Record, context: ProcessingContext, additional) -> Record:
        """
        Compose the results of a link.
        By default, this joins the fields of the record and the link, with
        the record taking precedence for identically named fields.

        :param record: The original record
        :param link: The linked record (may be None)
        :param context: The processing context
        :param additional: Any additional context

        :return: A composed record, or null for no record
        """
        if (link is None or not self.merge) and self.input_map is None:
            return record
        if link is None:
            linked_data = self._remap(record.data, self.input_map)
        else:
            linked_data = {}
            if self.overwrite:
                linked_data.update(self._remap(record.data, self.input_map))
                linked_data.update(self._remap(link.data, self.lookup_map))
            else:
                linked_data.update(self._remap(link.data, self.lookup_map))
                linked_data.update(self._remap(record.data, self.input_map))
        return Record(record.line, linked_data, record.issues)

    def _remap(self, data: dict, map: dict):
        if map is None:
            return dict(filter(lambda item: item[1] is not None, data.items()))
        return {map[key]: value for (key, value) in data.items() if key in map and value is not None}

@attr.s
class MergeTransform(Transform):
    """Merge multiple inputs into a single output"""
    sources: List[Port] = attr.ib()
    output: Port = attr.ib()

    @classmethod
    def create(cls, id: str, *args, **kwargs):
        output = Port.port(args[0].schema)
        return MergeTransform(id, args, output)


    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        index = 0
        for source in self.sources:
            inputs[str(index)] = source
            index += 1
        return inputs

    def outputs(self) -> Dict[str, Port]:
        outputs = super().outputs()
        outputs['output'] = self.output
        return outputs

    def execute(self, context: ProcessingContext):
        super().execute(context)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        additional = self.build_additional(context)
        for source in self.sources:
            data = context.acquire(source)
            for row in data.rows:
                try:
                    composed = self.compose(row, source, context, additional)
                    if composed is not None:
                        result.add(composed)
                        self.count(self.ACCEPTED_COUNT, composed, context)
                except Exception as err:
                    if self.fail_on_exception:
                        raise err
                    errors.add(Record.error(row, err))
                    self.count(self.ERROR_COUNT, row, context)
                self.count(self.PROCESSED_COUNT, row, context)
        context.save(self.output, result)
        context.save(self.error, errors)

    def compose(self, record: Record, source: Port, context: ProcessingContext, additional) -> Record:
        """
        Compose an output record. This will be the record mapped onto the output schema.

        :param record: The original record
        :param source: The source port
        :param context: The processing context
        :param additional: Any additional context

        :return: A composed record, or null for no record
        """
        if source.schema is self.output.schema:
            return record
        return record.mapped(self.output)

@attr.s
class MapTransform(ThroughTransform):
    """
    Map an input of one schema onto another schema
    """
    map: Dict[str, Callable] = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, schema: Schema, map: Dict[str, object], auto=False, **kwargs):
        if schema is None:
            schema = cls._build_schema(input.schema, map, auto)
        output = Port.port(schema)
        map = cls._build_map(input.schema, output.schema, map, auto)
        return MapTransform(id, input, output, None, map)

    @classmethod
    def constant(cls, value):
        """
        Mapping that returns a constant value

        :param value: The constant value
        :return:
        """
        return lambda: value

    @classmethod
    def capwords(cls, key):
        """
        Return a capitalised version of a word
        :param key: The record key
        :return: The capitalised version of that record
        """
        return lambda r: string.capwords(r.data[key]) if r.data[key] else None


    @classmethod
    def lowercase(cls, key):
        """
        Return a lowercase version of a value
        :param key: The record key
        :return: The lowercase version of that record
        """
        return lambda r: str(r.data[key]).lower() if r.data[key] else None

    @classmethod
    def default(cls, key):
        """
        Mapping that returns a default lookup on a context

        :param key: The context key
        :return:
        """
        return lambda r, c: c.get_default(key)

    @classmethod
    def orDefault(cls, transform: Callable, key: str):
        """
        Mapping that adds a default lookup on a context

        :param transform The actual mapping to use to get the data
        :param key: The context key
        :return:
        """
        sig = signature(transform)
        nargs = len(sig.parameters)
        if nargs > 2:
            raise ValueError("Can't handle 3+ parameter mapping")
        return lambda r, c: cls._or_default(r, c, transform, nargs, key)

    @classmethod
    def _or_default(cls, record: Record, context: ProcessingContext, transform: Callable, nargs: int, key: str):
        if nargs == 0:
            val = transform()
        elif nargs == 1:
            val = transform(record)
        elif nargs == 2:
            val = transform(record, context)
        else:
            val = None
        if val is None:
            val = context.get_default(key)
        return val

    @classmethod
    def choose(cls, *args):
        """
        Choose from a variety of possibilities

        :param args: The choices, either field names or lambda expressions

        :return: A transform that will choose the first non-none value
        """
        choices = []
        for arg in args:
            if isinstance(arg, str):
                transform = (1, cls._getter(arg))
            elif isinstance(arg, Callable):
                sig = signature(arg)
                nargs = len(sig.parameters)
                transform = (nargs, arg)
            else:
                raise ValueError("Can't make choice out of " + arg)
            choices.append(transform)
        return lambda r, c: cls._choose(r, c, choices)

    @classmethod
    def _choose(cls, record: Record, context: ProcessingContext, choices: List[Tuple[int, Callable]]):
        for (nargs, transform) in choices:
            if nargs == 0:
                val = transform()
            elif nargs == 1:
                val = transform(record)
            elif nargs == 2:
                val = transform(record, context)
            else:
                val = None
            if val is not None:
                return val
        return None

    @classmethod
    def dateparse(cls, field, *args):
        """
        Mapping that parses a date from a string.

        :param field: The field name to parse
        :param args: A list of date formats, tried in order.

        :return: The parsed date or None for no successful parsing
        """
        return lambda r: cls._dateparse(r.data.get(field), *args)

    @classmethod
    def _dateparse(cls, value, *args):
        """Parse a date string from a sequence of values"""
        if value is None:
            return None
        for format in args:
            try:
                dt = datetime.datetime.strptime(value, format)
                return dt
            except ValueError:
                pass
        return None

    @classmethod
    def uuid(cls):
        """Generate a new UUID"""
        return lambda r: str(uuid.uuid4())

    @classmethod
    def _getter(cls, name):
        return lambda r: r.data.get(name)

    @classmethod
    def _build_schema(cls, input: Schema, map: Dict[str, object], auto: bool):
        """
        Build an auto-schema based on the input and adding string fields for anything not recognised.


        :param input: The input schema
        :param map: The map giving the output fields in terms of the input fields.
        :param auto: If true, then any matching field names are mapped automatically, with approriate type transformations

        :return: An auto-generated schema
        """
        fields = OrderedDict()
        if auto:
            for (name, field) in input.fields.items():
                fields[name] = field
        for name in map.keys():
            field = input.fields.get(name)
            if field is not None:
                fields[name] = field
            else:
                fields[name] = String(missing = None)
        return Port.schema_from_dict(fields, ordered=True)()

    @classmethod
    def _build_map(cls, input: Schema, output: Schema, map: Dict[str, object], auto: bool):
        """
        Build the mapping.

        The input map can contain a number of possible values:

        * The most general is a function that takes a record as an argument and returns an appropriate value
        * A string value which corresponds to a field in the input schema. The value is copied across with conversion

        :param input: The input schema
        :param output: The output schema
        :param map: The map giving the output fields in terms of the input fields.
        :param auto: If true, then any matching field names are mapped automatically, with approriate type transformations

        :return: A complete map, with all shortcuts replaced by callable values
        """
        result = {}
        if auto:
            for (name, o_field) in output.fields.items():
                if name in input.fields:
                   i_field = input.fields.get(name)
                   if type(i_field) is type(o_field):
                       converter = cls._getter(name)
                   else:
                       converter = cls._converter(i_field, o_field, name)
                   result[name] = converter
        for (name, transform) in map.items():
            o_field = output.fields[name]
            if isinstance(transform, str) and transform in input.fields:
                i_field = input.fields.get(transform)
                converter = cls._converter(i_field, o_field, transform)
            elif isinstance(transform, Callable):
                converter = transform
            else:
                raise ValueError("Unable to decode mapping for " + str(name) + " from " + str(transform))
            result[name] = converter
        return result


    @classmethod
    def _converter(cls, input: marshmallow.fields.Field, output: marshmallow.fields.Field, attr: str):
        return lambda r: cls._convert(input, output, attr, r)

    @classmethod
    def _convert(cls, input: marshmallow.fields.Field, output: marshmallow.fields.Field, attr: str, record: Record):
        value = record.data.get(attr)
        if value is None:
            return None
        if not isinstance(input, marshmallow.fields.String):
            value = input._serialize(value, attr, record.data)
        if not isinstance(output, marshmallow.fields.String):
            value = output._deserialize(value, attr, record.data)
        return value

    def compose(self, record: Record, context: ProcessingContext, additional) -> Record:
        """
        Transform a record based on the supplied map

        :param record: The record
        :param context: The processing context
        :param additional: Any additional

        :return: The transformed record, or None for an ingored record
        """
        data = { }
        for (name, transform) in self.map.items():
            sig = signature(transform)
            nargs = len(sig.parameters)
            if nargs == 0:
                data[name] = transform()
            elif nargs == 1:
                data[name] = transform(record)
            elif nargs == 2:
                data[name] = transform(record, context)
            elif nargs == 3:
                data[name] = transform(record, context, additional)
            else:
                raise ProcessingException("Unable to process function with signature " + str(sig))
        self.output.schema.validate(data)
        return Record(record.line, data, record.issues)

@attr.s
class ReferenceTransform(ThroughTransform):
    """
    A transform that references another dataset as a source of information and links.
    """
    INVALID_COUNT = "invalid"

    reference: Port = attr.ib() # The reference data source
    invalid: Port = attr.ib() # Output for invalid records (may be None)
    reference_keys: Keys = attr.ib() # The reference data source primary key
    valid_keys: Keys = attr.ib() # The key needed to build the key lookup for a valid record (if None then no valid lookup)
    link_keys: Keys = attr.ib() # The key needed to build the key lookup for a link record (if None then no link lookup)
    allow_unmatched: bool = attr.ib(default=False, kw_only=True) # Treat

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['reference'] = self.reference
        return inputs

    def outputs(self) -> Dict[str, Port]:
        outputs = super().outputs()
        if self.invalid is not None:
            outputs['invalid'] = self.invalid
        return outputs

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        reference_records = context.acquire(self.reference)
        reference_index = Index.create(reference_records, self.reference_keys)
        result = Dataset.for_port(self.output)
        invalid = Dataset.for_port(self.invalid) if self.invalid is not None else None
        errors = Dataset.for_port(self.error)
        additional = self.build_additional(context)
        for record in data.rows:
            try:
                valid = reference_index.find(record, self.valid_keys) if self.valid_keys is not None else None
                link = reference_index.find(record, self.link_keys) if self.link_keys is not None else None
                if ((valid is None and self.valid_keys is not None) or (link is None and self.link_keys is not None)) and not self.allow_unmatched:
                    self.count(self.INVALID_COUNT, record, context)
                    if invalid is not None:
                        invalid.add(record)
                else:
                    composed = self.compose(record, valid, link, context, additional)
                    if composed is not None:
                        result.add(composed)
                        self.count(self.ACCEPTED_COUNT, composed, context)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                errors.add(Record.error(record, err))
                self.count(self.ERROR_COUNT, record, context)
            self.count(self.PROCESSED_COUNT, record, context)
        context.save(self.output, result)
        if invalid is not None:
            context.save(self.invalid, invalid)
        context.save(self.error, errors)

    def compose(self, record: Record, valid, reference: Record, context: ProcessingContext, additional) -> Record:
        """
        A DwC version of the record

        :param record: The original record
        :param value: The valid record in the reference set corresponding to the record in the reference dataset
        :param link: The link record, eg. a parent or accepted record
        :param context: The processing context
        :param additional: Any additional context

        :return: A composed record, or null for no record
        """
        raise NotImplementedError


@attr.s
class DenormaliseTransform(ThroughTransform):
    """
    A split a record based on a delimiter in a field.
    """
    INVALID_COUNT = "invalid"

    field: str = attr.ib() # The field to split
    expander: Callable = attr.ib() # The expander to use while splitting
    include_empty: bool = attr.ib(default=False, kw_only=True)


    @classmethod
    def expand(cls, id: str, input: Port, field: str, expander: Callable, **kwargs):
        """
        Create a denormaliser with an expansion function

        :param id: The identifier
        :param input: The input port
        :param field: The field to denormalise
        :param expander: The expansion function
        :keyword include_empty: Include empty records (false by default)

        :return: A denormalising transform
        """
        output = Port.port(input.schema)
        return DenormaliseTransform(id, input, output, None, field, expander, **kwargs)

    @classmethod
    def delimiter(cls, id: str, input: Port, field: str, delimiter: str, **kwargs):
        """
        Create a denomaliser by splitting by a delimiter

        :param id: The identifier
        :param input: The input port
        :param field: The field to denormalise
        :param delimiter: The delimiter to use
        :keyword include_empty: Include empty records (false by default)

        :return: A denormalising transform
        """
        expander = lambda r: cls._delimiter_expand(r, field, delimiter)
        return cls.expand(id, input, field, expander, **kwargs)

    @classmethod
    def _delimiter_expand(cls, r: Record, field: str, delimiter: str):
        value: str = r.data.get(field)
        value = value.strip() if value is not None else None
        if value is None or len(value) == 0:
            return None
        return str(value).split(delimiter)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        additional = self.build_additional(context)
        for record in data.rows:
            try:
                self.count(self.PROCESSED_COUNT, record, context)
                values = self.expander(record)
                if values is None or len(values) == 0:
                    if self.include_empty:
                        result.add(record)
                        self.count(self.ACCEPTED_COUNT, record, context, 0)
                    continue
                index = 0
                for v in values:
                    v = v.strip()
                    if len(v) > 0:
                        composed = self.compose(record, context, additional, v, index)
                        result.add(composed)
                        self.count(self.ACCEPTED_COUNT, composed, context)
                        index += 1
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                errors.add(Record.error(record, err))
                self.count(self.ERROR_COUNT, record, context)
        context.save(self.output, result)
        context.save(self.error, errors)

    def compose(self, record: Record, context: ProcessingContext, additional, value: str, index: int) -> Record:
        """
        Compose the record. Create a new record with a denormalised value

        :param record: The original record
        :param context: The processing context
        :param additional: Any additional context
        :param value The new value
        :param index The denormlisation index value counting from 0

        :return: A composed record, or null for no record
        """
        record = Record(record.line, dict(record.data), record.issues)
        record.data[self.field] = value
        record.data["_index"] = index
        return record

@attr.s
class DeduplicateTransform(ThroughTransform):
    """
    Remove duplicate entries.

    The first entry is uses as the chosen entry, all others are duplicates
    """
    DUPLICATE_COUNT = "duplicate"

    keys: Keys = attr.ib() # The keys to de-duplcate on

    @classmethod
    def create(cls, id: str, input: Port, keys, **kwargs):
        """
        Create a denomaliser

        :param id: The identifier
        :param input: The input port
        :param keys: The keys to deduplicate on

        :return: A deduplicating transform
        """
        output = Port.port(input.schema)
        duplicates = Port.port(input.schema)
        keys = Keys.make_keys(input.schema, keys)
        return DeduplicateTransform(id, input, output, duplicates, keys, **kwargs)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        duplicates = Dataset.for_port(self.reject)
        additional = self.build_additional(context)
        seen = set()
        for record in data.rows:
            try:
                self.count(self.PROCESSED_COUNT, record, context)
                record_keys = self.keys.get(record)
                if record_keys in seen:
                    duplicates.add(record)
                    self.count(self.DUPLICATE_COUNT, record, context)
                else:
                    seen.add(record_keys)
                    result.add(record)
                    self.count(self.ACCEPTED_COUNT, record, context)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                errors.add(Record.error(record, err))
                self.count(self.ERROR_COUNT, record, context)
        context.save(self.output, result)
        context.save(self.reject, duplicates)
        context.save(self.error, errors)

@attr.s
class TrailTransform(ThroughTransform):
    """
    Provide a complete reference list of entries, following parent and accepted links.

    Used when we have a reference dataset and a partial collection and we need to include all parents/accepted
    taxa as well as the actual taxon list.
    """
    reference: Port = attr.ib()
    reference_keys: Keys = attr.ib()
    parent_keys: Keys = attr.ib()
    accepted_keys: Keys = attr.ib()
    predicate: Predicate = attr.ib()

    @classmethod
    def create(cls, id: str,  input: Port, reference: Port, reference_keys, parent_keys, accepted_keys, predicate: Predicate = None, **kwargs):
        output = Port.port(reference.schema)
        reference_keys = Keys.make_keys(input.schema, reference_keys)
        parent_keys = Keys.make_keys(input.schema, parent_keys)
        accepted_keys = Keys.make_keys(input.schema, accepted_keys) if accepted_keys else None
        return TrailTransform(id, input, output, None, reference, reference_keys, parent_keys, accepted_keys, predicate, **kwargs)

    def trace(self, index: Index, record: Record, seen: Dict[Any, Record], result: Dataset, context: ProcessingContext, required: bool):
        reference_key = self.reference_keys.get(record)
        if reference_key in seen:
            return seen[reference_key]
        seen[reference_key] = record
        parent = index.find(record, self.parent_keys)
        if parent is not None:
            parent = self.trace(index, parent, seen, result, context, False)
            parent_key = self.reference_keys.get(parent) if parent else None
            self.parent_keys.set(record, parent_key)
        else:
            self.parent_keys.set(record, None)
        if self.accepted_keys:
            accepted = index.find(record, self.accepted_keys)
            if accepted is not None:
                accepted = self.trace(index, accepted, seen, result, context, False)
                accepted_key = self.reference_keys.get(accepted) if accepted else None
                self.accepted_keys.set(record, accepted_key)
            else:
                self.accepted_keys.set(record, None)
        self.count(self.ACCEPTED_COUNT, record, context)
        if required or self.predicate is None or self.predicate.test(record):
            result.add(record)
            return record
        if parent is not None and (self.predicate is None or self.predicate.test(parent)):
            seen[reference_key] = parent
            return parent
        seen[reference_key] = None
        return None

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        reference = context.acquire(self.reference)
        index = Index.create(reference, self.reference_keys, IndexType.UNIQUE)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        seen = dict()
        for record in data.rows:
            try:
                actual = index.find(record, self.reference_keys)
                if actual is None:
                    self.count(self.ERROR_COUNT, record, context)
                    errors.add(Record.error(record, "Missing reference entry"))
                else:
                    self.trace(index, actual, seen, result, context, True)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                self.count(self.ERROR_COUNT, record, context)
                errors.add(Record.error(record, err))
            self.count(self.PROCESSED_COUNT, record, context)
        context.save(self.output, result)
        context.save(self.error, errors)

@attr.s
class VariantTransform(ThroughTransform):
    """
    Construct variants of a field from an input.
    """

    keys: Keys = attr.ib()
    transforms: List[Callable] = attr.ib()
    annotation: Callable = attr.ib(kw_only=True, default=None)

    @classmethod
    def create(cls, id: str, input: Port, keys, *args, **kwargs):
        output = Port.port(input.schema)
        allow_duplicates = kwargs.pop('allow_duplicates', False)
        reject = None
        if not allow_duplicates:
            reject = Port.port(input.schema)
        keys = Keys.make_keys(input.schema, keys)
        transforms = list(args)
        return VariantTransform(id, input, output, reject, keys, transforms)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        result = Dataset.for_port(self.output)
        rejected = Dataset.for_port(self.reject) if self.reject else None
        errors = Dataset.for_port(self.error)
        additional = self.build_additional(context)
        seen = set(self.keys.get(r) for r in data.rows) if self.reject else None
        for record in data.rows:
            try:
                self.count(self.PROCESSED_COUNT, record, context)
                value: str = self.keys.get(record)
                value = value.strip() if value is not None else None
                for transform in self.transforms:
                    sig = signature(transform)
                    nargs = len(sig.parameters)
                    if nargs == 0:
                        variant = transform()
                    elif nargs == 1:
                        variant = transform(value)
                    elif nargs == 2:
                        variant = transform(value, record)
                    elif nargs == 3:
                        variant = transform(value, record, context)
                    elif nargs == 4:
                        variant = transform(value, record, context, additional)
                    else:
                        raise ProcessingException("Unable to process function with signature " + str(sig))
                    if variant is not None:
                        var_record = Record.copy(record)
                        if self.annotation is not None:
                            self.annotation(variant, var_record)
                        self.keys.set(var_record, variant)
                        if seen is not None and variant in seen:
                            rejected.add(var_record)
                            self.count(self.REJECTED_COUNT, var_record, context)
                        else:
                            seen.add(variant)
                            result.add(var_record)
                            self.count(self.ACCEPTED_COUNT, var_record, context)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                errors.add(Record.error(record, err))
                self.count(self.ERROR_COUNT, record, context)
        context.save(self.output, result)
        context.save(self.error, errors)
        if rejected is not None:
            context.save(self.reject, rejected)

@attr.s
class SortTransform(ThroughTransform):
    """
    Sort records by some sort of key expression

    key: The key to sort on
    reverse: Reverse sort order (False by default)
    """

    key: Callable = attr.ib()
    reverse: bool = attr.ib(kw_only=True, default=False)

    @classmethod
    def create(cls, id: str, input: Port, key, **kwargs):
        output = Port.port(input.schema)
        key = cls._build_key(key, input.schema)
        return SortTransform(id, input, output, None, key, **kwargs)

    @classmethod
    def _build_key(cls, key, schema: Schema):
        if isinstance(key, Callable):
            return key
        if isinstance(key, str):
            key = Keys.make_keys(schema, key)
            return cls._build_natural_comparator(key)
        raise ValueError(key + " must be a function or a field name")

    @classmethod
    def _build_natural_comparator(cls, key: Keys):
         return lambda r: key.get(r)

    def execute(self, context: ProcessingContext):
        sig = signature(self.key)
        nargs = len(sig.parameters)
        if nargs == 1:
            key = self.key
        if nargs == 2:
            key = lambda r: self.key(r, context)
        elif nargs > 2:
            raise ValueError("Can't handle a key with more than two arguments")
        data = context.acquire(self.input)
        result = Dataset.for_port(self.output)
        result.rows.extend(data.rows)
        result.rows.sort(key=key, reverse=self.reverse)
        self.count(self.PROCESSED_COUNT, None, context, len(result.rows))
        context.save(self.output, result)


@attr.s
class AcceptTransform(ThroughTransform):
    """
    A row-filter which selects records from an incoming dataset based on whether a value is in an accepted set.
     """
    values: Port = attr.ib()
    input_keys: Keys = attr.ib()
    value_keys: Keys = attr.ib()
    exclude: bool = attr.ib(kw_only=True, default=False)

    @classmethod
    def create(cls, id: str, input: Port, values: Port, input_keys, value_keys, **kwargs):
        """
        Construct a filter

        :param id: The filter id
        :param input: The input dataset
        :param values: The values to test against
        :param input_keys: The keys to look up on the input
        :param value_keys: The keys to look up on the output
        :keyword exclude: If set to true, accept records not matching any values
        :keyword case_insensitive: If set to true, make lookups case insensitive
        :keyword record_rejects: If set to true, record rejections

        :return: A filter with the appropriate information
        """
        output = Port(input.schema)
        reject = Port(input.schema) if kwargs.pop('record_rejects', False) else None
        case_insensitive = kwargs.pop('case_insensitive', False)
        input_keys = Keys.make_keys(input.schema, input_keys, case_insensitive=case_insensitive)
        value_keys = Keys.make_keys(values.schema, value_keys, case_insensitive=case_insensitive)
        return AcceptTransform(id, input, output, reject, values, input_keys, value_keys, **kwargs)

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['values'] = self.values
        return inputs

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        rejects = Dataset.for_port(self.reject) if self.reject is not None else None
        values = context.acquire(self.values)
        value_index = Index.create(values, self.value_keys, IndexType.FIRST)
        additional = self.build_additional(context)
        for row in data.rows:
            try:
                found = value_index.find(row, self.input_keys)
                if (not self.exclude and found is not None) or (self.exclude and found is None):
                    self.count(self.ACCEPTED_COUNT, row, context)
                    transformed = self.compose(row, context, additional)
                    result.add(transformed)
                else:
                    if rejects is not None:
                        self.count(self.REJECTED_COUNT, row, context)
                        rejects.add(row)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                errors.add(Record.error(row, err))
                self.count(self.ERROR_COUNT, row, context)
            self.count(self.PROCESSED_COUNT, row, context)
        context.save(self.output, result)
        context.save(self.error, errors)
        if self.reject is not None:
            context.save(self.reject, rejects)


    def compose(self, record: Record, context: ProcessingContext, additional) -> Record:
        """
        :return: Return the record if the predicate is true, otherwise None
        """
        return record


@attr.s
class ClusterTransform(ThroughTransform):
    """
    Cluster groups of records together and then either pick the "best" one or output them as a group
     """
    signature: Callable = attr.ib()
    selector: Callable = attr.ib()
    identifier_keys: Keys = attr.ib()
    parent_keys: Keys = attr.ib()
    accepted_keys: Keys = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, signature: Callable, selector: Callable, identifier_keys, parent_keys = None, accepted_keys = None, **kwargs):
        """
        Construct a clusterer

        :param id: The filter id
        :param input: The input dataset
        :param signature: The function to generate a signature for a record
        :param selector: An optional function to choose the elements of the cluster to output
        :param identifier_keys: If set, the source of identifiers
        :param parent_keys: The keys that match the identifier for the parent
        :param accepted_keys: The keys that match the identifier for accepted values
        :keyword record_rejects: If set to true, record rejections

        :return: A filter with the appropriate information
        """
        output = Port.port(input.schema)
        reject = Port.with_field(input.schema, "_cluster_signature") if kwargs.pop('record_rejects', False) else None
        identifier_keys = Keys.make_keys(input.schema, identifier_keys) if identifier_keys else None
        parent_keys = Keys.make_keys(input.schema, parent_keys) if parent_keys else None
        accepted_keys = Keys.make_keys(input.schema, accepted_keys) if accepted_keys else None
        return ClusterTransform(id, input, output, reject, signature, selector, identifier_keys, parent_keys, accepted_keys, **kwargs)


    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        rejects = Dataset.for_port(self.reject) if self.reject is not None else None
        additional = self.build_additional(context)
        clusters = OrderedDict()
        for row in data.rows:
            try:
                row = self.compose(row, context, additional)
                sig = self.signature(row)
                if sig in clusters:
                    append = clusters.get(sig)
                else:
                    append = list()
                    clusters[sig] = append
                append.append(row)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                errors.add(Record.error(row, err))
                self.count(self.ERROR_COUNT, row, context)
            self.count(self.PROCESSED_COUNT, row, context)
        used = list()
        remap = dict()
        for (sig, cluster) in clusters.items():
            all = cluster
            if self.selector:
                cluster = self.selector((sig), cluster)
            for row in cluster:
                if self.identifier_keys is not None:
                    id = self.identifier_keys.get(row)
                    remap[id] = id
                used.append(row)
                self.count(self.ACCEPTED_COUNT, row, context)
            if rejects is not None and len(cluster) < len(all):
                id = self.identifier_keys.get(cluster[0])
                for row in all:
                    if row not in cluster:
                        row = Record.copy(row)
                        row.data['_cluster_signature'] = str(sig)
                        if self.identifier_keys is not None:
                            oldid = self.identifier_keys.get(row)
                            remap[oldid] = id
                        rejects.add(row)
                        self.count(self.REJECTED_COUNT, row, context)
        for row in used:
            if self.parent_keys is None and self.accepted_keys is None:
                result.add(row)
            else:
                copy = Record.copy(row)
                if self.parent_keys is not None:
                    parentid = self.parent_keys.get(copy)
                    if parentid is not None:
                        if parentid not in remap:
                            raise ValueError(f"Unable to find parent id {parentid} in map")
                        parentid = remap[parentid]
                        self.parent_keys.set(copy, parentid)
                if self.accepted_keys is not None:
                    acceptedid = self.accepted_keys.get(copy)
                    if acceptedid is not None:
                        if acceptedid not in remap:
                            raise ValueError(f"Unable to find accepted id {acceptedid} in map")
                        acceptedid = remap[acceptedid]
                        self.accepted_keys.set(copy, acceptedid)
                result.add(copy)
        context.save(self.output, result)
        context.save(self.error, errors)
        if self.reject is not None:
            context.save(self.reject, rejects)


    def compose(self, record: Record, context: ProcessingContext, additional) -> Record:
        """
        :return: Return the record by default
        """
        return record

class NullTransform(ThroughTransform):
    """
    A transform that just copies the input to the output"
    """

    @classmethod
    def create(cls, id: str, input: Port):
        output = Port.port(input.schema)
        return NullTransform(id, input, output, None)

    def execute(self, context: ProcessingContext):
        source = context.acquire(self.input)
        context.save(self.output, source)

@attr.s
class ParentLookupTransform(LookupTransform):
    """Look up a value but, if there is not a match, try the parent of the input record until something is found or there is no parent."""

    PARENT_COUNT = 'parents'

    identifier_keys: Keys = attr.ib()
    parent_keys: Keys = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, lookup: Port, input_keys, lookup_keys, identifier_keys, parent_keys, **kwargs):
        """
        Construct a lookup transform that generates as ouput a joined port with the provided key

        Which columns are included/excluded from the result are specified by the
        input/lookup_map, _include and _exclude and lookup_prefix paremeters.
        Anything explicitly mapped takes precedence over inclusion (which is a name-name map)
        Exclusion only applies if there is no explicit inclusion
        If the lookup_prefix is supplied, then that is a default addition to lookup columns.

        :param id The transform id
        :param input: The input port
        :param lookup: The lookup port
        :param input_keys: The (source) keys for the input record
        :param lookup_keys: The (target) keys for the lookup
        :param identifier_keys: The (source) identifier keys
        :param parent_keys: The (source) parent keys
        :keyword input_map: The columns to take (and rename) from the input
        :keyword lookup_map: The columns to take (and rename) from the input
        :keyword input_include: The columns to include from the input
        :keyword lookup_include: The columns to include from the input
        :keyword input_exclude: The columns to exclude from the input
        :keyword lookup_exclude: The columns to exclude from the input
        :keyword lookup_prefix: The prefix to append to lookup columns
        :keyword reject: Reject unmatched input records (False by default)
        :keyword merge: Merge schemas (True by default)
        :keyword overwrite: Overwrite input values with lookup values if they have the same name
        :keyword lookup_type: The type of lookup to use
        :keyword record_unmatched: Provide an output for unmatched records

        :return: A lookup transform
        """
        input_map = kwargs.pop('input_map', None)
        input_include = kwargs.pop('input_include', None)
        input_exclude = kwargs.pop('input_exclude', None)
        lookup_map = kwargs.pop('lookup_map', None)
        lookup_include = kwargs.pop('lookup_include', None)
        lookup_exclude = kwargs.pop('lookup_exclude', None)
        lookup_prefix =  kwargs.pop('lookup_prefix', None)
        if kwargs.get('merge', True):
            (input_map, input_schema) = cls._build_map(input.schema, input_map, input_include, input_exclude, None)
            (lookup_map, lookup_schema) = cls._build_map(lookup.schema, lookup_map, lookup_include, lookup_exclude, lookup_prefix)
            output = Port.merged(input_schema, lookup_schema)
        else:
            (input_map, input_schema) = cls._build_map(input.schema, input_map, input_include, input_exclude, None)
            lookup_map = None
            output = Port.port(input_schema)
        unmatched = kwargs.pop('record_unmatched', False)
        unmatched = Port.port(input_schema) if unmatched else None
        input_keys = Keys.make_keys(input.schema, input_keys)
        lookup_keys = Keys.make_keys(lookup.schema, lookup_keys)
        identifier_keys = Keys.make_keys(input.schema, identifier_keys)
        parent_keys = Keys.make_keys(input.schema, parent_keys)
        return ParentLookupTransform(id, input, lookup, output, unmatched, input_keys, lookup_keys, input_map, lookup_map, identifier_keys, parent_keys, **kwargs)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        table = context.acquire(self.lookup)
        index = Index.create(table, self.lookup_keys, self.lookup_type)
        parent_index = Index.create(data, self.identifier_keys)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        missing = Dataset.for_port(self.unmatched) if self.unmatched is not None else None
        additional = self.build_additional(context)
        for row in data.rows:
            try:
                actual = row
                link = None
                while actual is not None and link is None:
                    link = index.find(actual, self.input_keys)
                    if link is None:
                        actual = parent_index.find(actual, self.parent_keys)
                        self.count(self.PARENT_COUNT, row, context)
                if link is None:
                    self.count(self.UNMATCHED_COUNT, row, context)
                    if missing is not None:
                        missing.add(row)
                if link is not None or not self.reject:
                    composed = self.compose(row, link, context, additional)
                    if composed is not None:
                        result.add(composed)
                        self.count(self.ACCEPTED_COUNT, composed, context)
            except Exception as err:
                if self.fail_on_exception:
                    self.logger.error("Exception raised in " + self.id + " for " + str(err))
                    raise err
                errors.add(Record.error(row, err))
                self.count(self.ERROR_COUNT, row, context)
            self.count(self.PROCESSED_COUNT, row, context)
        context.save(self.output, result)
        context.save(self.error, errors)
        if missing is not None:
            context.save(self.unmatched, missing)
