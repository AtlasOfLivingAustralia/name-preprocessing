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
import copy
import uuid
from collections import OrderedDict
from enum import Enum
from typing import List, Set, Dict, Tuple, Union

import attr
import marshmallow.fields as fields
from marshmallow import Schema, post_load


class Record:
    pass

class Port:
    pass

@attr.s(eq=False)
class Record:
    """
    A data record.

    Record data can be accessed via dot notation, so
    v.KEY will look up the data dictionary and return the value.
    """
    line: int = attr.ib(default = 0)
    data: Dict[str, object] = attr.ib(factory=dict)
    issues: str = attr.ib(default=None)

    @classmethod
    def copy(cls, record: Record):
        """
        Construct a modifiable copy of the record

        :param record: The record
        :return: A copy of the record that can have its internal data altered
        """
        return Record(record.line, record.data.copy(), record.issues)

    @classmethod
    def issue(cls, record: Record, issue: str):
        """
        Construct a copy of the record with an issue.
        Multiple issues are separated by a |

        :param record: The record
        :param issue: The issue

        :return: A record with the issue attached
        """
        return Record(record.line, record.data, record.issues + " | " + issue if record.issues is not None else issue)

    @classmethod
    def error(cls, record: Record, exception: BaseException = None, message: str = None):
        """
        Construct an error-annotated version of a record

        :param record: The originating record
        :param exception: The causing exception, if present
        :param message: An error message, if needed

        :return: An errorised version of the record
        """
        messages = [record.issues, exception, message]
        messages = ", ".join([str(m) for m in messages if m is not None])
        err = record.data.copy()
        err["_line"] = record.line
        err["_messages"] = messages
        return Record(record.line, err, messages)

    def mapped(self, port: Port) -> Record:
        """Create a new record mapped onto the scehema of the supplied port"""
        fields = port.schema.fields
        projected = {k: v for k, v in self.data.items() if k in fields }
        projected.update({k: None for k in fields.keys() if k not in self.data})
        return Record(self.line, projected, self.issues)

    def __getattr__(self, item):
        """
        Get an attribute from the underlying data.
        Missing attributes return None rather than throw an exception.

        :param item: The attribute key
        :return: The result, or None for not found
        """
        return self.data.get(item)

class Output:
    pass

class ErrorSchema(Schema):
    line = fields.Integer()
    data = fields.Dict()
    issues = fields.String()

    @post_load
    def make_error(self, data, **kwargs):
        return Record(**data)


class Keys:
    pass

@attr.s(frozen=True)
class Keys:
    """Key description for indexing"""
    keys: Tuple[fields.Field] = attr.ib()

    @classmethod
    def make_keys(cls, schema: Schema, keys):
        if isinstance(keys, Keys):
            multi = keys.multi
            ignore_duplicates = keys.ignore_duplicates
            keys = keys.keys
        get_key = lambda key: schema.fields[key.name] if isinstance(key, fields.Field) else schema.fields[key]
        key_fields = None
        if type(keys) == str:
            key_fields = (get_key(keys),)
        elif type(keys) == tuple or type(keys) == list:
            key_fields = tuple((get_key(key) for key in keys))
        if not all(key_fields):
            return None
        return Keys(key_fields)

    def make_key_map(self, record: Record, target: Keys = None) -> Dict[str, object]:
        """
        Make a key map out of a record

        :param record: The record, if None then the keys will be a tuple of Nones
        :param target: If not None, the labels to give to the keys

        :return: None for an empty key field, a value for a singleton key, a tuple for a multi-key
        """
        if target is None:
            target = self
        if record is None:
            return { target.keys[i].name: None for i, key in enumerate(self.keys) }
        return { target.keys[i].name: record.data.get(key.name) for i, key in enumerate(self.keys) }

    def get(self, record: Record):
        """
        Make a key for a record

        :param record: The record

        :return: None for an empty key field, a value for a singleton key, a tuple for a multi-key
        """
        if len(self.keys) == 0:
            return None
        if len(self.keys) == 1:
            return record.data.get(self.keys[0].name)
        return tuple((record.data.get(key.name) for key in self.keys))

    def set(self, record: Record, value):
        """
        Set a value in a record, depending on the key list

        :param record: The record to modify
        :param value: The new value. If multiple keys then the value should be a tuple
        :return:
        """
        if len(self.keys) > 1:
            if len(self.keys) != len(tuple):
                raise ValueError("Keys and value are not of the same length")
            for index, key in enumerate(self.keys):
                record.data[key.name] = value[index]
        elif len(self.keys) == 1:
            record.data[self.keys[0].name] = value
        else:
            raise ValueError("Empty key set")


@attr.s(eq=False)
class Port:
    id: str = attr.ib(kw_only=True)
    roles: List[str] = attr.ib(factory=list, kw_only=True)
    schema: Schema = attr.ib()

    @id.default
    def _default_id(self):
        return str(uuid.uuid4())

    @classmethod
    def port(cls, schema: Schema, **kwargs):
        """
        Create a port
        
        :param schema: Th
        :param keys: 
        :param kwargs: 
        :return: 
        """
        return Port(schema, **kwargs)

    @classmethod
    def merged(cls, schema1: Schema, schema2: Schema):
        fs = OrderedDict(schema1.fields)
        fs.update({k: v for (k, v) in schema2.fields.items() if k not in schema1.fields})
        uri = getattr(schema1.Meta, 'uri', None)
        namespace = getattr(schema1.Meta, 'namespace', None)
        sc = cls.schema_from_dict(fs, ordered=True, uri=uri, namespace=namespace)
        merged_schema = sc()
        return Port(merged_schema)

    @classmethod
    def error_port(cls, schema: Schema):
        fs = OrderedDict(schema.fields)
        fs['_line'] = fields.Integer()
        fs['_messages'] = fields.String()
        sc = cls.schema_from_dict(fs, ordered=schema.ordered)
        error_schema = sc()
        return Port(error_schema)

    @classmethod
    def schema_from_dict(
        cls,
        fields: Dict[str, Union[fields.Field, type]],
        *,
        name: str = "GeneratedSchema",
        ordered: bool = True,
        uri: str = None,
        namespace: str = None
    ) -> type:
        """Generate a `Schema` class given a dictionary of fields.

        Derived from Schema.from_dict which doesn't handled ordered schemas properly

        :param dict fields: Dictionary mapping field names to field instances.
        :param str name: Optional name for the class, which will appear in
            the ``repr`` for the class.
        :param bool ordered: Is this an ordered schema
        :param str uri: The rowtype of the schema
        :param str namespace: The default namespace for the schema

        .. versionadded:: 3.0.0
        """
        attrs = {k: copy.copy(v) for (k, v) in fields.items()}
        for (i, k) in enumerate(attrs):
            attrs[k]._creation_index = i
        attrs["Meta"] = type(
            "GeneratedMeta", (getattr(Schema, "Meta", object),), {"register": False, "ordered": ordered, "uri": uri, "namespace": namespace }
        )
        schema_cls = type(name, (Schema,), attrs)
        return schema_cls

    def field_set(self) -> Set[str]:
        """
        Get the set of fields that are defined by the port's schema

        :return: The field names
        """
        return self.schema.fields.keys()

@attr.s(eq=False, repr=False)
class Dataset:
    schema: Schema = attr.ib()
    rows: List[Record] = attr.ib(factory=list)

    @classmethod
    def for_port(cls, port: Port):
        """
        Construct a dataset corresponding to a specific port

        :param port: The port
        :return: A dataset corresponding to the port
        """
        return Dataset(port.schema)

    def add(self, row: Record):
        self.rows.append(row)

class IndexType(Enum):
    UNIQUE = 1,
    FIRST = 2,
    MULTI = 3

@attr.s
class Index:
    dataset: Dataset = attr.ib()
    keys: Keys = attr.ib()
    type: IndexType = attr.ib()
    index: dict = attr.ib(default=None, kw_only=True)

    def __attrs_post_init__(self):
        if self.index is None:
            self.index = dict()
            for record in self.dataset.rows:
                self._add(record)

    @classmethod
    def create(cls, dataset: Dataset, keys: Keys, type: IndexType = IndexType.UNIQUE, **kwargs):
        return Index(dataset, keys, type, **kwargs)

    def _add(self, record: Record):
        key = self.keys.get(record)
        if key is None:
            raise ValueError("No key for record")
        if self.type == IndexType.MULTI:
            existing = self.index.get(key)
            if existing is None:
               self.index[key] = [record]
            else:
               existing.append(record)
        elif key not in self.index:
            self.index[key] = record
        elif self.type == IndexType.FIRST:
                return
        else:
            raise ValueError("Duplicate key " + str(key))

    def findByKey(self, key) -> Record:
        return self.index.get(key)

    def find(self, record: Record, keys: Keys) -> Record:
        key = keys.get(record)
        return self.findByKey(key)



