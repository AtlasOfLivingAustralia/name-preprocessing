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
import re
import uuid
from typing import Callable, Dict, List, Tuple

import attr

from dwc.schema import MappingSchema, IdentifierSchema
from processing.dataset import Port, Keys, Index, Dataset, Record, IndexType
from processing.node import ProcessingContext
from processing.transform import ThroughTransform, Transform


@attr.s
class DwcTaxonValidate(ThroughTransform):
    """Test for structurally valid taxon entries"""
    taxon_keys: Keys = attr.ib()
    parent_keys: Keys = attr.ib()
    accepted_keys: Keys = attr.ib()

    @classmethod
    def create(cls, id: str,  input: Port, **kwargs):
        output = Port.port(input.schema)
        taxon_keys = Keys.make_keys(input.schema, 'taxonID')
        parent_keys = Keys.make_keys(input.schema, 'parentNameUsageID')
        accepted_keys = Keys.make_keys(input.schema, 'acceptedNameUsageID')
        return DwcTaxonValidate(id, input, output, None, taxon_keys, parent_keys, accepted_keys, **kwargs)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        index = Index.create(data, self.taxon_keys, IndexType.UNIQUE)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        for record in data.rows:
            try:
                err = []
                issue = []
                id = taxonID = self.taxon_keys.get(record)
                if taxonID is None:
                    err.append("No taxonID for record " + str(record.line))
                    id = '#' + str(record.line)
                if record.parentNameUsageID is not None and record.acceptedNameUsageID is not None:
                    err.append("Record " + id + " has both a parent and accepted name")
                parent = self.parent_keys.get(record)
                if parent is not None:
                    pr = index.find(record, self.parent_keys)
                    if pr is None:
                        err.append("Record " + str(id) + " has missing parent " + str(parent))
                accepted = self.accepted_keys.get(record)
                if accepted is not None:
                    ar = index.find(record, self.accepted_keys)
                    if ar is None:
                        err.append("Record " + str(id) + " has missing accepted " + str(accepted))
                if len(err) == 0:
                    self.count(self.ACCEPTED_COUNT, record, context)
                    result.add(record)
                else:
                    self.count(self.ERROR_COUNT, record, context)
                    errors.add(Record.error(record, ', '.join(err)))
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                self.count(self.ERROR_COUNT, record, context)
                errors.add(Record.error(record, err))
            self.count(self.PROCESSED_COUNT, record, context)
        context.save(self.output, result)
        context.save(self.error, errors)

@attr.s
class DwcTaxonClean(ThroughTransform):
    """Remove any invalid links. Used for cases where there is a filter of accepted records"""
    CLEANED_COUNT = 'cleaned'

    taxon_keys: Keys = attr.ib()
    parent_keys: Keys = attr.ib()
    accepted_keys: Keys = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, **kwargs):
        output = Port.port(input.schema)
        taxon_keys = Keys.make_keys(input.schema, 'taxonID')
        parent_keys = Keys.make_keys(input.schema, 'parentNameUsageID')
        accepted_keys = Keys.make_keys(input.schema, 'acceptedNameUsageID')
        return DwcTaxonClean(id, input, output, None, taxon_keys, parent_keys, accepted_keys, **kwargs)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        index = Index.create(data, self.taxon_keys, IndexType.UNIQUE)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        for record in data.rows:
            try:
                cleaned = None
                parent = self.parent_keys.get(record)
                if parent is not None:
                    pr = index.find(record, self.parent_keys)
                    if pr is None:
                        cleaned = Record.copy(record)
                        self.parent_keys.set(cleaned, None)
                accepted = self.accepted_keys.get(record)
                if accepted is not None:
                    ar = index.find(record, self.accepted_keys)
                    if ar is None or ar == record:
                        if cleaned is None:
                            cleaned = Record.copy(record)
                            self.accepted_keys.set(cleaned, None)
                if cleaned is not None:
                    self.count(self.CLEANED_COUNT, record, context)
                self.count(self.ACCEPTED_COUNT, record, context)
                result.add(cleaned if cleaned is not None else record)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                self.count(self.ERROR_COUNT, record, context)
                errors.add(Record.error(record, err))
            self.count(self.PROCESSED_COUNT, record, context)
        context.save(self.output, result)
        context.save(self.error, errors)

@attr.s
class DwcTaxonTrail(ThroughTransform):
    """
    Provide a complete reference list of accepted taxon entries, following parent and accepted links.

    Used when we have a reference dataset and a partial collection and we need to include all parents/accepted
    taxa as well as the actual taxon list.
    """
    reference: Port = attr.ib()
    reference_keys: Keys = attr.ib()
    parent_keys: Keys = attr.ib()
    accepted_keys: Keys = attr.ib()

    @classmethod
    def create(cls, id: str,  input: Port, reference: Port, reference_keys, parent_keys, accepted_keys, **kwargs):
        output = Port.port(reference.schema)
        reference_keys = Keys.make_keys(input.schema, reference_keys)
        parent_keys = Keys.make_keys(input.schema, parent_keys)
        accepted_keys = Keys.make_keys(input.schema, accepted_keys)
        return DwcTaxonTrail(id, input, output, None, reference, reference_keys, parent_keys, accepted_keys, **kwargs)

    def trace(self, index: Index, record: Record, seen: set, result: Dataset, context: ProcessingContext):
        reference_key = self.reference_keys.get(record)
        if reference_key in seen:
            return
        seen.add(reference_key)
        parent = index.find(record, self.parent_keys)
        if parent is not None:
            self.trace(index, parent, seen, result, context)
        accepted = index.find(record, self.accepted_keys)
        if accepted is not None:
            self.trace(index, accepted, seen, result, context)
        self.count(self.ACCEPTED_COUNT, record, context)
        result.add(record)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        reference = context.acquire(self.reference)
        index = Index.create(reference, self.reference_keys, IndexType.UNIQUE)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        seen = set()
        for record in data.rows:
            try:
                actual = index.find(record, self.reference_keys)
                if actual is None:
                    self.count(self.ERROR_COUNT, record, context)
                    errors.add(Record.error(record, "Missing reference entry"))
                else:
                    self.trace(index, actual, seen, result, context)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                self.count(self.ERROR_COUNT, record, context)
                errors.add(Record.error(record, err))
            self.count(self.PROCESSED_COUNT, record, context)
        context.save(self.output, result)
        context.save(self.error, errors)

@attr.s
class DwcTaxonReidentify(ThroughTransform):
    """
    Re-work the identifiers in a taxonmy so that the identifiers are re-we

    Used when we have a reference dataset and a partial collection and we need to include all parents/accepted
    taxa as well as the actual taxon list.
    """
    MAPPED_COUNT = "mapped"

    mapping: Port = attr.ib()
    identifier_keys: Keys = attr.ib()
    parent_keys: Keys = attr.ib()
    accepted_keys: Keys = attr.ib()
    identifier: Callable = attr.ib()

    @classmethod
    def create(cls, id: str,  input: Port, identifier_keys, parent_keys, accepted_keys, identifier: Callable, **kwargs):
        output = Port.port(input.schema)
        mapping = Port.port(MappingSchema())
        identifier_keys = Keys.make_keys(input.schema, identifier_keys)
        parent_keys = Keys.make_keys(input.schema, parent_keys)
        accepted_keys = Keys.make_keys(input.schema, accepted_keys)
        return DwcTaxonReidentify(id, input, output, None, mapping, identifier_keys, parent_keys, accepted_keys, identifier, **kwargs)

    def outputs(self) -> Dict[str, Port]:
        outputs = super().outputs()
        outputs['mapping'] = self.mapping
        return outputs

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        index = Index.create(data, self.identifier_keys, IndexType.FIRST)
        result = Dataset.for_port(self.output)
        mapping = Dataset.for_port(self.mapping)
        errors = Dataset.for_port(self.error)
        map_lookup = dict() # Use a lookup table because the identifier function may be stateful
        map_replace = dict()
        line = 0
        for record in data.rows:
            try:
                original = self.identifier_keys.get(record)
                id = self.identifier(record)
                if id in map_lookup:
                    id2 = str(uuid.uuid4())
                    self.logger.warning("Duplicate identifier for " + original + " of " + id + " replacing with " + id2)
                    id = id2
                else:
                    map_lookup[original] = id
                    map = Record(record.line, {'term': original, 'mapping': id})
                    mapping.add(map)
                map_replace[line] = id
                self.count(self.MAPPED_COUNT, record, context)
                line += 1
            except Exception as err:
                self.handle_exception(err, record, errors, context)
        line = 0
        for record in data.rows:
            try:
                composed = Record.copy(record)
                original = self.identifier_keys.get(record)
                id = map_replace.get(line, original)
                self.identifier_keys.set(composed, id)
                parent = index.find(record, self.parent_keys)
                if parent is not None:
                    original = self.identifier_keys.get(parent)
                    id = map_lookup.get(original, original)
                    self.parent_keys.set(composed, id)
                accepted = index.find(record, self.accepted_keys)
                if accepted is not None:
                    original = self.identifier_keys.get(accepted)
                    id = map_lookup.get(original, original)
                    self.accepted_keys.set(composed, id)
                result.add(composed)
                self.count(self.ACCEPTED_COUNT, record, context)
            except Exception as err:
                self.handle_exception(err, record, errors, context)
            line += 1
            self.count(self.PROCESSED_COUNT, record, context)
        context.save(self.output, result)
        context.save(self.mapping, mapping)
        context.save(self.error, errors)


@attr.s
class DwcTaxonParent(ThroughTransform):
    """
    Fill out parent classification information - genus, family, order, class, phylum, kingdom if not present.
    """
    identifier_keys: Keys = attr.ib()
    parent_keys: Keys = attr.ib()
    accepted_keys: Keys = attr.ib()
    name_keys: Keys = attr.ib()
    rank_keys = attr.ib()

    @classmethod
    def create(cls, id: str,  input: Port, identifier_keys, parent_keys, accepted_keys, name_keys, rank_keys, **kwargs):
        output = Port.port(input.schema)
        identifier_keys = Keys.make_keys(input.schema, identifier_keys)
        parent_keys = Keys.make_keys(input.schema, parent_keys)
        accepted_keys = Keys.make_keys(input.schema, accepted_keys)
        name_keys = Keys.make_keys(input.schema, name_keys)
        rank_keys = Keys.make_keys(input.schema, rank_keys)
        return DwcTaxonParent(id, input, output, None, identifier_keys, parent_keys, accepted_keys, name_keys, rank_keys, **kwargs)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        index = Index.create(data, self.identifier_keys, IndexType.FIRST)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        map_lookup = dict() # Use a lookup table because the identifier function may be stateful
        map_replace = dict()
        line = 0
        for record in data.rows:
            try:
                composed = Record.copy(record)
                while record is not None:
                    accepted = index.find(record, self.accepted_keys)
                    if accepted is not None:
                        record = accepted
                    rank = self.rank_keys.get(record)
                    name = self.name_keys.get(record)
                    if rank == 'kingdom' and composed.kingdom is None:
                        composed.data['kingdom'] = name
                    elif rank == 'phylum' and composed.phylum is None:
                        composed.data['phylum'] = name
                    elif rank == 'subphylum' and composed.subphylum is None:
                        composed.data['subphylum'] = name
                    elif rank == 'class' and composed.class_ is None:
                        composed.data['class_'] = name
                    elif rank == 'subclass' and composed.subclass is None:
                        composed.data['subclass'] = name
                    elif rank == 'order' and composed.order is None:
                        composed.data['order'] = name
                    elif rank == 'suborder' and composed.suborder is None:
                        composed.data['suborder'] = name
                    elif rank == 'infraorder' and composed.infraorder is None:
                        composed.data['infraorder'] = name
                    elif rank == 'family' and composed.family is None:
                        composed.data['family'] = name
                    elif rank == 'family' and composed.family is None:
                        composed.data['family'] = name
                    record = index.find(record, self.parent_keys)
                result.add(composed)
                self.count(self.ACCEPTED_COUNT, record, context)
            except Exception as err:
                self.handle_exception(err, record, errors, context)
            line += 1
            self.count(self.PROCESSED_COUNT, record, context)
        context.save(self.output, result)
        context.save(self.error, errors)

def _default_dataset_id():
    return lambda context, record, identifier:  context.get_default('datasetID')

@attr.s
class DwcIdentifierTranslator:
    identifier: Callable = attr.ib()
    status: Callable = attr.ib()
    datasetID: Callable = attr.ib()
    title: Callable = attr.ib()
    subject: Callable = attr.ib()
    format: Callable = attr.ib()
    source: Callable = attr.ib()
    provenance: Callable = attr.ib()

    @classmethod
    def _build_callable(cls, accessor) -> Callable:
        if accessor is None:
            return lambda context, record, identifier: None
        if isinstance(accessor, Callable):
            return accessor
        if isinstance(accessor, str):
            return lambda context, record, identifier: accessor
        raise ValueError("Unable to build callable for " + accessor)

    @classmethod
    def create(cls, identifier, status = 'variant', datasetID = _default_dataset_id(), title = None, subject = None, format = None, source = None, provenance = None):
        """
        Create a translator based on regular expressions.
        Translation of other features depend on a generator

        :param identifier: The identifier to build
        :param status: The status generator (defaults to 'alternative')
        :param datasetID: The datasetID generator (defaults to the same as the source datasetID)
        :param title: The title generator (defaults to none)
        :param subject: The subject generator (defaults to none)
        :param format: The format generator (defaults to none)
        :param source: The source generator (defaults to none)
        :param provenance: The provenance generator (defaults to none)
        :return: A regular expression replacer
        """
        identifier = DwcIdentifierTranslator._build_callable(identifier)
        status = DwcIdentifierTranslator._build_callable(status)
        datasetID = DwcIdentifierTranslator._build_callable(datasetID)
        title = DwcIdentifierTranslator._build_callable(title)
        subject = DwcIdentifierTranslator._build_callable(subject)
        format = DwcIdentifierTranslator._build_callable(format)
        source = DwcIdentifierTranslator._build_callable(source)
        provenance = DwcIdentifierTranslator._build_callable(provenance)
        return DwcIdentifierTranslator(identifier, status, datasetID, title, subject, format, source, provenance)

    @classmethod
    def regex(cls, pattern: str, replace: str, status = 'alternative', datasetID = _default_dataset_id(), title = None, subject = None, format = None, source = None, provenance = None):
        """
        Create a translator based on regular expressions.
        Translation of other features depend on a generator

        :param pattern: The pattern to match
        :param replace: The pattern to replace the match with
        :param status: The status generator (defaults to 'alternative')
        :param datasetID: The datasetID generator (defaults to the same as the source datasetID)
        :param title: The title generator (defaults to none)
        :param subject: The subject generator (defaults to none)
        :param format: The format generator (defaults to none)
        :param source: The source generator (defaults to none)
        :param provenance: The provenance generator (defaults to none)
        :return: A regular expression replacer
        """
        pattern = re.compile(pattern)
        identifier = lambda context, record, identifier: pattern.sub(replace, identifier)
        return DwcIdentifierTranslator.create(identifier, status, datasetID, title, subject, format, source, provenance)

    def translate(self, context, record, key, identifier) -> Tuple[Record, str]:
        id = self.identifier(context, record, identifier)
        if id is None:
            return (None, None)
        data = {}
        data['taxonID'] = str(key)
        data['identifier'] = id
        data['status'] = self.status(context, record, id)
        data['datasetID'] = self.datasetID(context, record, id)
        data['title'] = self.title(context, record, id)
        data['subject'] = self.subject(context, record, id)
        data['format'] = self.format(context, record, id)
        data['source'] = self.source(context, record, id)
        data['provenance'] = self.provenance(context, record, id)
        return (Record(record.line, data, record.issues), id)

@attr.s
class DwcIdentifierGenerator(Transform):
    CREATED = "created"
    """Build a set of new  """
    input: Port = attr.ib()
    output: Port = attr.ib()
    taxon_keys: Keys = attr.ib()
    identifier_keys: Keys = attr.ib()
    translators: List[DwcIdentifierTranslator] = attr.ib()
    keep_all: bool = attr.ib(kw_only=True, default=False)

    @classmethod
    def create(cls, id: str,  input: Port, taxon_keys, identifier_keys, *args, **kwargs):
        output = Port.port(IdentifierSchema())
        taxon_keys = Keys.make_keys(input.schema, taxon_keys)
        identifier_keys = Keys.make_keys(input.schema, identifier_keys)
        translators = list(args)
        return DwcIdentifierGenerator(id, input, output, taxon_keys, identifier_keys, translators, **kwargs)

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['input'] = self.input
        return inputs

    def outputs(self) -> Dict[str, Port]:
        outputs = super().outputs()
        outputs['output'] = self.output
        return outputs

    def execute(self, context: ProcessingContext):
        super().execute(context)
        data = context.acquire(self.input)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        for row in data.rows:
            try:
                seen = set()
                working = set()
                key = self.taxon_keys.get(row)
                working.add(key)
                while len(working) > 0:
                    changes = set()
                    for id in working:
                        for translator in self.translators:
                            (additional, identifier) = translator.translate(context, row, key, id)
                            if additional is not None and identifier not in seen and (self.keep_all or identifier != id):
                                self.count(self.CREATED, additional, context)
                                result.add(additional)
                                seen.add(identifier)
                                changes.add(identifier)
                    working = changes
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                errors.add(Record.error(row, err))
                self.count(self.ERROR_COUNT, row, context)
            self.count(self.PROCESSED_COUNT, row, context)
        context.save(self.output, result)
        context.save(self.error, errors)

@attr.s
class DwcAncestorIdentifierGenerator(Transform):
    """
    Build a list of ancestor identifiers for a taxon.
    This can be used if the source dataset provides a trail of elements.
    """
    input: Port = attr.ib()
    full: Port = attr.ib()
    output: Port = attr.ib()
    taxon_keys: Keys = attr.ib()
    ancestor_keys: Keys = attr.ib()
    translator: DwcIdentifierTranslator = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, full: Port, taxon_keys, ancestor_keys, translator: DwcIdentifierTranslator, **kwargs):
        taxon_keys = Keys.make_keys(input.schema, taxon_keys)
        ancestor_keys = Keys.make_keys(full.schema, ancestor_keys)
        output = Port.port(IdentifierSchema())
        return DwcAncestorIdentifierGenerator(id, input, full, output, taxon_keys, ancestor_keys, translator, **kwargs)

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['input'] = self.input
        inputs['full'] = self.full
        return inputs

    def outputs(self) -> Dict[str, Port]:
        outputs = super().outputs()
        outputs['output'] = self.output
        return outputs

    def execute(self, context: ProcessingContext):
        super().execute(context)
        data = context.acquire(self.input)
        table = context.acquire(self.full)
        index = Index.create(table, self.taxon_keys)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        additional = self.build_additional(context)
        for record in data.rows:
            try:
                ancestor = record
                trail = set()
                while True:
                    kv = self.ancestor_keys.get(ancestor)
                    if kv is None:
                        break
                    if kv in trail:
                        self.logger.warning("Circular trail at %s in %s", kv, trail)
                        errors.add(Record.error(record, None, "Circular history reference at " + str(kv)))
                        self.count(self.ERROR_COUNT, record, context)
                        break
                    trail.add(kv)
                    ancestor = index.find(ancestor, self.ancestor_keys)
                    if ancestor is None:
                         break
                    composed = self.compose(record, ancestor, context, additional)
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
        context.save(self.error, errors)

    def compose(self, record: Record, ancestor: Record, context: ProcessingContext, additional) -> Record:
        """
        Make an updated version of the record with the accepted parent

        :param record: The original record
        :param parent: The parent record (null for none)
        :param context: The processing context
        :param additional: Any additional context

        :return: A composed record, or null for no record
        """
        parent_id = self.taxon_keys.get(record)
        id = self.taxon_keys.get(ancestor)
        (composed, _id) = self.translator.translate(context, ancestor, parent_id, id)
        return composed
