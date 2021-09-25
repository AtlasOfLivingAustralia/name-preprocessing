import uuid
from typing import Callable, Dict

import attr

from dwc.schema import MappingSchema
from processing.dataset import Port, Keys, Index, Dataset, Record, IndexType
from processing.node import ProcessingContext
from processing.transform import ThroughTransform


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
