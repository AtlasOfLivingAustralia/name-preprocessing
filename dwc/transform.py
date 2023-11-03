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

from dwc.schema import MappingSchema, IdentifierSchema, DistributionSchema, ClassificationSchema
from processing.dataset import Port, Keys, Index, Dataset, Record, IndexType
from processing.node import ProcessingContext
from processing.transform import ThroughTransform, Transform

UNINOMIAL = re.compile(r"^[A-Z][a-zü]|[A-Z][A-Z]")
SCIENTIFIC_START = re.compile(r"^(?:\"\s*)?(?:[Xx]\s+)?[A-Z][a-zü]")


@attr.s
class DwcTaxonValidate(ThroughTransform):
    """Test for structurally valid taxon entries"""
    taxon_keys: Keys = attr.ib()
    parent_keys: Keys = attr.ib()
    accepted_keys: Keys = attr.ib()
    scientific_name_keys: Keys = attr.ib()
    kingdom_keys: Keys = attr.ib()
    phylum_keys: Keys = attr.ib()
    class_keys: Keys = attr.ib()
    subclass_keys: Keys = attr.ib()
    order_keys: Keys = attr.ib()
    suborder_keys: Keys = attr.ib()
    infraorder_keys: Keys = attr.ib()
    family_keys: Keys = attr.ib()
    genus_keys: Keys = attr.ib()
    subgenus_keys: Keys = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, **kwargs):
        check_names = kwargs.pop('check_names', True)
        output = Port.port(input.schema)
        taxon_keys = Keys.make_keys(input.schema, 'taxonID')
        parent_keys = Keys.make_keys(input.schema, 'parentNameUsageID')
        accepted_keys = Keys.make_keys(input.schema, 'acceptedNameUsageID')
        scientific_name_keys = Keys.make_keys(input.schema, kwargs.pop('scientific_name_keys',
                                                                       'scientificName')) if check_names else None
        kingdom_keys = Keys.make_keys(input.schema, kwargs.pop('kingdom_keys', 'kingdom')) if check_names else None
        phylum_keys = Keys.make_keys(input.schema, kwargs.pop('phylum_keys', 'phylum')) if check_names else None
        class_keys = Keys.make_keys(input.schema, kwargs.pop('class_keys', 'class_')) if check_names else None
        subclass_keys = Keys.make_keys(input.schema, kwargs.pop('subclass_keys', 'subclass')) if check_names else None
        order_keys = Keys.make_keys(input.schema, kwargs.pop('order_keys', 'order')) if check_names else None
        suborder_keys = Keys.make_keys(input.schema, kwargs.pop('suborder_keys', 'suborder')) if check_names else None
        infraorder_keys = Keys.make_keys(input.schema,
                                         kwargs.pop('infraorder_keys', 'infraorder')) if check_names else None
        family_keys = Keys.make_keys(input.schema, kwargs.pop('family_keys', 'family')) if check_names else None
        genus_keys = Keys.make_keys(input.schema, kwargs.pop('genus_keys', 'genus')) if check_names else None
        subgenus_keys = Keys.make_keys(input.schema, kwargs.pop('subgenus_keys', 'subgenus')) if check_names else None
        return DwcTaxonValidate(id, input, output, None, taxon_keys, parent_keys, accepted_keys, scientific_name_keys,
                                kingdom_keys, phylum_keys, class_keys, subclass_keys, order_keys, suborder_keys,
                                infraorder_keys, family_keys, genus_keys, subgenus_keys, **kwargs)

    def check_scientific_name(self, record: Record, keys: Keys, uninomial: bool, errors: List[str]):
        if not keys:
            return
        name = keys.get(record)
        if not name:
            return
        if not isinstance(name, str):
            return
        name = name.strip()
        if uninomial and UNINOMIAL.match(name):
            return
        if SCIENTIFIC_START.match(name):
            return
        errors.append(f"Invalid scientific name {name}")

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
                self.check_scientific_name(record, self.scientific_name_keys, False, err)
                self.check_scientific_name(record, self.kingdom_keys, True, err)
                self.check_scientific_name(record, self.phylum_keys, True, err)
                self.check_scientific_name(record, self.class_keys, True, err)
                self.check_scientific_name(record, self.subclass_keys, True, err)
                self.check_scientific_name(record, self.order_keys, True, err)
                self.check_scientific_name(record, self.suborder_keys, True, err)
                self.check_scientific_name(record, self.infraorder_keys, True, err)
                self.check_scientific_name(record, self.family_keys, True, err)
                self.check_scientific_name(record, self.genus_keys, True, err)
                self.check_scientific_name(record, self.subgenus_keys, True, err)
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
class DwcTaxonReidentify(ThroughTransform):
    """
    Re-work the identifiers in a taxonmy so that the identifiers are re-we
    """
    MAPPED_COUNT = "mapped"

    mapping: Port = attr.ib()
    identifier_keys: Keys = attr.ib()
    parent_keys: Keys = attr.ib()
    accepted_keys: Keys = attr.ib()
    identifier: Callable = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, identifier_keys, parent_keys, accepted_keys, identifier: Callable, **kwargs):
        output = Port.port(input.schema)
        mapping = Port.port(MappingSchema())
        identifier_keys = Keys.make_keys(input.schema, identifier_keys)
        parent_keys = Keys.make_keys(input.schema, parent_keys)
        accepted_keys = Keys.make_keys(input.schema, accepted_keys)
        return DwcTaxonReidentify(id, input, output, None, mapping, identifier_keys, parent_keys, accepted_keys,
                                  identifier, **kwargs)

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
        map_lookup = dict()  # Use a lookup table because the identifier function may be stateful
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
    author_keys: Keys = attr.ib()
    rank_keys = attr.ib()
    kingdoms: Port = attr.ib()
    kingdom_keys: Keys = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, identifier_keys, parent_keys, accepted_keys, name_keys, author_keys,
               rank_keys, **kwargs):
        output = Port.merged(input.schema, ClassificationSchema())
        identifier_keys = Keys.make_keys(input.schema, identifier_keys)
        parent_keys = Keys.make_keys(input.schema, parent_keys)
        accepted_keys = Keys.make_keys(input.schema, accepted_keys)
        name_keys = Keys.make_keys(input.schema, name_keys)
        author_keys = Keys.make_keys(input.schema, author_keys) if author_keys else None
        rank_keys = Keys.make_keys(input.schema, rank_keys)
        kingdoms = kwargs.pop('kingdoms', None)
        kingdom_keys = None
        if kingdoms is not None:
            kingdom_keys = kwargs.pop('kingdom_keys', 'kingdom')
            kingdom_keys = Keys.make_keys(kingdoms.schema, kingdom_keys)
        return DwcTaxonParent(id, input, output, None, identifier_keys, parent_keys, accepted_keys, name_keys,
                              author_keys, rank_keys, kingdoms, kingdom_keys, **kwargs)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        index = Index.create(data, self.identifier_keys, IndexType.FIRST)
        kingdom_index = Index.create(context.acquire(self.kingdoms), self.kingdom_keys,
                                     IndexType.FIRST) if self.kingdoms else None
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        map_lookup = dict()  # Use a lookup table because the identifier function may be stateful
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
                    author = self.author_keys.get(record) if self.author_keys is not None else None
                    if author and name.endswith(author):
                        name = name[0:-len(author)].strip()
                    if composed.kingdom is None and ((kingdom_index is None and rank == 'kingdom') or (
                            kingdom_index is not None and kingdom_index.findByKey(name) is not None
                            and (rank == 'kingdom' or rank == 'unranked'))):
                        # kingdom rank is  the fix for Bacteria -
                        # unranked is to overcome a problem that rank of kingdom creates with Viruses
                        # fix for Issue #14 - https://github.com/AtlasOfLivingAustralia/name-preprocessing/issues/14
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
                    elif rank == 'genus' and composed.genus is None:
                        composed.data['genus'] = name
                    elif rank == 'subgenus' and composed.subgenus is None:
                        composed.data['subgenus'] = name
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
    return lambda context, record, identifier: context.get_default('datasetID')


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
    def create(cls, identifier, status='variant', datasetID=_default_dataset_id(), title=None, subject=None,
               format=None, source=None, provenance=None):
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
    def regex(cls, pattern: str, replace: str, status='alternative', datasetID=_default_dataset_id(), title=None,
              subject=None, format=None, source=None, provenance=None):
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
    def create(cls, id: str, input: Port, taxon_keys, identifier_keys, *args, **kwargs):
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
                            if additional is not None and identifier not in seen and (
                                    self.keep_all or identifier != id):
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
    def create(cls, id: str, input: Port, full: Port, taxon_keys, ancestor_keys, translator: DwcIdentifierTranslator,
               **kwargs):
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


@attr.s
class DwcClearChildlessFamilies(ThroughTransform):
    """Remove family records that have no children"""

    @classmethod
    def create(cls, id: str, input: Port, **kwargs):
        output = Port.port(input.schema)
        return DwcClearChildlessFamilies(id, input, output, None, **kwargs)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        families = set()
        for record in data.rows:  # generate list of families that are used in the data set
            if record.family is not None:
                families.add(record.family)
        for record in data.rows:  # remove and entry with a rank of family from output
            try:
                if "family" in record.taxonRank:
                    if record.scientificName in families:  # if scientific name not in family list, excluded
                        result.add(record)
                else:  # not a family record, ignore.
                    result.add(record)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                self.count(self.ERROR_COUNT, record, context)
                errors.add(Record.error(record, err))
        context.save(self.output, result)
        context.save(self.error, errors)


@attr.s
class DwcAddAdditionalAPNIRelationships(ThroughTransform):
    """Add in references to AcceptedNames where additional information has been provided by APNI"""
    """Two separate files - one for orthographic variants and one for other relationships"""
    """Starting with the orth-variant file"""

    reference: Port = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, reference: Port, **kwargs):
        output = Port.port(input.schema)
        return DwcAddAdditionalAPNIRelationships(id, input, output, None, reference, **kwargs)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        reference_data = context.acquire(self.reference)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        reference_lookup = {}
        for record in reference_data.rows:  # generate a reference lookup for the orth var
            reference_values = [record.accepted_name_usage, record.apc_relationship, record.accepted_name_usage_id]
            reference_lookup[record.scientific_name_id] = reference_values

        for record in data.rows:  # if record has an matching reference lookup, add accepted name and status
            try:
                if record.taxonID in reference_lookup:
                    modified = Record.copy(record)
                  #  modified.data['acceptedNameUsage'] = reference_lookup[record.taxonID][0]
                    modified.data['acceptedNameUseageID'] = reference_lookup[record.taxonID][2]
                    modified.data['taxonomicStatus'] = reference_lookup[record.taxonID][1]
                    modified.data['family'] = None
                    modified.data['genus'] = None
                    modified.data['specificEpithet'] = None
                    modified.data['infraspecificEpithet'] = None
                    result.add(modified)
                    # accepted_record = Record.copy(record)
                    # accepted_record.data['scientificName'] = reference_lookup[record.taxonID][0]
                    # accepted_record.data['taxonomicStatus'] = "accepted"
                    # accepted_record.data['taxonID'] = reference_lookup[record.taxonID][2]
                    # accepted_record.data['family'] = None
                    # accepted_record.data['genus'] = None
                    # accepted_record.data['specificEpithet'] = None
                    # accepted_record.data['infraspecificEpithet'] = None
                    # accepted_record.data['scientificNameAuthor'] = ''
                    # accepted_record.data['datasetID'] = 'dr5214'
                    # result.add(accepted_record)
                else:  # no match copy record as is
                    result.add(record)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                self.count(self.ERROR_COUNT, record, context)
                errors.add(Record.error(record, err))
        context.save(self.output, result)
        context.save(self.error, errors)


@attr.s
class DwcAddAdditionalAPNISynonyms(ThroughTransform):
    """Add in references to AcceptedNames where additional information has been provided by APNI"""
    """Two separate files - one for orthographic variants and one for other relationships"""
    """Starting with the orth-variant file"""

    reference: Port = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, reference: Port, **kwargs):
        output = Port.port(input.schema)
        return DwcAddAdditionalAPNISynonyms(id, input, output, None, reference, **kwargs)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        reference_data = context.acquire(self.reference)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        reference_lookup = {}
        relationship_rows = []
        accepted_rows = []
        synonym_rows = []

        output_rows = []
        key_store = {}  # key of form taxonId|acceptedNameUsageId to eliminate duplicates
        accepted_key_store = {}  # store for simple taxonID|Name key
        synonym_key_store = {}
        duplicate_synonym_store = {}

        # def result_dictionary(taxon_id, scientific_name, taxon_rank, taxonomy_status, accepted_name_usage,
        #                       accepted_name_usage_id):
        #     return {"taxonId": taxon_id, "scientificName": scientific_name, "taxonRank": taxon_rank,
        #             "taxonomyStatus": taxonomy_status, "acceptedNameUsage": accepted_name_usage,
        #             "acceptedNameUsageId": accepted_name_usage_id}
        taxonID_prefix = "https://id.biodiversity.org.au/name/apni/" # used as only id is supplied in this file.

        for record in reference_data.rows:  # generate a reference lookup for the synonym relationships
            if record.relationship:  # Filter out the rows that have relationships

                if not record.accepted_name_usage:  # both are unplaced Rule 2
                    # accepted_entry = result_dictionary(r_entry.name_id,
                    #                                    r_entry.scientific_name,
                    #                                    "",
                    #                                    "unreviewed",
                    #                                    r_entry.scientific_name,
                    #                                    r_entry.name_id)

                    # synonym_entry = result_dictionary(r_entry.syn_name_id,
                    #                                   r_entry.full_name,
                    #                                   "",
                    #                                   "synonym",
                    #                                   r_entry.scientific_name,
                    #                                   r_entry.name_id)
                    reference_values = [taxonID_prefix + record.name_id, "synonym"]
                    reference_lookup[taxonID_prefix + record.syn_name_id] = reference_values

                    # synonym_entry_key = synonym_entry["scientificName"] + "|" + synonym_entry["acceptedNameUsageId"]
                    # duplicate_synonym_store_key = synonym_entry["taxonId"] + "|" + synonym_entry["scientificName"]
                    #
                    # if synonym_entry_key not in key_store:
                    #     # output_rows.append(synonym_entry)
                    #     synonym_rows.append(synonym_entry)
                    #     key_store[synonym_entry_key] = True
                    #     if duplicate_synonym_store_key not in duplicate_synonym_store:
                    #         duplicate_synonym_store[duplicate_synonym_store_key] = 1
                    #     else:
                    #         duplicate_synonym_store[duplicate_synonym_store_key] += 1
                else:
                    # don't need to create accepted entry as the accepted entry is in the taxonomy
                    # but need to consider rule 3 and 4
                    if record.apc_relationship != "excluded":  # ignore this row if excluded -> rule 4
                        taxon_status_type = "synonym"
                        if (record.apc_relationship != "accepted") and (
                                "synonym" not in record.apc_relationship):
                            taxon_status_type = record.apc_relationship  # rule 3 => this changes synonym to missapplied,etc
                        reference_values = [record.accepted_name_usage_id, taxon_status_type]
                        reference_lookup[taxonID_prefix + record.name_id] = reference_values
                        # synonym_entry = result_dictionary(r_entry["name_id"],
                        #                                   r_entry["scientific_name"],
                        #                                   "",
                        #                                   taxon_status_type,
                        #                                   r_entry["accepted_name_usage"],
                        #                                   r_entry["accepted_name_usage_id"])

        for record in data.rows:  # if record has an matching reference lookup, add accepted name and status
            try:
                if record.taxonID in reference_lookup:
                    modified = Record.copy(record)
                    modified.data['acceptedNameUsageID'] = reference_lookup[record.taxonID][0]
                    modified.data['taxonomicStatus'] = reference_lookup[record.taxonID][1]
                    result.add(modified)
                else:  # no match copy record as is
                    result.add(record)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                self.count(self.ERROR_COUNT, record, context)
                errors.add(Record.error(record, err))
        context.save(self.output, result)
        context.save(self.error, errors)


@attr.s
class DwcSyntheticNames(ThroughTransform):
    """Create genus/species names for dangling taxa in partial lists"""
    taxon_keys: Keys = attr.ib()
    name_keys: Keys = attr.ib()
    rank_keys: Keys = attr.ib()
    ranks: List[str] = attr.ib()

    RANK_MARKER = '\\s+(?:gen|sp|ssp|subsp|var|f|form)\\.?'
    PLACEHOLDER = re.compile('([A-Z][a-z]+)' + RANK_MARKER + '(?:\\s+.*|$)')
    HYBRID = re.compile('([A-Z][a-z]+)(?:\\s+[a-z]+)?\\s+(?:X\\s+|x\\s+|\u00d7\\s+|\u00d7[a-z]).*')
    NAME_PARTS = re.compile(
        '([A-Z][a-z]*)(\\s+\\(([A-Z][a-z]*)\\))?(\\s+[a-z]+)(?:' + RANK_MARKER + ')?(\\s+[a-z]+)?.*')

    CLEAR_ALL = [
        'scientificNameAuthorship',
        'parentNameUsage',
        'acceptedNameUsage',
        'parentNameUsageID',
        'acceptedNameUsageID',
        'taxonConceptID',
        'scientificNameID'
        'taxonRemarks',
        'nomenclaturalStatus',
        'nameComplete',
        'nameFormatted',
        'nameAccordingToID',
        'nameAccordingTo',
        'namePublishedInID',
        'namePublishedIn',
        'namePublishedInYear',
        'establishmentMeans'
    ]
    CLEAR_RANK = {
        'kingdom': ['phylum', 'class_', 'subclass', 'order', 'suborder', 'infraorder', 'family', 'genus', 'subgenus',
                    'specificEpithet', 'infraspecificEpithet'],
        'phylum': ['class_', 'subclass', 'order', 'suborder', 'infraorder', 'family', 'genus', 'subgenus',
                   'specificEpithet', 'infraspecificEpithet'],
        'class': ['subclass', 'order', 'suborder', 'infraorder', 'family', 'genus', 'subgenus', 'specificEpithet',
                  'infraspecificEpithet'],
        'subclass': ['order', 'suborder', 'infraorder', 'family', 'genus', 'subgenus', 'specificEpithet',
                     'infraspecificEpithet'],
        'order': ['suborder', 'infraorder', 'family', 'genus', 'subgenus', 'specificEpithet', 'infraspecificEpithet'],
        'suborder': ['infraorder', 'family', 'genus', 'subgenus', 'specificEpithet', 'infraspecificEpithet'],
        'infraorder': ['family', 'genus', 'subgenus', 'specificEpithet', 'infraspecificEpithet'],
        'family': ['genus', 'subgenus', 'specificEpithet', 'infraspecificEpithet'],
        'genus': ['subgenus', 'specificEpithet', 'infraspecificEpithet'],
        'subgenus': ['specificEpithet', 'infraspecificEpithet'],
        'species': ['infraspecificEpithet']
    }

    @classmethod
    def create(cls, id: str, input: Port, **kwargs):
        output = Port.port(input.schema)
        taxon_keys = Keys.make_keys(input.schema, 'taxonID')
        name_keys = Keys.make_keys(input.schema, 'scientificName')
        rank_keys = Keys.make_keys(input.schema, 'taxonRank')
        ranks = ['species', 'subgenus', 'genus', 'family']
        return DwcSyntheticNames(id, input, output, None, taxon_keys, name_keys, rank_keys, ranks, **kwargs)

    def generate_parents(self, name: str) -> List[Tuple[str, str, str, str, str, str]]:
        if ' ' not in name:
            return []
        match = self.PLACEHOLDER.match(name)
        if match:
            genus = match.group(1)
            return [(genus, 'genus', genus, None, None, None)]
        match = self.HYBRID.match(name)
        if match:
            genus = match.group(1)
            return [(genus, 'genus', genus, None, None, None)]
        match = self.NAME_PARTS.match(name)
        if not match:
            return []
        genus = None
        subgenus = None
        genus = value = match.group(1).strip()
        fragments = [(value, 'genus', genus, None, None, None)]
        if match.group(2):
            subgenus = match.group(3)
            value = value + match.group(2)
            fragments.append((value, 'subgenus', genus, subgenus, None, None))
        if match.group(5):
            value = value + match.group(4)
            fragments.append((value, 'species', genus, subgenus, match.group(4).strip(), None))
        return fragments

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        names = set()
        for record in data.rows:
            names.add(self.name_keys.get(record))
        for record in data.rows:
            try:
                self.count(self.PROCESSED_COUNT, record, context)
                base_name = self.name_keys.get(record)
                parents = self.generate_parents(base_name)
                if record.subgenus is not None and not filter(lambda x: x[1] == 'subgenus', parents):
                    parents.append((record.subgenus, 'subgenus', record.genus, record.subgenus, None, None))
                if record.genus is not None and not filter(lambda x: x[1] == 'genus', parents):
                    parents.append((record.genus, 'genus', record.genus, None, None, None))
                if record.family is not None:
                    parents.append((record.family, 'family', None, None, None, None))
                if record.infraorder is not None:
                    parents.append((record.infraorder, 'infraorder', None, None, None, None))
                if record.suborder is not None:
                    parents.append((record.suborder, 'suborder', None, None, None, None))
                if record.order is not None:
                    parents.append((record.order, 'order', None, None, None, None))
                if record.subclass is not None:
                    parents.append((record.subclass, 'subclass', None, None, None, None))
                if record.class_ is not None:
                    parents.append((record.class_, 'class', None, None, None, None))
                if record.phylum is not None:
                    parents.append((record.phylum, 'phylum', None, None, None, None))
                if record.kingdom is not None:
                    parents.append((record.kingdom, 'kingdom', None, None, None, None))
                if len(parents) == 0:
                    result.add(record)
                    continue
                taxon_id = self.taxon_keys.get(record)
                subid = 0
                modified = Record.copy(record)
                for (name, rank, genus, subgenus, specificEpithet, infraspecificEpithet) in parents:
                    if name not in names and rank in self.ranks:
                        names.add(name)
                        synthetic = Record.copy(record)
                        ntid = taxon_id + '_' + str(subid)
                        subid += 1
                        self.taxon_keys.set(synthetic, ntid)
                        self.name_keys.set(synthetic, name)
                        self.rank_keys.set(synthetic, rank)
                        synthetic.data['genus'] = genus
                        if 'subgenus' in synthetic.data or subgenus is not None:
                            synthetic.data['subgenus'] = subgenus
                        if 'specificEpithet' in synthetic.data or specificEpithet is not None:
                            synthetic.data['specificEpithet'] = specificEpithet
                        if 'infraspecificEpithet' in synthetic.data or infraspecificEpithet is not None:
                            synthetic.data['infraspecificEpithet'] = infraspecificEpithet
                        synthetic.data['taxonomicStatus'] = context.get_default('defaultTaxonomicStatus',
                                                                                'inferredAccepted')
                        for field in self.CLEAR_ALL:
                            if field in synthetic.data:
                                synthetic.data[field] = None
                        clear = self.CLEAR_RANK.get(rank)
                        if clear is not None:
                            for level in clear:
                                if level in synthetic.data:
                                    synthetic.data[level] = None
                        synthetic.data[
                            'provenance'] = "Created from " + base_name + " " + taxon_id + " for inferred placement"
                        synthetic.data['taxonomicFlags'] = 'synthetic'
                        result.add(synthetic)
                    if modified.genus is None and genus is not None:
                        modified.data['genus'] = genus
                    if modified.subgenus is None and subgenus is not None:
                        modified.data['subgenus'] = subgenus
                    if modified.specificEpithet is None and specificEpithet is not None:
                        modified.data['specificEpithet'] = specificEpithet
                    self.count(self.ACCEPTED_COUNT, record, context)
                result.add(modified)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                self.count(self.ERROR_COUNT, record, context)
                errors.add(Record.error(record, err))
        context.save(self.output, result)
        context.save(self.error, errors)


@attr.s
class DwcRename(ThroughTransform):
    """
    Rename common invalid entries in a taxon
    """
    MAPPED_COUNT = "name-mapping"

    mapping: Port = attr.ib()
    rank_keys: Keys = attr.ib()
    scientific_name_keys: Keys = attr.ib()
    kingdom_keys: Keys = attr.ib()
    phylum_keys: Keys = attr.ib()
    class_keys: Keys = attr.ib()
    subclass_keys: Keys = attr.ib()
    order_keys: Keys = attr.ib()
    suborder_keys: Keys = attr.ib()
    infraorder_keys: Keys = attr.ib()
    family_keys: Keys = attr.ib()
    genus_keys: Keys = attr.ib()
    subgenus_keys: Keys = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, mapping: Port, **kwargs):
        output = Port.port(input.schema)
        rank_keys = Keys.make_keys(input.schema, kwargs.pop('rank_keys', 'taxonRank'))
        scientific_name_keys = Keys.make_keys(input.schema, kwargs.pop('scientific_name_keys', 'scientificName'))
        kingdom_keys = Keys.make_keys(input.schema, kwargs.pop('kingdom_keys', 'kingdom'))
        phylum_keys = Keys.make_keys(input.schema, kwargs.pop('phylum_keys', 'phylum'))
        class_keys = Keys.make_keys(input.schema, kwargs.pop('class_keys', 'class_'))
        subclass_keys = Keys.make_keys(input.schema, kwargs.pop('subclass_keys', 'subclass'))
        order_keys = Keys.make_keys(input.schema, kwargs.pop('order_keys', 'order'))
        suborder_keys = Keys.make_keys(input.schema, kwargs.pop('suborder_keys', 'suborder'))
        infraorder_keys = Keys.make_keys(input.schema, kwargs.pop('infraorder_keys', 'infraorder'))
        family_keys = Keys.make_keys(input.schema, kwargs.pop('family_keys', 'family'))
        genus_keys = Keys.make_keys(input.schema, kwargs.pop('genus_keys', 'genus'))
        subgenus_keys = Keys.make_keys(input.schema, kwargs.pop('subgenus_keys', 'subgenus'))
        return DwcRename(id, input, output, None, mapping, rank_keys, scientific_name_keys, kingdom_keys, phylum_keys,
                         class_keys, subclass_keys, order_keys, suborder_keys, infraorder_keys, family_keys, genus_keys,
                         subgenus_keys, **kwargs)

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['mapping'] = self.mapping
        return inputs

    def rename(self, record: Record, map: Dict[str, List[Record]], keys: Keys, rank: str,
               context: ProcessingContext) -> Record:
        rank = rank.lower() if rank else None
        name = keys.get(record)
        if not name:
            return record
        lookup = map.get(name)
        if not lookup:
            return record
        replacement = None
        found = False
        if rank:
            for mapping in lookup:
                if mapping.rank is not None and mapping.rank.lower() == rank:
                    replacement = mapping.replacement
                    found = True
                    break
        if not found:
            for mapping in lookup:
                if mapping.rank is None:
                    replacement = mapping.replacement
                    found = True
                    break
        if found:
            self.count(self.MAPPED_COUNT, record, context)
            record = Record.copy(record)
            keys.set(record, replacement)
        return record

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        mapping = context.acquire(self.mapping)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        map = dict()
        for record in mapping.rows:
            original = record.original
            replacements = map.get(original)
            if replacements is None:
                replacements = list()
                map[original] = replacements
            replacements.append(record)
        line = 0
        for record in data.rows:
            try:
                self.count(self.PROCESSED_COUNT, record, context)
                rank = self.rank_keys.get(record)
                record = self.rename(record, map, self.scientific_name_keys, rank, context)
                record = self.rename(record, map, self.kingdom_keys, 'kingdom', context)
                record = self.rename(record, map, self.phylum_keys, 'phylum', context)
                record = self.rename(record, map, self.class_keys, 'class', context)
                record = self.rename(record, map, self.subclass_keys, 'subclass', context)
                record = self.rename(record, map, self.order_keys, 'order', context)
                record = self.rename(record, map, self.suborder_keys, 'suborder', context)
                record = self.rename(record, map, self.infraorder_keys, 'infraorder', context)
                record = self.rename(record, map, self.family_keys, 'family', context)
                record = self.rename(record, map, self.genus_keys, 'genus', context)
                record = self.rename(record, map, self.subgenus_keys, 'subgenus', context)
                result.add(record)
                self.count(self.ACCEPTED_COUNT, record, context)
                line += 1
            except Exception as err:
                self.handle_exception(err, record, errors, context)
        context.save(self.output, result)
        context.save(self.error, errors)


@attr.s
class DwcVernacularStatus(ThroughTransform):
    """
    Change vernacular status based on name patterns
    """
    status: Port = attr.ib()
    vernacular_name_keys: Keys = attr.ib()
    status_keys: Keys = attr.ib()
    taxon_remarks_keys: Keys = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, status: Port, **kwargs):
        output = Port.port(input.schema)
        reject = Port.port(input.schema)
        vernacular_name_keys = Keys.make_keys(input.schema, kwargs.pop('vernacular_name_keys', 'vernacularName'))
        status_keys = Keys.make_keys(input.schema, kwargs.pop('status_keys', 'status'))
        taxon_remarks_keys = Keys.make_keys(input.schema, kwargs.pop('taxon_remarks', 'taxonRemarks'))
        return DwcVernacularStatus(id, input, output, reject, status, vernacular_name_keys, status_keys,
                                   taxon_remarks_keys, **kwargs)

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['status'] = self.status
        return inputs

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        status = context.acquire(self.status)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        rejects = Dataset.for_port(self.reject)
        patterns = list()
        for record in status.rows:
            pattern = re.compile(record.pattern)
            patterns.append((pattern, record))
        for record in data.rows:
            try:
                self.count(self.PROCESSED_COUNT, record, context)
                name = self.vernacular_name_keys.get(record)
                status = self.status_keys.get(record)
                taxon_remarks = self.taxon_remarks_keys.get(record)
                include = True
                match = False
                for pattern in patterns:
                    if not pattern[0].fullmatch(name):
                        continue
                    match = True
                    include = include and pattern[1].include
                    status = pattern[1].status if pattern[1].status else status
                    taxon_remarks = (taxon_remarks + " " if taxon_remarks else "") + pattern[1].taxonRemarks if pattern[
                        1].taxonRemarks else taxon_remarks
                if match:
                    record = Record.copy(record)
                    self.status_keys.set(record, status)
                    self.taxon_remarks_keys.set(record, taxon_remarks)
                    if not include:
                        self.count(self.REJECTED_COUNT, record, context)
                        rejects.add(record)
                        continue
                result.add(record)
                self.count(self.ACCEPTED_COUNT, record, context)
            except Exception as err:
                self.handle_exception(err, record, errors, context)
        context.save(self.output, result)
        context.save(self.reject, rejects)
        context.save(self.error, errors)


@attr.s
class DwcDefaultDistribution(ThroughTransform):
    """Create default distribution entries for taxonomic entries"""
    CLEANED_COUNT = 'cleaned'

    distribution: Port = attr.ib()
    location: Port = attr.ib()
    taxon_keys: Keys = attr.ib()
    taxonomic_status_keys: Keys = attr.ib()
    distribution_keys: Keys = attr.ib()
    location_keys: Keys = attr.ib()

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        if self.distribution:
            inputs['distribution'] = self.distribution
        inputs['location'] = self.location
        return inputs

    @classmethod
    def create(cls, id: str, input: Port, distribution: Port, location: Port, **kwargs):
        output = Port.port(DistributionSchema())
        taxon_keys = Keys.make_keys(input.schema, kwargs.pop('taxon_keys', 'taxonID'))
        taxonomic_status_keys = Keys.make_keys(input.schema, kwargs.pop('taxonomic_status_keys', 'taxonomicStatus'))
        distribution_keys = Keys.make_keys(distribution.schema,
                                           kwargs.pop('distribution_keys', 'taxonID')) if distribution else None
        location_keys = Keys.make_keys(location.schema, kwargs.pop('location_keys', 'locationID'))
        return DwcDefaultDistribution(id, input, output, None, distribution, location, taxon_keys,
                                      taxonomic_status_keys, distribution_keys, location_keys, **kwargs)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        locations = context.acquire(self.location)
        lookup = None
        if self.distribution:
            distributions = context.acquire(self.distribution)
            lookup = Index.create(distributions, self.distribution_keys, IndexType.FIRST)
        location_lookup = Index.create(locations, self.location_keys, IndexType.UNIQUE)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        datasetID = context.get_default('datasetID')
        defaultLocationID = context.get_default('defaultLocationID')
        defaultLocation = location_lookup.findByKey(defaultLocationID)
        applyToStatus = set(context.get_default('applyLocationToTaxonomicStatus').split('|'))
        for record in data.rows:
            try:
                self.count(self.PROCESSED_COUNT, record, context)
                status = self.taxonomic_status_keys.get(record)
                if len(applyToStatus) > 0 and not status in applyToStatus:
                    continue
                dist = lookup.find(record, self.taxon_keys) if lookup else None
                if dist:
                    continue
                defaultDist = {
                    'taxonID': self.taxon_keys.get(record),
                    'lifeStage': None,
                    'occurrenceStatus': None,
                    'establishmentMeans': None,
                    'degreeOfEstablishment': None,
                    'pathway': None,
                    'threatStatus': None,
                    'appendixCITES': None,
                    'eventDate': None,
                    'source': None,
                    'occurrenceRemarks': None,
                    'datasetID': datasetID,
                    'provenance': 'Default created for taxon of status ' + status
                }
                defaultDist.update(defaultLocation.data)
                dist = Record(record.line, defaultDist)
                self.count(self.ACCEPTED_COUNT, dist, context)
                result.add(dist)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                self.count(self.ERROR_COUNT, record, context)
                errors.add(Record.error(record, err))
        context.save(self.output, result)
        context.save(self.error, errors)


@attr.s
class DwcScientificNameStatus(ThroughTransform):
    """
    Change vernacular status based on name patterns
    """
    status: Port = attr.ib()
    scientific_name_keys: Keys = attr.ib()
    taxonomic_status_keys: Keys = attr.ib()
    nomenclatural_status_keys: Keys = attr.ib()
    taxon_remarks_keys: Keys = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, status: Port, **kwargs):
        output = Port.port(input.schema)
        reject = Port.port(input.schema)
        scientific_name_keys = Keys.make_keys(input.schema, kwargs.pop('vernacular_name_keys', 'scientificName'))
        taxonomic_status_keys = Keys.make_keys(input.schema, kwargs.pop('taxonomic_status_keys', 'taxonomicStatus'))
        nomenclatural_status_keys = Keys.make_keys(input.schema,
                                                   kwargs.pop('nomenclatural_status_keys', 'nomenclaturalStatus'))
        taxon_remarks_keys = Keys.make_keys(input.schema, kwargs.pop('taxon_remarks', 'taxonRemarks'))
        return DwcScientificNameStatus(id, input, output, reject, status, scientific_name_keys, taxonomic_status_keys,
                                       nomenclatural_status_keys, taxon_remarks_keys, **kwargs)

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['status'] = self.status
        return inputs

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        status = context.acquire(self.status)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        rejects = Dataset.for_port(self.reject)
        patterns = list()
        for record in status.rows:
            pattern = re.compile(record.pattern)
            patterns.append((pattern, record))
        for record in data.rows:
            try:
                self.count(self.PROCESSED_COUNT, record, context)
                name = self.scientific_name_keys.get(record)
                taxonomic_status = self.taxonomic_status_keys.get(record)
                nomenclatural_status = self.nomenclatural_status_keys.get(record)
                taxon_remarks = self.taxon_remarks_keys.get(record)
                include = True
                match = False
                for pattern in patterns:
                    matcher = pattern[0].fullmatch(name)
                    if not matcher:
                        continue
                    match = True
                    include = include and pattern[1].include
                    taxonomic_status = pattern[1].taxonomicStatus if pattern[1].taxonomicStatus else taxonomic_status
                    nomenclatural_status = pattern[1].nomenclaturalStatus if pattern[
                        1].nomenclaturalStatus else nomenclatural_status
                    taxon_remarks = (taxon_remarks + " " if taxon_remarks else "") + matcher.expand(
                        pattern[1].taxonRemarks) if pattern[1].taxonRemarks else taxon_remarks
                    if pattern[1].replace:
                        name = matcher.expand(pattern[1].replace)
                if match:
                    record = Record.copy(record)
                    self.scientific_name_keys.set(record, name)
                    self.taxonomic_status_keys.set(record, taxonomic_status)
                    self.nomenclatural_status_keys.set(record, nomenclatural_status)
                    self.taxon_remarks_keys.set(record, taxon_remarks)
                if not include:
                    self.count(self.REJECTED_COUNT, record, context)
                    rejects.add(record)
                else:
                    result.add(record)
                    self.count(self.ACCEPTED_COUNT, record, context)
            except Exception as err:
                self.handle_exception(err, record, errors, context)
        context.save(self.output, result)
        context.save(self.reject, rejects)
        context.save(self.error, errors)
