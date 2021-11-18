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

import attr
import dwc.schema
from processing.dataset import Port, Dataset, Keys, Record, Index
from processing.node import ProcessingContext, ProcessingException
from processing.transform import ThroughTransform

def quote_url_special(s: str):
    """
    Remove any odd results from a URL, replacing spaces, tabs, etc. that may have crept in.

    :param str: The source URL
    :return:
    """
    if s is None or len(s) == 0:
        return None
    return s.replace(' ', '%20').replace('\t', '%09')

def assembleAuthor(author: str, year: str, changed_comb: bool) -> str:
    build = []
    if author and author != '-':
        build.append(author.strip())
    if year and year != '-':
        if len(build) > 0:
            build.append(', ')
        build.append(year.strip())
    if changed_comb and len(build) > 0:
        build.insert(0, '(')
        build.append(')')
    return ''.join(build) if len(build) > 0 else None

@attr.s
class AcceptedToDwcTaxonTransform(ThroughTransform):
    """
    Convert data in AFD form to Darwin Core
    """
    INVALID_COUNT = "invalid"

    valid: Port = attr.ib()
    valid_keys: Keys = attr.ib()
    parent_keys: Keys = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, valid: Port, valid_keys, parent_keys, **kwargs):
        valid_keys = Keys.make_keys(valid.schema, valid_keys)
        parent_keys = Keys.make_keys(valid.schema, parent_keys) if parent_keys is not None else None
        output = Port.port(dwc.schema.TaxonSchema())
        return AcceptedToDwcTaxonTransform(id, input, output, None, valid, valid_keys, parent_keys, **kwargs)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        valid_records = context.acquire(self.valid)
        valid_index = Index.create(valid_records, self.valid_keys)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        additional = self.build_additional(context)
        for record in data.rows:
            try:
                valid = valid_index.find(record, self.valid_keys)
                if valid is None:
                    self.count(self.INVALID_COUNT, record, context)
                    continue
                parent = valid_index.find(valid, self.parent_keys) if self.parent_keys is not None else None
                composed = self.compose(record, valid, parent, context, additional)
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

    def compose(self, record: Record, valid: Record, parent: Record, context: ProcessingContext, additional) -> Record:
        """
        A DwC version of the record

        :param record: The original record
        :param parent: The parent record (null for none)
        :param context: The processing context
        :param additional: Any additional context

        :return: A composed record, or null for no record
        """
        taxonID = valid.TAXON_GUID_ID
        if taxonID is None:
            raise ProcessingException("Record has no taxonID")
        scientificName = record.SCIENTIFIC_NAME
        if scientificName is None:
            raise ProcessingException("Record has no scientific name")
        rank = ''
        if valid.RANK_PREFIX is not None:
            rank = rank + valid.RANK_PREFIX
        if valid.Rank is not None:
            rank = rank + valid.Rank
        rank = rank.lower()
        dwc = {
            'taxonID': taxonID,
            'parentNameUsageID': parent.TAXON_GUID_ID if parent is not None else None,
            'acceptedNameUsageID': None,
            'datasetID': context.get_default('datasetID'),
            'nomenclaturalCode': context.get_default('nomenclaturalCode'),
            'scientificName': scientificName,
            'scientificNameAuthorship': assembleAuthor(record.AUTHOR, record.YEAR, record.CHANGED_COMB),
            'taxonRank': rank,
            'taxonConceptID': taxonID,
            'scientificNameID': record.NAME_GUID_ID,
            'taxonomicStatus': record.taxonomicStatus if record.taxonomicStatus else 'accepted',
            'nomenclaturalStatus': record.nomenclaturalStatus,
            'establishmentMeans': None,
            'nameAccordingToID': None,
            'nameAccordingTo': None,
            'namePublishedInID': record.namePublishedInID,
            'namePublishedIn': record.namePublishedIn,
            'namePublishedInYear': record.namePublishedInYear,
            'nameComplete': None,
            'nameFormatted': None,
            'taxonRemarks': None,
            'provenance': None,
            'source': quote_url_special(record.CITE_AS)
        }
        errors = self.output.schema.validate(dwc)
        if errors:
            raise ProcessingException("Invalid mapping " + str(errors))
        return Record(record.line, dwc, record.issues)

@attr.s
class SynonymToDwcTaxonTransform(ThroughTransform):
    """
    Convert data in AFD form to Darwin Core
    """
    INVALID_COUNT = "invalid"
    SELF_SYNONYM_COUNT = "self-synonym"

    valid: Port = attr.ib()
    valid_keys: Keys = attr.ib()
    accepted_keys: Keys = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, valid: Port, valid_keys, accepted_keys, **kwargs):
        valid_keys = Keys.make_keys(valid.schema, valid_keys)
        accepted_keys = Keys.make_keys(valid.schema, accepted_keys)
        output = Port.port(dwc.schema.TaxonSchema())
        return SynonymToDwcTaxonTransform(id, input, output, None, valid, valid_keys, accepted_keys, **kwargs)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        valid_records = context.acquire(self.valid)
        valid_index = Index.create(valid_records, self.valid_keys)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        additional = self.build_additional(context)
        for record in data.rows:
            self.count(self.PROCESSED_COUNT, record, context)
            try:
                accepted = valid_index.find(record, self.accepted_keys)
                if accepted is None:
                    self.count(self.INVALID_COUNT, record, context)
                    continue
                if accepted.VALID_NAME == record.SCIENTIFIC_NAME:
                    self.count(self.SELF_SYNONYM_COUNT, record, context)
                    continue
                composed = self.compose(record, accepted, context, additional)
                if composed is not None:
                    result.add(composed)
                    self.count(self.ACCEPTED_COUNT, composed, context)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                errors.add(Record.error(record, err))
                self.count(self.ERROR_COUNT, record, context)
        context.save(self.output, result)
        context.save(self.error, errors)

    def compose(self, record: Record, accepted: Record, context: ProcessingContext, additional) -> Record:
        """
        A DwC version of the record

        :param record: The original record
        :param accepted: The accepted record
        :param context: The processing context
        :param additional: Any additional context

        :return: A composed record, or null for no record
        """
        taxonID = record.NAME_GUID_ID
        if taxonID is None:
            raise ProcessingException("Record has no taxonID")
        scientificName = record.SCIENTIFIC_NAME
        if scientificName is None:
            raise ProcessingException("Record has no scientific name")
        dwc = {
            'taxonID': taxonID,
            'parentNameUsageID': None,
            'acceptedNameUsageID': accepted.TAXON_GUID_ID,
            'datasetID': context.get_default('datasetID'),
            'nomenclaturalCode': context.get_default('nomenclaturalCode'),
            'scientificName': scientificName,
            'scientificNameAuthorship': assembleAuthor(record.AUTHOR, record.YEAR, record.CHANGED_COMB),
            'taxonRank': 'unranked',
            'taxonConceptID': taxonID,
            'scientificNameID': record.NAME_GUID_ID,
            'taxonomicStatus': record.taxonomicStatus if record.taxonomicStatus else 'synonym',
            'nomenclaturalStatus': record.nomenclaturalStatus,
            'establishmentMeans': None,
            'nameAccordingToID': None,
            'nameAccordingTo': None,
            'namePublishedInID': record.namePublishedInID,
            'namePublishedIn': record.namePublishedIn,
            'namePublishedInYear': record.namePublishedInYear,
            'nameComplete': None,
            'nameFormatted': None,
            'taxonRemarks': None,
            'provenance': None,
            'source': quote_url_special(record.CITE_AS)
        }
        errors = self.output.schema.validate(dwc)
        if errors:
            raise ProcessingException("Invalid mapping " + str(errors))
        return Record(record.line, dwc, record.issues)


@attr.s
class VernacularToDwcTransform(ThroughTransform):
    """
    Convert data in AFD form to Darwin Core
    """
    INVALID_COUNT = "invalid"

    valid: Port = attr.ib()
    valid_keys: Keys = attr.ib()
    taxon_keys: Keys = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, valid: Port, valid_keys, taxon_keys, **kwargs):
        valid_keys = Keys.make_keys(valid.schema, valid_keys)
        taxon_keys = Keys.make_keys(input.schema, taxon_keys)
        output = Port.port(dwc.schema.VernacularSchema())
        return VernacularToDwcTransform(id, input, output, None, valid, valid_keys, taxon_keys, **kwargs)

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        valid_records = context.acquire(self.valid)
        valid_index = Index.create(valid_records, self.valid_keys)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        additional = self.build_additional(context)
        for record in data.rows:
            try:
                valid = valid_index.find(record, self.taxon_keys)
                if valid is None:
                    self.count(self.INVALID_COUNT, record, context)
                    continue
                composed = self.compose(record, valid, context, additional)
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

    def compose(self, record: Record, valid: Record, context: ProcessingContext, additional) -> Record:
        """
        A DwC version of the record

        :param record: The original record
        :param context: The processing context
        :param additional: Any additional context

        :return: A composed record, or null for no record
        """
        taxonID = valid.TAXON_GUID_ID
        if taxonID is None:
            raise ProcessingException("Record has no taxonID")
        vernacularName = record.SCIENTIFIC_NAME
        if vernacularName is None:
            raise ProcessingException("Record has no name")
        dwc = {
            'taxonID': taxonID,
            'nameID': record.NAME_GUID_ID,
            'datasetID': context.get_default('datasetID'),
            'vernacularName': vernacularName,
            'status': record.nomenclaturalStatus if record.nomenclaturalStatus else 'common',
            'language': context.get_default('language'),
            'temporal': None,
            'locationID': None,
            'locality': None,
            'countryCode': context.get_default('countryCode'),
            'sex': None,
            'lifeStage': None,
            'isPlural': None,
            'isPreferredName': record.nomenclaturalStatus == 'preferred',
            'organismPart': None,
            'labels': None,
            'taxonRemarks': None,
            'provenance': None,
            'source': quote_url_special(record.CITE_AS)
        }
        errors = self.output.schema.validate(dwc)
        if errors:
            raise ProcessingException("Invalid mapping " + str(errors))
        return Record(record.line, dwc, record.issues)
