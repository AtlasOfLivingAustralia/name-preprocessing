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
import string

import attr

import dwc.schema
from nsl.todwc import choose, strip_markup, normalise_spaces
from processing.dataset import Port, Keys, Record
from processing.node import ProcessingContext, ProcessingException
from processing.transform import ThroughTransform, ReferenceTransform


@attr.s
class CaabToDwcTaxonTaxonTransform(ReferenceTransform):
    """
    Convert data in CAAB form to Darwin Core.
    """
    taxonomicStatus: str = attr.ib(default='accepted', kw_only=True)

    @classmethod
    def create(cls, id: str, input: Port, reference: Port, reference_keys, parent_keys, **kwargs):
        reference_keys = Keys.make_keys(reference.schema, reference_keys)
        parent_keys = Keys.make_keys(input.schema, parent_keys) if parent_keys is not None else None
        invalid = Port.port(input.schema)
        output = Port.port(dwc.schema.TaxonSchema())
        return CaabToDwcTaxonTaxonTransform(id, input, output, None, reference, invalid, reference_keys, None, parent_keys, **kwargs)

    def compose(self, record: Record, valid: Record, parent: Record, context: ProcessingContext, additional) -> Record:
        """
        A DwC version of the record

        :param record: The original record
        :param valid: The equivalent valid record (may be None)
        :param parent: The parent record (may be None)
        :param context: The processing context
        :param additional: Any additional context

        :return: A composed record, or null for no record
        """
        taxonID = str(record.SPCODE)
        if taxonID is None:
            raise ProcessingException("Record has no taxonID")
        scientificName = choose(record.DISPLAY_NAME, record.SCIENTIFIC_NAME)
        if scientificName is None:
            raise ProcessingException("Record has no scientific name")
        dwc = {
            'taxonID': taxonID,
            'parentNameUsageID': str(parent.SPCODE) if parent is not None else None,
            'datasetID': context.get_default('datasetID'),
            'nomenclaturalCode': context.get_default('nomenclaturalCode'),
            'scientificName': normalise_spaces(scientificName),
            'scientificNameAuthorship': record.AUTHORITY,
            'kingdom': record.KINDOM,
            'phylum': record.PHYLUM,
            'subphylum': record.SUBPHYLUM,
            'class': record.CLASS,
            'subclass': record.SUBCLASS,
            'order': record.ORDER_NAME,
            'suborder': record.SUBORDER,
            'infraorder': record.INFRAORDER,
            'family': record.FAMILY,
            'genus': record.GENUS,
            'subgenus': record.SUBGENUS,
            'specificEpithet': record.SPECIFICEPITHET,
            'infraspecificEpithet': record.INFRASPECIFICEPITHET,
            'taxonRank': choose(record.RANK, 'unknown'),
            'taxonConceptID': taxonID,
            'taxonomicStatus': self.taxonomicStatus,
            'taxonRemarks': strip_markup(record.TAXON_WWW_NOTES),
        }
        errors = self.output.schema.validate(dwc)
        if errors:
            raise ProcessingException("Invalid mapping " + str(errors))
        return Record(record.line, dwc, record.issues)

@attr.s
class CaabToDwcTaxonSynonymTransform(ThroughTransform):
    """
    Convert data in CAAB form to Darwin Core.
    """
    taxonomicStatus: str = attr.ib(default='synonym', kw_only=True)

    @classmethod
    def create(cls, id: str, input: Port, **kwargs):
        output = Port.port(dwc.schema.TaxonSchema())
        return CaabToDwcTaxonSynonymTransform(id, input, output, None, **kwargs)

    def compose(self, record: Record, context: ProcessingContext, additional) -> Record:
        """
        A DwC version of the record

        :param record: The original record
        :param valid: The equivalent valid record (may be None)
        :param parent: The parent record (may be None)
        :param context: The processing context
        :param additional: Any additional context

        :return: A composed record, or null for no record
        """
        taxonID = "SY_" + str(record.SPCODE) + "_" + str(record.data.get("_index", 0))
        if taxonID is None:
            raise ProcessingException("Record has no taxonID")
        scientificName = record.RECENT_SYNONYMS
        if scientificName is None:
            raise ProcessingException("Record has no scientific name")
        dwc = {
            'taxonID': taxonID,
            'acceptedNameUsageID': str(record.SPCODE),
            'datasetID': context.get_default('datasetID'),
            'nomenclaturalCode': context.get_default('nomenclaturalCode'),
            'scientificName': normalise_spaces(scientificName),
            'taxonRank': choose(record.RANK, "unknown"),
            'taxonConceptID': taxonID,
            'taxonomicStatus': self.taxonomicStatus
        }
        errors = self.output.schema.validate(dwc)
        if errors:
            raise ProcessingException("Invalid mapping " + str(errors))
        return Record(record.line, dwc, record.issues)

@attr.s
class CaabToDwcVernacularTransform(ThroughTransform):
    """
    Convert data in CAAB form to Darwin Core.
    """
    status: str = attr.ib(default='common', kw_only=True)
    isPreferredName: bool = attr.ib(default=False, kw_only=True)

    @classmethod
    def create(cls, id: str, input: Port, **kwargs):
        output = Port.port(dwc.schema.VernacularSchema())
        return CaabToDwcVernacularTransform(id, input, output, None, **kwargs)

    def compose(self, record: Record, context: ProcessingContext, additional) -> Record:
        """
        A DwC version of the record

        :param record: The original record
        :param valid: The equivalent valid record (may be None)
        :param parent: The parent record (may be None)
        :param context: The processing context
        :param additional: Any additional context

        :return: A composed record, or null for no record
        """
        vernacularName = record.COMMON_NAME if self.isPreferredName else record.COMMON_NAMES_LIST
        if vernacularName is None:
            return None
        if not self.isPreferredName and record.COMMON_NAME is not None and record.COMMON_NAMES_LIST is not None and record.COMMON_NAME.lower() == record.COMMON_NAMES_LIST.lower():
            return None
        taxonID = str(record.SPCODE)
        nameID = ('SV_' if self.isPreferredName else 'V_') + str(record.SPCODE) + ('_' + str(record._index) if record._index else '')
        if taxonID is None:
            raise ProcessingException("Record has no taxonID")
        dwc = {
            'taxonID': taxonID,
            'nameID': nameID,
            'datasetID': context.get_default('datasetID'),
            'vernacularName': string.capwords(vernacularName),
            'status': self.status,
            'language': context.get_default('language'),
            'countryCode': context.get_default('countryCode'),
            'isPreferredName': self.isPreferredName,
            'source': context.get_default('source')
        }
        errors = self.output.schema.validate(dwc)
        if errors:
            raise ProcessingException("Invalid mapping " + str(errors))
        return Record(record.line, dwc, record.issues)
