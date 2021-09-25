import importlib
from typing import Dict

import attr
from lxml.etree import XSLT, fromstring

import dwc.schema
from processing.dataset import Port, Dataset, Keys, Record, Index, IndexType
from processing.node import ProcessingContext, ProcessingException
from processing.transform import ThroughTransform, choose, strip_markup, normalise_spaces

class NameFormatter:
    def __init__(self):
        name_to_html = importlib.resources.read_text(__package__, 'name_to_html.xslt', 'utf-8')
        name_to_html = fromstring(name_to_html)
        self.name_to_html = XSLT(name_to_html)

    def format(self, name: str, rank: str):
        if name is None:
            return None
        name = name.replace('&amp;', '&').replace('&', '&amp;') # Sigh
        name = fromstring(name)
        name = self.name_to_html(name, rank=XSLT.strparam(rank))
        name = normalise_spaces(str(name))
        return name


@attr.s
class NslToDwcTaxonTransform(ThroughTransform):
    """
    Convert data in NSL form to Darwin Core
    """
    INVALID_COUNT = "invalid"

    reference: Port = attr.ib()
    invalid: Port = attr.ib()
    reference_keys: Keys = attr.ib()
    link_keys: Keys = attr.ib()
    defaultStatus: str = attr.ib()
    link_term: str = attr.ib()
    formatter: NameFormatter = attr.ib(factory=NameFormatter, kw_only=True)
    allow_unmatched: bool = attr.ib(default=False, kw_only=True)

    @classmethod
    def create(cls, id: str, input: Port, reference: Port, reference_keys, link_keys, defaultStatus: str, link_term: str, **kwargs):
        reference_keys = Keys.make_keys(reference.schema, reference_keys)
        link_keys = Keys.make_keys(reference.schema, link_keys) if link_keys is not None else None
        output = Port.port(dwc.schema.TaxonSchema())
        invalid = Port.port(input.schema)
        return NslToDwcTaxonTransform(id, input, output, None, reference, invalid, reference_keys, link_keys, defaultStatus, link_term, **kwargs)

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['reference'] =  self.reference
        return inputs

    def outputs(self) -> Dict[str, Port]:
        outputs = super().outputs()
        outputs['invalid'] = self.invalid
        return outputs

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        reference_records = context.acquire(self.reference)
        reference_index = Index.create(reference_records, self.reference_keys)
        result = Dataset.for_port(self.output)
        invalid = Dataset.for_port(self.invalid)
        errors = Dataset.for_port(self.error)
        additional = self.build_additional(context)
        for record in data.rows:
            try:
                reference = None
                link = self.link_keys.get(record) if self.link_keys is not None else None
                if link is not None:
                    reference = reference_index.find(record, self.link_keys) if link is not None else None
                if reference is None and link is not None and not self.allow_unmatched:
                    self.count(self.INVALID_COUNT, record, context)
                    error = Record.error(record, None, "Missing " + str(self.link_keys.keys) + " of " + str(self.link_keys.get(record)))
                    invalid.add(error)
                else:
                    if reference is not None and reference.taxonID == record.taxonID:
                        reference = None
                    composed = self.compose(record, reference, context, additional)
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
        context.save(self.invalid, invalid)
        context.save(self.error, errors)

    def compose(self, record: Record, reference: Record, context: ProcessingContext, additional) -> Record:
        """
        A DwC version of the record

        :param record: The original record
        :param reference: The reference record, either a parent or accepted record
        :param context: The processing context
        :param additional: Any additional context

        :return: A composed record, or null for no record
        """
        taxonID = record.taxonID
        if taxonID is None:
            raise ProcessingException("Record has no taxonID")
        scientificName = choose(record.name_canonicalName, record.canonicalName)
        if scientificName is None:
            raise ProcessingException("Record has no scientific name")
        taxonRank = record.mappedTaxonRank
        nameFormatted = self.formatter.format(record.name_scientificNameHTML, taxonRank)
        dwc = {
            'taxonID': taxonID,
            self.link_term: reference.taxonID if reference is not None else None,
            'datasetID': context.get_default('datasetID'),
            'nomenclaturalCode': choose(record.name_nomenclaturalCode, record.nomenclaturalCode, context.get_default('nomenclaturalCode')),
            'scientificName': scientificName,
            'scientificNameAuthorship': choose(record.name_scientificNameAuthorship, record.scientificNameAuthorship),
            'taxonRank': taxonRank,
            'taxonConceptID': record.taxonConceptID,
            'scientificNameID': record.scientificNameID,
            'taxonomicStatus': choose(record.mappedTaxonomicStatus, self.defaultStatus),
            'nomenclaturalStatus': record.nomenclaturalStatus,
            'establishmentMeans': None,
            'nameAccordingToID': record.nameAccordingToID,
            'nameAccordingTo': strip_markup(record.nameAccordingTo),
            'namePublishedInID': record.namePublishedInID,
            'namePublishedIn': strip_markup(record.name_namePublishedIn),
            'namePublishedInYear': record.name_namePublishedInYear,
            'nameComplete': choose(record.name_scientificName, record.scientificName),
            'nameFormatted': nameFormatted,
            'taxonRemarks': strip_markup(record.taxonRemarks),
            'provenance': None,
            'source': taxonID
        }
        errors = self.output.schema.validate(dwc)
        if errors:
            raise ProcessingException("Invalid mapping " + str(errors))
        return Record(record.line, dwc, record.issues)

@attr.s
class NslAdditionalToDwcTransform(ThroughTransform):
    """
    Convert data in NSL name form to Darwin Core
    """

    defaultStatus: str = attr.ib()
    formatter: NameFormatter = attr.ib(factory=NameFormatter, kw_only=True)
    allow_unmatched: bool = attr.ib(default=False, kw_only=True)

    @classmethod
    def create(cls, id: str, input: Port, defaultStatus: str, **kwargs):
        output = Port.port(dwc.schema.TaxonSchema())
        return NslAdditionalToDwcTransform(id, input, output, None, defaultStatus, **kwargs)

    def compose(self, record: Record, context: ProcessingContext, additional) -> Record:
        """
        A DwC version of the record

        :param record: The original record
        :param context: The processing context
        :param additional: Any additional context

        :return: A composed record, or null for no record
        """
        taxonID = record.scientificNameID
        if taxonID is None:
            raise ProcessingException("Record has no taxonID")
        scientificName = record.canonicalName
        if scientificName is None:
            raise ProcessingException("Record has no scientific name")
        taxonRank = record.mappedTaxonRank
        nameFormatted = self.formatter.format(record.scientificNameHTML, taxonRank)
        kingdom = record.kingdom
        if kingdom is None or len(kingdom) < 3:
            kingdom = 'Plantae' if record.taxonRankLevel > 200 else None
        dwc = {
            'taxonID': taxonID,
            'datasetID': context.get_default('datasetID'),
            'nomenclaturalCode': record.nomenclaturalCode,
            'scientificName': scientificName,
            'scientificNameAuthorship': record.scientificNameAuthorship,
            'kingdom': kingdom,
            'family': record.family,
            'genus': record.genericName,
            'specificEpithet': record.specificEpithet,
            'infraspecificEpithet': record.infraspecificEpithet,
            'taxonRank': taxonRank,
            'taxonConceptID': record.scientificNameID,
            'scientificNameID': record.scientificNameID,
            'taxonomicStatus': self.defaultStatus,
            'nomenclaturalStatus': record.nomenclaturalStatus,
            'establishmentMeans': None,
            'nameAccordingToID': record.nameAccordingToID,
            'nameAccordingTo': record.nameAccordingTo,
            'namePublishedInID': record.namePublishedInID,
            'namePublishedIn': record.name_namePublishedIn,
            'namePublishedInYear': record.name_namePublishedInYear,
            'nameComplete': record.scientificName,
            'nameFormatted': nameFormatted,
            'taxonRemarks': strip_markup(record.taxonRemarks),
            'provenance': None,
            'source': record.ccAttributionIRI
        }
        errors = self.output.schema.validate(dwc)
        if errors:
            raise ProcessingException("Invalid mapping " + str(errors))
        return Record(record.line, dwc, record.issues)


@attr.s
class VernacularToDwcTransform(ThroughTransform):
    """
    Convert data in NSL form to Darwin Core
    """
    INVALID_COUNT = "invalid"

    reference: Port = attr.ib()
    reference_keys: Keys = attr.ib()
    accepted_usage_keys: Keys = attr.ib()
    status: str = attr.ib(default='common')
    isPreferredName: bool = attr.ib(default=False)
    formatter: NameFormatter = attr.ib(factory=NameFormatter, kw_only=True)
    allow_unmatched: bool = attr.ib(default=False, kw_only=True)

    @classmethod
    def create(cls, id: str, input: Port, reference: Port, reference_keys, accepted_usage_keys, **kwargs):
        reference_keys = Keys.make_keys(reference.schema, reference_keys)
        accepted_usage_keys = Keys.make_keys(input.schema, accepted_usage_keys) if accepted_usage_keys is not None else None
        output = Port.port(dwc.schema.VernacularSchema())
        return VernacularToDwcTransform(id, input, output, None, reference, reference_keys, accepted_usage_keys, **kwargs)


    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['reference'] = self.reference
        return inputs

    def execute(self, context: ProcessingContext):
        data = context.acquire(self.input)
        reference_records = context.acquire(self.reference)
        reference_index = Index.create(reference_records, Keys.make_keys(reference_records.schema, self.reference_keys.keys), IndexType.MULTI)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        additional = self.build_additional(context)
        for record in data.rows:
            try:
                accepted = reference_index.find(record, self.accepted_usage_keys) if self.accepted_usage_keys is not None else None
                if accepted is None:
                        if not self.allow_unmatched:
                            self.count(self.INVALID_COUNT, record, context)
                            accepted = []
                        else:
                            accepted = [None]
                for acc in accepted:
                    composed = self.compose(record, acc, context, additional)
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

    def compose(self, record: Record, accepted: Record, context: ProcessingContext, additional) -> Record:
        """
        A DwC version of the record

        :param record: The original record
        :param parent: The parent record (null for none)
        :param context: The processing context
        :param additional: Any additional context

        :return: A composed record, or null for no record
        """
        if accepted is None:
            return None
        taxonID = accepted.taxonID
        if taxonID is None:
            raise ProcessingException("Record has no taxonID")
        dwc = {
            'taxonID': taxonID,
            'nameID': record.common_name_id,
            'datasetID': context.get_default('datasetID'),
            'vernacularName': record.common_name,
            'status': self.status,
            'language': context.get_default('language'),
            'countryCode': context.get_default('countryCode'),
            'isPreferredName': self.isPreferredName,
            'nameAccordingTo': record.citation,
            'source': record.ccAttributionIRI
        }
        errors = self.output.schema.validate(dwc)
        if errors:
            raise ProcessingException("Invalid mapping " + str(errors))
        return Record(record.line, dwc, record.issues)
