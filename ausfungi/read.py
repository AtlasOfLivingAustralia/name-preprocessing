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

import uuid

from ala.transform import PublisherSource, CollectorySource
from ausfungi.schema import AusFungiTaxonSchema, AusFungiIdentifierSchema
from dwc.meta import MetaFile, EmlFile
from dwc.schema import TaxonSchema, TaxonomicStatusMapSchema
from dwc.transform import DwcTaxonValidate, DwcTaxonReidentify, DwcTaxonClean
from processing.dataset import Record
from processing.orchestrate import Orchestrator
from processing.sink import CsvSink
from processing.source import CsvSource
from processing.transform import normalise_spaces, LookupTransform, FilterTransform, MapTransform, choose


def clean_author(name: str, author: str):
    index = name.find(author)
    if index > 0:
        name = name[:index] + ' ' + name[index + len(author):]
    return name

def clean_scientific(name: str, author: str):
    if author is None:
        return name
    name = clean_author(name, '(' + author + ')')
    name = clean_author(name, ' ' + author)
    return normalise_spaces(name)

def clean_name(name: str):
    return None if name == 'Not assigned' else name

def remap_taxonid(record: Record):
    if record.taxonomicStatus == 'invalid':
        return str(uuid.uuid4())
    return record.taxonID

def make_identifier(record: Record):
    return record.identifier if record.identifier is not None else record.taxonID

def use_taxon(record: Record):
    return record.taxonRank != 'life'

def clean_taxonomic_status(record: Record):
    if record.occurrenceStatus == '[Not in Australia]':
        return 'excluded'
    return choose(record.status_DwC, record.taxonomicStatus)

def reader() -> Orchestrator:
    taxon_file = "taxon.csv"
    identifier_file = "identifier.csv"
    taxonomic_status_file = "Taxonomic_Status_Map.csv"

    fungi_taxon_schema = AusFungiTaxonSchema()
    fungi_identifer_schema = AusFungiIdentifierSchema()
    taxonomic_status_map_schema = TaxonomicStatusMapSchema()
    taxonomic_status_map = CsvSource.create("taxonomic_status_map", taxonomic_status_file, "ala", taxonomic_status_map_schema)
    taxon_source = CsvSource.create("taxon_source", taxon_file, 'excel', fungi_taxon_schema, no_errors=False)
    identifier_source = CsvSource.create('identifier_source', identifier_file, 'excel', fungi_identifer_schema, no_errors=False)
    taxon_identified = LookupTransform.create('taxon_identified', taxon_source.output, identifier_source.output, 'taxonID', 'coreid')
    taxon_status = LookupTransform.create('taxon_status', taxon_identified.output, taxonomic_status_map.output, 'taxonomicStatus', 'Term', lookup_prefix="status_")
    taxon_used = FilterTransform.create('taxon_used', taxon_status.output, use_taxon)
    taxon_map = MapTransform.create("taxon_map", taxon_used.output, TaxonSchema(), {
        'datasetID': MapTransform.constant('datasetID'),
        'parentNameUsageID': lambda r: r.parentNameUsageID if r.status_Accepted else None,
        'acceptedNameUsageID': lambda r: r.acceptedNameUsageID if not r.status_Accepted else None,
        'scientificName': lambda r: clean_scientific(r.scientificName, r.scientificNameAuthorship),
        'taxonomicStatus': clean_taxonomic_status,
        'source': lambda r: r.identifier if r.identifier is not None and r.identifier.startswith('http') else None,
    }, auto=True)
    taxon_reidentify = DwcTaxonReidentify.create("taxon_reidentify", taxon_map.output, 'taxonID', 'parentNameUsageID', 'acceptedNameUsageID', make_identifier)
    taxon_clean = DwcTaxonClean.create("taxon_clean", taxon_reidentify.output)
    taxon_validate = DwcTaxonValidate.create("taxon_validate", taxon_clean.output)
    taxon_output = CsvSink.create("taxon_output", taxon_validate.output, "taxon.csv", "excel", reduce=True)
    dwc_meta = MetaFile.create('dwc_meta', taxon_output)
    publisher = PublisherSource.create('publisher')
    metadata = CollectorySource.create('metadata')
    dwc_eml = EmlFile.create('dwc_eml', metadata.output, publisher.output)

    orchestrator = Orchestrator("ausfungi",
                                [
                                    taxonomic_status_map,
                                    taxon_source,
                                    identifier_source,
                                    taxon_identified,
                                    taxon_status,
                                    taxon_used,
                                    taxon_reidentify,
                                    taxon_clean,
                                    taxon_validate,
                                    taxon_map,
                                    taxon_output,
                                    dwc_meta,
                                    metadata,
                                    publisher,
                                    dwc_eml
                                ])
    return orchestrator
