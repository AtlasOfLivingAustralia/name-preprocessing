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

from typing import Dict

import attr

from ala.transform import PublisherSource, CollectorySource
from col.schema import ColTaxonSchema, ColDistributionSchema, ColVernacularSchema, ColAcceptedKingdomSchema, \
    ColAcceptedDatasetSchema, ColAcceptedLocationSchema, ColAcceptedLanguageSchema, ColNomenclaturalCodeMapSchema
from dwc.meta import MetaFile, EmlFile
from dwc.schema import TaxonSchema, VernacularSchema, TaxonomicStatusMapSchema
from dwc.transform import DwcTaxonValidate, DwcTaxonTrail, DwcTaxonReidentify
from processing.dataset import Port, Index, Keys, Record, IndexType
from processing.node import ProcessingContext
from processing.orchestrate import Orchestrator
from processing.sink import CsvSink
from processing.source import CsvSource
from processing.transform import normalise_spaces, LookupTransform, Predicate, FilterTransform, MapTransform, choose


@attr.s
class ColUsePredicate(Predicate):
    """Select based on kingdom, dataset (optional) and distribution (optional)"""
    kingdoms: Port = attr.ib()
    datasets: Port = attr.ib()
    distributions: Port = attr.ib()

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['kingdoms'] = self.kingdoms
        if self.datasets:
            inputs['datasets'] = self.datasets
        if self.distributions:
            inputs['distributions'] = self.distributions
        return inputs

    def begin(self, context: ProcessingContext):
        super().begin(context)
        kingdoms = context.acquire(self.kingdoms)
        self.kingdom_keys = Keys.make_keys(self.kingdoms.schema, 'kingdom')
        self.kingdom_index = Index.create(kingdoms, self.kingdom_keys)
        if self.datasets is not None:
            datasets = context.acquire(self.datasets)
            self.dataset_keys = Keys.make_keys(self.datasets.schema, 'datasetID')
            self.dataset_index = Index.create(datasets, self.dataset_keys, IndexType.FIRST)
        if self.distributions is not None:
            distrbutions = context.acquire(self.distributions)
            self.distribution_keys = Keys.make_keys(self.distributions.schema, 'taxonID')
            self.distribution_index = Index.create(distrbutions, self.distribution_keys,  IndexType.FIRST)

    def execute(self, context: ProcessingContext):
        pass

    def test(self, record: Record):
        kr = self.kingdom_index.find(record, self.kingdom_keys)
        if kr is None:
            return False
        if self.datasets is not None and kr.useDataset:
            dr = self.dataset_index.find(record, self.dataset_keys)
            if dr is None:
                return False
        if self.distributions is not None and kr.useDistribution:
            xr = self.distribution_index.find(record, self.distribution_keys)
            if xr is None:
                return False
        return True

@attr.s
class ColLocationPredicate(Predicate):
    """Select based on kingdom, dataset (optional) and distribution (optional)"""
    locations: Port = attr.ib()

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['locations'] = self.locations
        return inputs

    def begin(self, context: ProcessingContext):
        super().begin(context)
        locations = context.acquire(self.locations)
        self.location_keys = Keys.make_keys(self.locations.schema, 'locationID')
        self.location_index = Index.create(locations, self.location_keys, IndexType.FIRST)

    def execute(self, context: ProcessingContext):
        pass

    def test(self, record: Record):
        lr = self.location_index.find(record, self.location_keys)
        if lr is None:
            return False
        return True

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

def make_identifier(record: Record):
    return record.identifier if record.identifier is not None else 'CoL:' + str(record.taxonID)


def reader() -> Orchestrator:
    taxon_file = "taxa.txt"
    distribution_file = "distribution.txt"
    vernacular_file = "vernacular.txt"
    kingdom_file = "Accepted_Kingdoms.csv"
    dataset_file = "Accepted_Datasets.csv"
    location_file = "Accepted_Locations.csv"
    language_file = "Accepted_Languages.csv"
    taxonomic_status_file = "Taxonomic_Status_Map.csv"
    nomenclautural_code_file = "Nomenclatural_Code_Map.csv"

    col_taxon_schema = ColTaxonSchema()
    col_distribution_schema = ColDistributionSchema()
    col_vernacular_schema = ColVernacularSchema()
    col_accepted_kingdom_schema = ColAcceptedKingdomSchema()
    col_accepted_dataset_schema = ColAcceptedDatasetSchema()
    col_accepted_location_schema = ColAcceptedLocationSchema()
    col_accepted_language_schema = ColAcceptedLanguageSchema()
    col_taxonomic_status_map_schema = TaxonomicStatusMapSchema()
    col_nomenclatural_code_map_schema = ColNomenclaturalCodeMapSchema()

    # Only use those taxa from a list of accepted kingdoms and, for some kingdoms, specific locations and datasets
    accepted_kingdoms = CsvSource.create("accepted_kingdoms", kingdom_file, "ala", col_accepted_kingdom_schema)
    accepted_datasets = CsvSource.create("accepted_datasets", dataset_file, "ala", col_accepted_dataset_schema)
    accepted_locations = CsvSource.create("accepted_locations", location_file, "ala", col_accepted_location_schema)
    accepted_languages = CsvSource.create("accepted_languages", language_file, "ala", col_accepted_language_schema)
    taxonomic_status_map = CsvSource.create("taxonomic_status_map", taxonomic_status_file, "ala", col_taxonomic_status_map_schema)
    nomenclautural_code_map = CsvSource.create("nomenclatural_code_map", nomenclautural_code_file, "ala", col_nomenclatural_code_map_schema)
    # Initial use predicate - filter by kingdom
    col_use_predciate = ColUsePredicate('col_kingdom_use', accepted_kingdoms.output, None, None)
    taxon_source = CsvSource.create("taxon_source", taxon_file, 'col', col_taxon_schema, no_errors=False, encoding='utf-8-sig', predicate=col_use_predciate)
    # Use the reference one for faster loading if you have run this once.
    #taxon_source = CsvSource.create("taxon_source", 'reference.csv', 'excel', col_taxon_schema, no_errors=False, predicate=col_use_predciate)
    taxon_source_reference = CsvSink.create("taxon_source_reference", taxon_source.output, "reference.csv", "excel", work=True)
    # Initial distro predicate - allowed locations
    col_location_predicate = ColLocationPredicate('col_location_use', accepted_locations.output)
    distribution_source = CsvSource.create("distribution_source", distribution_file, 'col', col_distribution_schema, no_errors=False, encoding='utf-8-sig', predicate=col_location_predicate)
    # Only include taxa by kingdom, dataset, distribution
    col_filter_predicate = ColUsePredicate('col_use', accepted_kingdoms.output, accepted_datasets.output, distribution_source.output)
    taxon_use = FilterTransform.create("taxon_use", taxon_source.output, col_filter_predicate)
    taxon_trail = DwcTaxonTrail.create("taxon_trail", taxon_use.output, taxon_source.output, 'taxonID', 'parentNameUsageID', 'acceptedNameUsageID')
    taxon_status_mapped = LookupTransform.create("taxon_status_mapped", taxon_trail.output, taxonomic_status_map.output, 'taxonomicStatus', 'Term', lookup_map={'DwC': 'mappedTaxonomicStatus'})
    taxon_code_mapped = LookupTransform.create("nomenclatural_code_mapped", taxon_status_mapped.output, nomenclautural_code_map.output, 'kingdom', 'kingdom')
    taxon_reidentify = DwcTaxonReidentify.create("taxon_reidentify", taxon_code_mapped.output, 'taxonID', 'parentNameUsageID', 'acceptedNameUsageID', make_identifier)
    taxon_map = MapTransform.create("taxon_map", taxon_reidentify.output, TaxonSchema(), {
        'datasetID': MapTransform.default('datasetID'),
        'scientificName': lambda r: clean_scientific(r.scientificName, r.scientificNameAuthorship),
        'kingdom': lambda r: clean_name(r.kingdom),
        'phylum': lambda r: clean_name(r.phylum),
        'class_': lambda r: clean_name(r.class_),
        'order': lambda r: clean_name(r.order),
        'family': lambda r: clean_name(r.family),
        'genus': lambda r: clean_name(r.genus),
        'specificEpithet': lambda r: clean_name(r.specificEpithet),
        'infraspecificEpithet': lambda r: clean_name(r.infraspecificEpithet),
        'taxonomicStatus': lambda r: choose(r.mappedTaxonomicStatus, r.taxonomicStatus, 'inferredSynonym' if r.acceptedNameUsageID is not None else 'inferredAccepted'),
        'source': 'references'
    }, auto=True)
    taxon_validate = DwcTaxonValidate.create("taxon_validate", taxon_map.output)
    taxon_output = CsvSink.create("taxon_output", taxon_validate.output, "taxon.csv", "excel", reduce=True)
    vernacular_source = CsvSource.create("vernacular_source", vernacular_file, 'col', col_vernacular_schema, no_errors=False, encoding='utf-8-sig')
    vernacular_language = LookupTransform.create("vernacular_laguage", vernacular_source.output, accepted_languages.output, 'language', 'language', reject=True)
    vernacular_use = LookupTransform.create("vernacular_use", vernacular_language.output, taxon_trail.output, 'taxonID', 'taxonID', reject=True, merge=False)
    vernacular_reidentify = LookupTransform.create("vernacular_reidentify", vernacular_use.output, taxon_reidentify.mapping, 'taxonID', 'term', lookup_map={'mapping': 'mappedTaxonID'})
    vernacular_map = MapTransform.create("vernacular_map", vernacular_reidentify.output, VernacularSchema(), {
        'taxonID': 'mappedTaxonID',
        'nameID': lambda r: 'CoL_V_' + str(r.line),
        'datasetID': MapTransform.default('datasetID'),
        'language': 'code',
        'status': MapTransform.constant('common'),
        'isPreferredName': MapTransform.constant(False)
    }, auto=True)
    vernacular_output = CsvSink.create("vernacular_output", vernacular_map.output, "vernacularName.csv", "excel", reduce=True)
    dwc_meta = MetaFile.create('dwc_meta', taxon_output, vernacular_output)
    publisher = PublisherSource.create('publisher')
    metadata = CollectorySource.create('metadata')
    dwc_eml = EmlFile.create('dwc_eml', metadata.output, publisher.output)

    orchestrator = Orchestrator("col",
                                [
                                    accepted_kingdoms,
                                    accepted_datasets,
                                    accepted_locations,
                                    accepted_languages,
                                    taxonomic_status_map,
                                    nomenclautural_code_map,
                                    col_use_predciate,
                                    col_location_predicate,
                                    taxon_source,
                                    taxon_source_reference,
                                    distribution_source,
                                    col_filter_predicate,
                                    taxon_use,
                                    taxon_trail,
                                    taxon_status_mapped,
                                    taxon_code_mapped,
                                    taxon_reidentify,
                                    taxon_validate,
                                    taxon_map,
                                    taxon_output,
                                    vernacular_source,
                                    vernacular_language,
                                    vernacular_use,
                                    vernacular_reidentify,
                                    vernacular_map,
                                    vernacular_output,
                                    dwc_meta,
                                    metadata,
                                    publisher,
                                    dwc_eml
                                ])
    return orchestrator
