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
from typing import Dict

import attr

from ala.transform import PublisherSource, CollectorySource
from col.schema import ColTaxonSchema, ColDistributionSchema, ColVernacularSchema, ColAcceptedKingdomSchema, \
    ColAcceptedDatasetSchema, ColAcceptedLocationSchema, ColAcceptedLanguageSchema, ColTaxonWithClassificationSchema, \
    ColAcceptedRankSchema
from dwc.meta import MetaFile, EmlFile
from dwc.schema import TaxonSchema, VernacularSchema, TaxonomicStatusMapSchema, NomenclaturalCodeMapSchema, \
    LocationSchema, DistributionSchema, LocationIdentifierMapSchema
from dwc.transform import DwcTaxonValidate, DwcTaxonReidentify, DwcTaxonParent
from processing.dataset import Port, Index, Keys, Record, IndexType
from processing.node import ProcessingContext, NullNode
from processing.orchestrate import Orchestrator
from processing.sink import CsvSink
from processing.source import CsvSource, NullSource
from processing.transform import normalise_spaces, LookupTransform, Predicate, FilterTransform, MapTransform, choose, \
    TrailTransform, AcceptTransform, MergeTransform

MR_RECORD = re.compile("mrgid:(\\d+)")
TDWG_RECORD = re.compile("tdwg:([\\d\\w\\-]+)")
def id_records(r: Record) -> bool:
    locationID = r.locationID
    return locationID is not None and MR_RECORD.fullmatch(locationID) is not None


@attr.s
class ColUsePredicate(Predicate):
    """Select based on kingdom, dataset (optional) and distribution (optional)"""
    kingdoms: Port = attr.ib()
    datasets: Port = attr.ib()
    distributions: Port = attr.ib()
    ranks: Port = attr.ib()
    excluded_names: set = attr.ib()

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['kingdoms'] = self.kingdoms
        if self.datasets:
            inputs['datasets'] = self.datasets
        if self.distributions:
            inputs['distributions'] = self.distributions
        if self.ranks:
            inputs['ranks'] = self.ranks
        return inputs

    def begin(self, context: ProcessingContext):
        super().begin(context)
        kingdoms = context.acquire(self.kingdoms)
        self.kingdom_keys = Keys.make_keys(self.kingdoms.schema, 'kingdom')
        self.kingdom_index = Index.create(kingdoms, self.kingdom_keys)
        if self.ranks is not None:
            ranks = context.acquire(self.ranks)
            self.rank_keys = Keys.make_keys(self.ranks.schema, 'taxonRank')
            self.rank_index = Index.create(ranks, self.rank_keys, IndexType.FIRST)
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
        if self.ranks is not None and kr.useRank:
            rr = self.rank_index.find(record, self.rank_keys)
            if rr is None:
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
    return 'https://www.catalogueoflife.org/data/taxon/' + str(record.taxonID)


def reader(use_reference: bool, all_genus: bool) -> Orchestrator:
    """

    :param use_reference: Use the pre-computed reference data, rather than the whole lot
    :param all_genus: Accept all genera, otherwise only include certain kingdoms
    """
    taxon_file = "Taxon.tsv"
    distribution_file = "Distribution.tsv"
    vernacular_file = "VernacularName.tsv"
    accepted_kingdom_file = "Accepted_Kingdoms_Genus.csv" if all_genus else "Accepted_Kingdoms_Extra.csv"
    accepted_dataset_file = "Accepted_Datasets.csv"
    accepted_location_file = "Accepted_Locations.csv"
    accepted_rank_file = "Accepted_Ranks_Genus.csv" if all_genus else None
    accepted_language_file = "Accepted_Languages.csv"
    taxonomic_status_file = "Taxonomic_Status_Map.csv"
    nomenclautural_code_file = "Nomenclatural_Code_Map.csv"
    location_file = "Location.csv"
    location_identifier_file = "Location_Identifiers.csv"
    reference_file = "reference.csv"
    exclude_names = set(['Biota'])

    col_taxon_schema = ColTaxonSchema()
    col_taxon_with_classification_schema = ColTaxonWithClassificationSchema()
    col_distribution_schema = ColDistributionSchema()
    col_vernacular_schema = ColVernacularSchema()
    col_accepted_kingdom_schema = ColAcceptedKingdomSchema()
    col_accepted_dataset_schema = ColAcceptedDatasetSchema()
    col_accepted_rank_schema = ColAcceptedRankSchema()
    col_accepted_location_schema = ColAcceptedLocationSchema()
    col_accepted_language_schema = ColAcceptedLanguageSchema()
    col_taxonomic_status_map_schema = TaxonomicStatusMapSchema()
    col_nomenclatural_code_map_schema = NomenclaturalCodeMapSchema()
    location_schema = LocationSchema()
    location_identifier_map_schema = LocationIdentifierMapSchema()

    with Orchestrator('col') as orchestrator:
        # Only use those taxa from a list of accepted kingdoms and, for some kingdoms, specific locations and datasets
        accepted_kingdoms = CsvSource.create("accepted_kingdoms", accepted_kingdom_file, "ala", col_accepted_kingdom_schema)
        accepted_datasets = CsvSource.create("accepted_datasets", accepted_dataset_file, "ala", col_accepted_dataset_schema)
        accepted_languages = CsvSource.create("accepted_languages", accepted_language_file, "ala", col_accepted_language_schema)
        accepted_ranks = CsvSource.create("accepted_ranks", accepted_rank_file, "ala", col_accepted_rank_schema) if all_genus else NullSource.create('accepted_ranks', col_accepted_rank_schema)
        taxonomic_status_map = CsvSource.create("taxonomic_status_map", taxonomic_status_file, "ala", col_taxonomic_status_map_schema)
        nomenclautural_code_map = CsvSource.create("nomenclatural_code_map", nomenclautural_code_file, "ala", col_nomenclatural_code_map_schema)
        # Initial use predicate - filter by kingdom
        if use_reference:
            taxon_source = NullNode.create('taxon_source')
            taxon_with_kingdom = NullNode.create("taxon_with_kingdom")
            col_filter_predicate = NullNode.create("col_filter")
            taxon_use = NullNode.create('taxon_use')
            taxon_used_reference = NullNode('taxon_used_reference')
            taxon_trail = NullNode('taxon_trail')
            taxon_synonyms = NullNode('taxon_synonyms')
            taxon_synonyms_new = NullNode('taxon_synonyms_new')
            taxon_complete = CsvSource.create("taxon_complete", reference_file, 'excel', col_taxon_with_classification_schema, no_errors=False)
        else:
            taxon_source = CsvSource.create("taxon_source", taxon_file, 'col', col_taxon_schema, no_errors=False, encoding='utf-8-sig', post_gc=True)
            taxon_with_kingdom = DwcTaxonParent.create('taxon_with_kingdom', taxon_source.output, 'taxonID', 'parentNameUsageID', 'acceptedNameUsageID', 'scientificName', 'scientificNameAuthorship', 'taxonRank', kingdoms=accepted_kingdoms.output)
            # Only include taxa by kingdom, dataset, distribution, rank
            col_filter_predicate = ColUsePredicate('col_filter', accepted_kingdoms.output, None, None, accepted_ranks.output, exclude_names)
            taxon_use = FilterTransform.create("taxon_use", taxon_with_kingdom.output, col_filter_predicate)
            taxon_trail = TrailTransform.create("taxon_trail", taxon_use.output, taxon_with_kingdom.output, 'taxonID',
                                                'parentNameUsageID', 'acceptedNameUsageID', col_filter_predicate)
            # Load synonyms
            taxon_synonyms = AcceptTransform.create('taxon_synonyms', taxon_with_kingdom.output, taxon_trail.output, 'acceptedNameUsageID', 'taxonID')
            taxon_synonyms_new = AcceptTransform.create('taxon_synonyms_new', taxon_synonyms.output, taxon_trail.output, 'taxonID', 'taxonID', exclude=True)
            taxon_complete = MergeTransform.create('taxon_complete', taxon_trail.output, taxon_synonyms_new.output)
            # Use the reference one for faster loading if you have run this once.
            #taxon_source = CsvSource.create("taxon_source", 'reference.csv', 'excel', col_taxon_schema, no_errors=False, predicate=col_use_predciate)
            CsvSink.create("taxon_used_reference", taxon_complete.output, "reference.csv", "excel", work=True)
        # Initial distro predicate - allowed locations
        # col_location_predicate = ColLocationPredicate('col_location_use', accepted_locations.output)
        taxon_status_mapped = LookupTransform.create("taxon_status_mapped", taxon_complete.output, taxonomic_status_map.output, 'taxonomicStatus', 'Term', lookup_map={'DwC': 'mappedTaxonomicStatus'})
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
            'source': 'taxonID'
        }, auto=True)
        taxon_validate = DwcTaxonValidate.create("taxon_validate", taxon_map.output, check_names=False)
        taxon_output = CsvSink.create("taxon_output", taxon_validate.output, "taxon.csv", "excel", reduce=True)
        taxon_mapping = CsvSink.create('taxon_mapping', taxon_reidentify.mapping, "identifier_mapping.csv", "excel", work=True)

        vernacular_source = CsvSource.create("vernacular_source", vernacular_file, 'col', col_vernacular_schema, no_errors=False, encoding='utf-8-sig')
        vernacular_language = LookupTransform.create("vernacular_language", vernacular_source.output, accepted_languages.output, 'language', 'language', reject=True)
        vernacular_use = LookupTransform.create("vernacular_use", vernacular_language.output, taxon_complete.output, 'taxonID', 'taxonID', reject=True, merge=False)
        vernacular_reidentify = LookupTransform.create("vernacular_reidentify", vernacular_use.output, taxon_reidentify.mapping, 'taxonID', 'term', lookup_map={'mapping': 'mappedTaxonID'})
        vernacular_map = MapTransform.create("vernacular_map", vernacular_reidentify.output, VernacularSchema(), {
            'taxonID': 'mappedTaxonID',
            'nameID': lambda r: 'col:vernacular:' + str(r.line),
            'datasetID': MapTransform.default('datasetID'),
            'language': 'code',
            'status': MapTransform.constant('common'),
            'isPreferredName': MapTransform.constant(False)
        }, auto=True)
        vernacular_output = CsvSink.create("vernacular_output", vernacular_map.output, "vernacularName.csv", "excel", reduce=True)

        distribution_source = CsvSource.create("distribution_source", distribution_file, 'col', col_distribution_schema, no_errors=False, encoding='utf-8-sig', predicate=id_records)
        location = CsvSource.create("location", location_file, 'ala', location_schema)
        location_identifier_source = CsvSource.create("location_identifier_source", location_identifier_file, 'ala', location_identifier_map_schema)
        location_identifier_map = LookupTransform.create("location_identifier_map", location_identifier_source.output, location.output, 'locationID', 'locationID', lookup_prefix='c_')
        distribution_used = AcceptTransform.create('distribution_used', distribution_source.output, taxon_complete.output, 'taxonID', 'taxonID')
        dwc_distribution = LookupTransform.create('dwc_distribution', distribution_used.output, location_identifier_map.output, 'locationID', 'identifier', record_unmatched=True, lookup_prefix='m_')
        dwc_distribution_reidentify = LookupTransform.create("dwc_distribution_reidentify", dwc_distribution.output, taxon_reidentify.mapping, 'taxonID', 'term', lookup_map={'mapping': 'mappedTaxonID'}, reject=True)
        dwc_distribution_mapped = MapTransform.create("dwc_distribution_mapped", dwc_distribution_reidentify.output, DistributionSchema(), {
            'taxonID': 'mappedTaxonID',
            'locationID': MapTransform.choose('m_locationID', 'locationID'),
            'locality': 'm_mappedLocality',
            'countryCode': 'countryCode',
            'establishmentMeans': 'occurrenceStatus',
            'datasetID': MapTransform.default('datasetID'),
            'provenance': lambda r: f"Original locationID {r.locationID}" + ('' if r.m_mappedLocality == r.m_locality else f" locality {r.m_locality}")
        })
        dwc_distribution_output = CsvSink.create("distribution_output", dwc_distribution_mapped.output, "distribution.csv", "excel", reduce=True)

        MetaFile.create('dwc_meta', taxon_output, vernacular_output, dwc_distribution_output)
        publisher = PublisherSource.create('publisher')
        metadata = CollectorySource.create('metadata')
        EmlFile.create('dwc_eml', metadata.output, publisher.output)
    return orchestrator

