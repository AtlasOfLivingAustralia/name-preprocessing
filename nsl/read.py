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

from ala.transform import PublisherSource, CollectorySource
from dwc.meta import MetaFile, EmlFile
from dwc.schema import NomenclaturalCodeMapSchema, DistributionSchema, EstablishmentMeansMapSchema, LocationMapSchema, \
    LocationSchema, VernacularStatusSchema
from dwc.transform import DwcIdentifierGenerator, DwcIdentifierTranslator, DwcSyntheticNames, DwcVernacularStatus, \
    DwcDefaultDistribution
from nsl.schema import TaxonSchema, NameSchema, CommonNameSchema, RankMapSchema, TaxonomicStatusMapSchema
from nsl.todwc import VernacularToDwcTransform, NslToDwcTaxonTransform, NslAdditionalToDwcTransform
from processing.dataset import Record, IndexType
from processing.orchestrate import Orchestrator
from processing.sink import CsvSink
from processing.source import CsvSource
from processing.transform import FilterTransform, LookupTransform, MergeTransform, DenormaliseTransform, MapTransform, \
    ProjectTransform
import re

LOC_AND_ER = re.compile(r'\s*([A-Za-z]+)\s+\(([a-z\s]+)\)\s*')

def is_scientific_taxon(record: Record):
    return record.taxonomicStatus != 'common name'

def is_accepted_taxon(record: Record):
    status = record.Accepted
    return status is not None and status

def is_synonym_taxon(record: Record):
    status = record.Synonym
    return status is not None and status

def is_misapplied_taxon(record: Record):
    status = record.Misapplied
    return status is not None and status

def is_unplaced_taxon(record: Record):
    status = record.Unplaced
    return status is not None and status

def is_excluded_taxon(record: Record):
    status = record.Excluded
    return status is not None and status

def is_unknown_taxon(record: Record):
    return not is_accepted_taxon(record) and not is_synonym_taxon(record) and not is_misapplied_taxon(record) and not is_unplaced_taxon(record) and not is_excluded_taxon(record)

def is_placed_name(record: Record):
    return not is_vernacular_name(record) and record.taxonomicStatus != 'unplaced'

def is_vernacular_name(record: Record):
    return record.nameType == 'common' or record.nameType == 'vernacular'

def is_unplaced_name(record: Record):
    return not is_vernacular_name(record) and record.taxonomicStatus == 'unplaced'

def extract_location(record: Record):
    loc: str = record.taxonDistribution
    if loc is None:
        return None
    match = LOC_AND_ER.match(loc)
    if not match:
        return loc.strip()
    return match.group(1)

def extract_establishment_means(record: Record):
    loc: str = record.taxonDistribution
    if loc is None:
        return None
    match = LOC_AND_ER.match(loc)
    if not match:
        return None
    return match.group(2)

def reader() -> Orchestrator:
    taxon_file = "taxon.csv"
    name_file = "names.csv"
    vernacular_name_file = "common-names.csv"
    taxonomic_status_map_file = "Taxonomic_Status_Map.csv"
    nomenclatural_code_map_file = "Nomenclatural_Code_Map.csv"
    establishment_means_map_file = "Establishment_Means_Map.csv"
    location_map_file = "Location_Lookup.csv"
    location_file = "Location.csv"
    vernacular_status_file = "Vernacular_Status.csv"
    rank_map_file = "ranks.csv"
    taxon_schema = TaxonSchema()
    name_schema = NameSchema()
    common_name_schema = CommonNameSchema()
    rank_map_schema = RankMapSchema()
    taxonomic_status_schema = TaxonomicStatusMapSchema()
    nomenclatural_code_schema = NomenclaturalCodeMapSchema()
    distribution_schema = DistributionSchema()
    establishment_means_schema = EstablishmentMeansMapSchema()
    location_map_schema = LocationMapSchema()
    location_schema = LocationSchema()
    vernacular_status_schema = VernacularStatusSchema()

    taxon_source = CsvSource.create("taxon_source", taxon_file, "excel", taxon_schema, no_errors=False, fail_on_exception=True)
    scientific_taxon = FilterTransform.create("scientific_taxon", taxon_source.output, is_scientific_taxon)
    rank_source = CsvSource.create("rank_source", rank_map_file, "ala", rank_map_schema)
    taxon_rank_lookup = LookupTransform.create("taxon_rank_lookup", scientific_taxon.output, rank_source.output, 'taxonRank', 'term', reject=True, record_unmatched=True, lookup_map={'taxonRank': 'mappedTaxonRank', 'taxonRankLevel': 'taxonRankLevel' })
    nomenclatural_code_map = CsvSource.create("nomenclatural_code_map", nomenclatural_code_map_file, "ala", nomenclatural_code_schema)
    establishment_means_map = CsvSource.create("establishment_means_map", establishment_means_map_file, "ala", establishment_means_schema)
    location_map = CsvSource.create("location_map", location_map_file, "ala", location_map_schema)
    location = CsvSource.create("location", location_file, "ala", location_schema)
    taxon_coded = LookupTransform.create('taxon_coded', taxon_rank_lookup.output, nomenclatural_code_map.output, 'kingdom', 'kingdom', record_unmatched=True, lookup_map={'nomenclaturalCode': 'kingdom_nomenclaturalCode', 'taxonomicFlags': 'taxonomicFlags'})
    status_source = CsvSource.create("status_source", taxonomic_status_map_file, "ala", taxonomic_status_schema)
    taxon_status_lookup = LookupTransform.create("taxon_status_lookup", taxon_coded.output, status_source.output, 'taxonomicStatus', 'Term', reject=True, record_unmatched=True, lookup_map={'DwC': 'mappedTaxonomicStatus'}, lookup_include=['Accepted', 'Synonym', 'Misapplied', 'Unplaced', 'Excluded'])
    accepted_taxon = FilterTransform.create('accepted_taxon', taxon_status_lookup.output, is_accepted_taxon)
    synonym_taxon = FilterTransform.create('synonym_taxon', taxon_status_lookup.output, is_synonym_taxon)
    misapplied_taxon = FilterTransform.create('misapplied_taxon', taxon_status_lookup.output, is_misapplied_taxon)
    unplaced_taxon = FilterTransform.create('unplaced_taxon', taxon_status_lookup.output, is_unplaced_taxon)
    excluded_taxon = FilterTransform.create('excluded_taxon', taxon_status_lookup.output, is_excluded_taxon)
    unknown_taxon = FilterTransform.create('unknown_taxon', taxon_status_lookup.output, is_unknown_taxon)
    reference_taxon = MergeTransform.create("reference_taxon", accepted_taxon.output, synonym_taxon.output, misapplied_taxon.output, excluded_taxon.output)

    name_source = CsvSource.create("name_source", name_file, "excel", name_schema, no_errors=False)
    placed_name = FilterTransform.create('placed_name', name_source.output, is_placed_name)
    vernacular_name = FilterTransform.create('vernacular_name', name_source.output, is_vernacular_name)
    unused_name = FilterTransform.create('unused_name', name_source.output, is_unplaced_name)

    accepted_name = LookupTransform.create('accepted_name', accepted_taxon.output, placed_name.output, 'scientificNameID', 'scientificNameID', record_unmatched=True, lookup_prefix='name_', lookup_type=IndexType.FIRST)
    synonym_name = LookupTransform.create('synonym_name', synonym_taxon.output, placed_name.output, 'scientificNameID', 'scientificNameID', record_unmatched=True, lookup_prefix='name_',  lookup_type=IndexType.FIRST)
    misapplied_name = LookupTransform.create('misapplied_name', misapplied_taxon.output, placed_name.output, 'scientificNameID', 'scientificNameID', record_unmatched=True, lookup_prefix='name_',  lookup_type=IndexType.FIRST)
    unplaced_name = LookupTransform.create('unplaced_name', unplaced_taxon.output, placed_name.output, 'scientificNameID', 'scientificNameID', record_unmatched=True, lookup_prefix='name_',  lookup_type=IndexType.FIRST)
    excluded_name = LookupTransform.create('excluded_name', excluded_taxon.output, placed_name.output, 'scientificNameID', 'scientificNameID', record_unmatched=True, lookup_prefix='name_',  lookup_type=IndexType.FIRST)

    accepted_dwc = NslToDwcTaxonTransform.create("accepted_dwc", accepted_name.output, reference_taxon.output, 'taxonID', 'parentNameUsageID', 'accepted', 'parentNameUsageID')
    synonym_dwc = NslToDwcTaxonTransform.create("synonym_dwc", synonym_name.output, reference_taxon.output, 'taxonID', 'acceptedNameUsageID', 'synonym', 'acceptedNameUsageID')
    misapplied_dwc = NslToDwcTaxonTransform.create("misapplied_dwc", misapplied_name.output, reference_taxon.output, 'taxonID', 'acceptedNameUsageID', 'misapplied', 'acceptedNameUsageID', allow_unmatched=True)
    excluded_dwc = NslToDwcTaxonTransform.create("excluded_dwc", excluded_name.output, reference_taxon.output, 'taxonID', 'acceptedNameUsageID', 'excluded', 'acceptedNameUsageID', allow_unmatched=True)
    taxon_dwc = MergeTransform.create("merge_dwc", accepted_dwc.output, synonym_dwc.output, misapplied_dwc.output, excluded_dwc.output)
    taxon_dwc_output = CsvSink.create("taxon_dwc_output", taxon_dwc.output, "taxon.csv", "excel")

    vernacular_source = CsvSource.create('vernacular_source', vernacular_name_file, "excel", common_name_schema, no_errors=False)
    vernacular_dwc = VernacularToDwcTransform.create('vernacular_dwc', vernacular_source.output, taxon_dwc.output, 'scientificNameID', 'scientific_name_id')
    vernacular_status = CsvSource.create("vernacular_status", vernacular_status_file, "ala", vernacular_status_schema)
    vernacular_dwc_filtered = DwcVernacularStatus.create("vernacular_dwc_filtered", vernacular_dwc.output, vernacular_status.output)
    vernacular_dwc_output = CsvSink.create("vernacular_dwc_output", vernacular_dwc_filtered.output, "vernacularName.csv", "excel")

    dwc_identifier = DwcIdentifierGenerator.create('dwc_identifier', taxon_dwc.output, 'taxonID', 'taxonID',
        DwcIdentifierTranslator.regex('^https://', 'http://', title = 'Taxon', status = 'variant'),
        fail_on_exception = True
    )
    dwc_identifier_output = CsvSink.create("dwc_identifier_output", dwc_identifier.output, "identifier.csv", "excel",reduce=True)

    distribution = DenormaliseTransform.delimiter('distribution', reference_taxon.output, 'taxonDistribution', ',')
    dwc_distribution_base = MapTransform.create('distribution_dwc', distribution.output, distribution_schema, {
        'taxonID': 'taxonID',
        'datasetID': MapTransform.default('datasetID'),
        'locality': extract_location,
        'establishmentMeans': extract_establishment_means
    }, )
    dwc_distribution_location_id = LookupTransform.create('distribution_location_id', dwc_distribution_base.output, location_map.output, 'locality', 'locality', lookup_include = ['locationID'], lookup_type=IndexType.FIRST, record_unmatched=True)
    dwc_distribution_location = LookupTransform.create('distribution_location', dwc_distribution_location_id.output, location.output, 'locationID', 'locationID', lookup_include = ['locationID', 'locality'], overwrite=True, record_unmatched=True)
    dwc_distribution = LookupTransform.create('distribution_establishment_means', dwc_distribution_location.output, establishment_means_map.output, 'establishmentMeans', 'term', lookup_include = ['establishmentMeans'], overwrite=True, record_unmatched=True)
    dwc_default_distribution = DwcDefaultDistribution.create('default_distribution', taxon_dwc.output, dwc_distribution.output, location.output)
    dwc_merged_distribution = MergeTransform.create('merged_distribution', dwc_default_distribution.output, dwc_distribution.output)
    dwc_projected_distribution = ProjectTransform.create('projected_distribution', dwc_merged_distribution.output, DistributionSchema())
    dwc_distribution_output = CsvSink.create("distribution_output", dwc_projected_distribution.output, "distribution.csv", "excel", reduce=True)

    dwc_meta = MetaFile.create('dwc_meta', taxon_dwc_output, vernacular_dwc_output, dwc_identifier_output, dwc_distribution_output)
    publisher = PublisherSource.create('publisher')
    metadata = CollectorySource.create('metadata')
    dwc_eml = EmlFile.create('dwc_eml', metadata.output, publisher.output)

    unplaced_taxon_output = CsvSink.create("unplaced_taxon_output", unplaced_taxon.output, "unplaced_taxon.csv", "excel", True)
    unused_name_output = CsvSink.create("unused_name_output", unused_name.output, "unused_name.csv", "excel", True)

    orchestrator = Orchestrator("nsl",
                                [
                                    rank_source,
                                    status_source,
                                    nomenclatural_code_map,
                                    establishment_means_map,
                                    location_map,
                                    location,
                                    taxon_source,
                                    taxon_rank_lookup,
                                    taxon_coded,
                                    taxon_status_lookup,
                                    scientific_taxon,
                                    accepted_taxon,
                                    synonym_taxon,
                                    misapplied_taxon,
                                    unknown_taxon,
                                    excluded_taxon,
                                    unplaced_taxon,
                                    unknown_taxon,
                                    reference_taxon,
                                    name_source,
                                    placed_name,
                                    vernacular_name,
                                    unused_name,
                                    accepted_name,
                                    synonym_name,
                                    misapplied_name,
                                    unplaced_name,
                                    excluded_name,
                                    accepted_dwc,
                                    synonym_dwc,
                                    misapplied_dwc,
                                    excluded_dwc,
                                    taxon_dwc,
                                    taxon_dwc_output,
                                    vernacular_source,
                                    vernacular_dwc,
                                    vernacular_status,
                                    vernacular_dwc_filtered,
                                    vernacular_dwc_output,
                                    dwc_identifier,
                                    dwc_identifier_output,
                                    distribution,
                                    dwc_distribution_base,
                                    dwc_distribution_location_id,
                                    dwc_distribution_location,
                                    dwc_distribution,
                                    dwc_default_distribution,
                                    dwc_merged_distribution,
                                    dwc_projected_distribution,
                                    dwc_distribution_output,
                                    dwc_meta,
                                    metadata,
                                    publisher,
                                    dwc_eml,
                                    unused_name_output,
                                    unplaced_taxon_output
                                ])
    return orchestrator


def additional_reader() -> Orchestrator:
    name_file = "names.csv"
    vernacular_name_file = "common-names.csv"
    rank_map_file = "ranks.csv"
    name_schema = NameSchema()
    common_name_schema = CommonNameSchema()
    rank_map_schema = RankMapSchema()
    rank_source = CsvSource.create("rank_source", rank_map_file, "ala", rank_map_schema)
    taxon_file = "taxon.csv"
    taxon_schema = TaxonSchema()
    vernacular_status_file = "Vernacular_Status.csv"
    vernacular_status_schema = VernacularStatusSchema()

    taxon_source = CsvSource.create("taxon_source", taxon_file, "excel", taxon_schema, no_errors=False)
    name_source = CsvSource.create("name_source", name_file, "excel", name_schema, no_errors=False)
    scientific_name = FilterTransform.create('unused_name', name_source.output, lambda r: not is_vernacular_name(r), record_rejects=True)
    # Remove anything that already has a name in the placed names
    unused_name = LookupTransform.create('unused_original_name', scientific_name.output, taxon_source.output, 'scientificNameID', 'scientificNameID', lookup_type=IndexType.FIRST, reject=True, merge=False, record_unmatched=True)

    vernacular_source = CsvSource.create('vernacular_source', vernacular_name_file, "excel", common_name_schema,
                                         no_errors=False)

    # Unplaced names get turned into additional taxa
    name_rank_lookup = LookupTransform.create("name_rank_lookup", unused_name.unmatched, rank_source.output, 'taxonRank',
                                              'term', reject=True, record_unmatched=True,
                                              lookup_map={'taxonRank': 'mappedTaxonRank',
                                                          'taxonRankLevel': 'taxonRankLevel'})
    dwc_base = NslAdditionalToDwcTransform.create("dwc_base", name_rank_lookup.output, 'inferredAccepted')
    dwc_taxon = DwcSyntheticNames.create("synthetic_names", dwc_base.output)
    dwc_output = CsvSink.create("dwc_output", dwc_taxon.output, "taxon.csv", "excel", reduce=True)
    vernacular_dwc = VernacularToDwcTransform.create('vernacular_dwc', vernacular_source.output,
                                                                dwc_taxon.output, 'scientificNameID',
                                                                'scientific_name_id')
    vernacular_status = CsvSource.create("vernacular_status", vernacular_status_file, "ala", vernacular_status_schema)
    vernacular_dwc_filtered = DwcVernacularStatus.create("vernacular_dwc_filtered", vernacular_dwc.output, vernacular_status.output)
    vernacular_dwc_output = CsvSink.create("vernacular_dwc_output",
                                                      vernacular_dwc_filtered.output, "vernacularName.csv", "excel", reduce=True)

    dwc_meta = MetaFile.create('additional_dwc_meta', dwc_output,
                                          vernacular_dwc_output)
    publisher = PublisherSource.create('publisher')
    metadata = CollectorySource.create('metadata')
    dwc_eml = EmlFile.create('dwc_eml', metadata.output, publisher.output)


    orchestrator = Orchestrator("additional_nsl", [
        name_source,
        taxon_source,
        scientific_name,
        unused_name,
        rank_source,
        vernacular_source,
        publisher,
        name_rank_lookup,
        dwc_base,
        dwc_taxon,
        vernacular_dwc,
        vernacular_status,
        vernacular_dwc_filtered,
        dwc_output,
        vernacular_dwc_output,
        dwc_meta,
        metadata,
        publisher,
        dwc_eml
    ])
    return orchestrator
