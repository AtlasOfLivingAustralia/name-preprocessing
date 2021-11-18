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

from afd.parent import ParentTransform
from afd.references import PublicationTransform, ReferenceTransform
from afd.schema import TaxonSchema, NameSchema, TaxonomicStatusMapSchema, ReferenceSchema, PublicationSchema, \
    RankMapSchema
from afd.todwc import AcceptedToDwcTaxonTransform, SynonymToDwcTaxonTransform, VernacularToDwcTransform
from ala.transform import PublisherSource, CollectorySource
from dwc.meta import MetaFile, EmlFile
from processing.dataset import Record, IndexType
from processing.orchestrate import Orchestrator
from processing.sink import CsvSink, NullSink
from processing.source import CsvSource
from processing.transform import FilterTransform, LookupTransform, MergeTransform


def is_current_taxon(record: Record):
    return not record.UNPLACED and record.STATUS == 'taxon.status.P' and record.data['END_DATE'] is None

def is_current_name(record: Record):
    return record.END_DATE is None

def is_valid_name(record: Record):
    status = record.Accepted
    return status is not None and status

def is_synonym_name(record: Record):
    status = record.Synonym
    return status is not None and status

def is_misapplied_name(record: Record):
    status = record.Misapplied
    return status is not None and status

def is_vernacular_name(record: Record):
    status = record.Vernacular
    return status is not None and status

def is_unused_name(record: Record):
    return not is_valid_name(record) and not is_synonym_name(record) and not is_misapplied_name(record) and not is_vernacular_name(record)

def reader():
    taxon_file = "taxon.dsv"
    name_file = "name.dsv"
    taxonomic_status_map_file = "Taxonomic_Status_Map.csv"
    rank_map_file = "Rank_Map.csv"
    reference_file = "reference.dsv"
    publication_file = "publication.dsv"
    taxon_schema = TaxonSchema()
    name_schema = NameSchema()
    taxonomic_status_schema = TaxonomicStatusMapSchema()
    rank_map_schema = RankMapSchema()
    reference_schema = ReferenceSchema()
    publication_schema = PublicationSchema()


    taxon_source = CsvSource.create("taxon_source", taxon_file, "afd", taxon_schema, no_errors=False)
    taxon_filter = FilterTransform.create("current_taxon", taxon_source.output, is_current_taxon, record_rejects=True)
    taxon_parent = ParentTransform.create("parent_taxon", taxon_filter.output, taxon_source.output, 'TAXON_ID', 'PARENT_ID', 'taxon.rank.H.K', 'PRIMARY_RANK', 'ANIMALIA', 'VALID_NAME', fail_on_exception=True)
    rank_source = CsvSource.create("rank_source", rank_map_file, "ala", rank_map_schema)
    taxon_rank_lookup = LookupTransform.create("taxon_rank_lookup", taxon_parent.output, rank_source.output, 'PRIMARY_RANK', 'Key', reject=True, record_unmatched=True)

    name_source = CsvSource.create("name_source", name_file, "afd", name_schema, no_errors=False)
    name_filter = FilterTransform.create("current_name", name_source.output, is_current_name)

    status_source = CsvSource.create("status_source", taxonomic_status_map_file, "ala", taxonomic_status_schema)
    name_lookup = LookupTransform.create("name_status_lookup", name_filter.output, status_source.output, ('TYPE', 'SUBTYPE'), ('Type', 'Subtype'), reject=True, record_unmatched=True)

    reference_source = CsvSource.create("reference_source", reference_file, "afd", reference_schema, no_errors=False)
    publication_source = CsvSource.create("publication_source", publication_file, "afd", publication_schema, no_errors=False)
    publication_format = PublicationTransform.create("publication_format", publication_source.output, 'PARENT_PUBLICATION_ID', 'PUBLICATION_ID')
    publication_output = CsvSink.create("publication_sink", publication_format.output, "publication.csv", "excel", True)
    reference_format = ReferenceTransform.create("reference_format", reference_source.output, publication_format.output, 'PUBLICATION_ID', 'PUBLICATION_ID')
    reference_output = CsvSink.create("reference_sink", reference_format.output, "reference.csv", "excel", True)

    referenced_name = LookupTransform.create("referenced_name", name_lookup.output, reference_format.output, 'NAME_ID', 'OBJECT_ID', lookup_type=IndexType.FIRST)

    valid_name = FilterTransform.create('valid_name', referenced_name.output, is_valid_name)
    synonym_name = FilterTransform.create('synonym_name', referenced_name.output, is_synonym_name)
    misapplied_name = FilterTransform.create('misapplied_name', referenced_name.output, is_misapplied_name)
    vernacular_name = FilterTransform.create('vernacular_name', referenced_name.output, is_vernacular_name)
    unused_name = FilterTransform.create('unused_name', referenced_name.output, is_unused_name)

    dwc_accepted = AcceptedToDwcTaxonTransform.create("dwc_accepted", valid_name.output, taxon_rank_lookup.output, 'TAXON_ID', 'PARENT_ID')
    dwc_synonym = SynonymToDwcTaxonTransform.create("dwc_synonym", synonym_name.output, taxon_rank_lookup.output, 'TAXON_ID', 'TAXON_ID')
    dwc_misapplied = SynonymToDwcTaxonTransform.create("dwc_misapplied", misapplied_name.output, taxon_rank_lookup.output, 'TAXON_ID', 'TAXON_ID')
    dwc_taxon = MergeTransform.create("dwc_taxon_merge", dwc_accepted.output, dwc_synonym.output, dwc_misapplied.output, fail_on_exception=True)
    dwc_taxon_output = CsvSink.create("dwc_taxon_output", dwc_taxon.output, "taxon.csv", "excel")

    dwc_vernacular = VernacularToDwcTransform.create('dwc_vernacular', vernacular_name.output, taxon_rank_lookup.output, 'TAXON_ID', 'TAXON_ID', fail_on_exception=True)
    dwc_vernacular_output = CsvSink.create("dwc_vernacular_output", dwc_vernacular.output, "vernacularName.csv", "excel")

    dwc_meta = MetaFile.create('dwc_meta', dwc_taxon_output, dwc_vernacular_output)
    publisher = PublisherSource.create('publisher')
    metadata = CollectorySource.create('metadata')
    dwc_eml = EmlFile.create('dwc_eml', metadata.output, publisher.output)

    orchestrator = Orchestrator("afd",
                                [
                                    status_source,
                                    taxon_source,
                                    taxon_filter,
                                    taxon_parent,
                                    rank_source,
                                    taxon_rank_lookup,
                                    name_source,
                                    name_filter,
                                    name_lookup,
                                    valid_name,
                                    synonym_name,
                                    misapplied_name,
                                    vernacular_name,
                                    unused_name,
                                    reference_source,
                                    publication_source,
                                    publication_format,
                                    publication_output,
                                    reference_format,
                                    reference_output,
                                    referenced_name,
                                    dwc_accepted,
                                    dwc_synonym,
                                    dwc_misapplied,
                                    dwc_taxon,
                                    dwc_taxon_output,
                                    dwc_vernacular,
                                    dwc_vernacular_output,
                                    dwc_meta,
                                    metadata,
                                    publisher,
                                    dwc_eml
                                ])
    return orchestrator