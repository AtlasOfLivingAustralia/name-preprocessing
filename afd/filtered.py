from afd.schema import TaxonSchema, NameSchema, TaxonomicStatusMapSchema, ReferenceSchema, PublicationSchema
from processing.dataset import Record
from processing.node import ProcessingContext
from processing.orchestrate import Orchestrator
from processing.sink import CsvSink
from processing.source import CsvSource
from processing.transform import FilterTransform, LookupTransform, ReindexTransform, MergeTransform

"""
Generate a sampled collection of taxa, names, publication etc.
"""

def is_current_name(record: Record):
    return record.data['END_DATE'] is None

config_dir = "/Users/pal155/ALA/Naming/AFD"
work_dir = "/Users/pal155/ALA/Naming/AFD/2020-09-30/work"
input_dir = "/Users/pal155/ALA/Naming/AFD/2020-09-30/AFD_export_2020_09_30"
output_dir = "/Users/pal155/ALA/Naming/AFD/2020-09-30/output"
taxon_file = "taxon-100.dsv"
name_file = "name.dsv"
reference_file = "reference.dsv"
publication_file = "publication.dsv"
taxon_schema = TaxonSchema()
name_schema = NameSchema()
taxonomic_status_schema = TaxonomicStatusMapSchema()
reference_schema = ReferenceSchema()
publication_schema = PublicationSchema()

taxon_source = CsvSource.create("taxon_source", taxon_file, "afd", taxon_schema, 'TAXON_ID', no_errors=False)

name_source = CsvSource.create("name_source", name_file, "afd", name_schema, 'NAME_ID', no_errors=False)
name_filter = FilterTransform.create("current_name", name_source.output, is_current_name)

name_lookup = LookupTransform.create("name_lookup", name_filter.output, taxon_source.output, 'TAXON_ID', reject=True, merge=False)
name_output = CsvSink.create("name_output", name_lookup.output, 'name-sampled.dsv', 'afd')

reference_source = CsvSource.create("reference_source", reference_file, "afd", reference_schema, 'REFERENCE_ID', no_errors=False, ignore_duplicates=True)
reference_lookup = LookupTransform.create("reference_lookup", reference_source.output, name_lookup.output, 'OBJECT_ID', reject=True, merge=False)
reference_output = CsvSink.create("reference_output", reference_lookup.output, 'reference-sampled.dsv', 'afd')
reference_pid = ReindexTransform.create("reference_pid", reference_lookup.output, 'PUBLICATION_ID', ignore_duplicates=True)

publication_source = CsvSource.create("publication_source", publication_file, "afd", publication_schema, 'PUBLICATION_ID', no_errors=False)
publication_lookup = LookupTransform.create("publication_lookup", publication_source.output, reference_pid.output, 'PUBLICATION_ID', reject=True, merge=False)
publication_parent = LookupTransform.create("publication_parent", publication_source.output, publication_lookup.output, 'PARENT_PUBLICATION_ID', reject=True, merge=False)
publication_all = MergeTransform.create("publication_merged", [publication_lookup.output, publication_parent.output], fail_on_exception=True)
publication_output = CsvSink.create("publication_output", publication_all.output, 'publication-sampled.dsv', 'afd')

context = ProcessingContext("ctx", CsvSink, work_dir=work_dir, config_dir=config_dir, input_dir=input_dir, output_dir=output_dir)
orchestrator = Orchestrator("orch",
                            [
                                taxon_source,
                                name_source,
                                name_filter,
                                name_lookup,
                                name_output,
                                reference_source,
                                reference_lookup,
                                reference_output,
                                reference_pid,
                                publication_source,
                                publication_lookup,
                                publication_parent,
                                publication_all,
                                publication_output
                            ])
orchestrator.run(context)
