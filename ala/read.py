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

from ala.transform import SpeciesListSource, CollectorySource, PublisherSource, VernacularListSource
from dwc.meta import MetaFile, EmlFile
from dwc.schema import NomenclaturalCodeMapSchema, VernacularNameSchema, TaxonSchema, VernacularSchema, NameMapSchema
from dwc.transform import DwcTaxonValidate, DwcSyntheticNames, DwcRename
from processing.orchestrate import Orchestrator
from processing.sink import CsvSink
from processing.source import CsvSource
from processing.transform import LookupTransform, MapTransform, FilterTransform, ProjectTransform, DenormaliseTransform, \
    MergeTransform, DeduplicateTransform


def reader() -> Orchestrator:
    species_list = SpeciesListSource.create('species_list')
    species_metadata = CollectorySource.create('collectory_source')
    taxon_list = ProjectTransform.create("taxon_list", species_list.output, TaxonSchema())
    name_map = CsvSource.create('name_map', 'Name_Map.csv', 'ala', NameMapSchema())
    dwc_renamed = DwcRename.create('rename', taxon_list.output, name_map.output)
    nomenclatural_code_map = CsvSource.create('nomenclatural_code_map', 'Nomenclatural_Code_Map.csv', 'ala', NomenclaturalCodeMapSchema())
    default_codes = LookupTransform.create('default_nomenclatural_codes', dwc_renamed.output, nomenclatural_code_map.output, 'kingdom', 'kingdom', overwrite=True)
    dwc_base = DwcTaxonValidate.create("species_validate", default_codes.output, check_names=True, no_errors=True)
    dwc_taxon = DwcSyntheticNames.create("synthetic_names", dwc_base.output)
    dwc_taxon_output = CsvSink.create("dwc_taxon", dwc_taxon.output, "taxon.csv", "excel", reduce=True)
    vernacular_list = FilterTransform.create("vernacular_list", species_list.output, lambda r: r.vernacularName is not None and r.vernacularName != '-')
    dwc_vernacular = MapTransform.create("dwc_vernacular", vernacular_list.output, VernacularSchema(), {
        'vernacularName': MapTransform.capwords('vernacularName'),
        'datasetID': MapTransform.orDefault(MapTransform.choose('datasetID'), 'datasetID'),
        'status': MapTransform.orDefault(MapTransform.choose('status'), 'defaultVernacularStatus')
    }, auto = True)
    dwc_vernacular_denormalised = DenormaliseTransform.delimiter("dwc_vernacular_denormalised", dwc_vernacular.output, 'vernacularName', ',')
    dwc_vernacular_identified = MapTransform.create("dwc_vernacular_identifier", dwc_vernacular_denormalised.output, VernacularSchema(), {
        'nameID': MapTransform.uuid()
    }, auto = True)
    dwc_vernacular_output = CsvSink.create("dwc_vernacular_output", dwc_vernacular_identified.output, "vernacularName.csv", "excel", reduce=True)
    dwc_meta = MetaFile.create('dwc_meta', dwc_taxon_output, dwc_vernacular_output)
    publisher = PublisherSource.create("publisher")
    dwc_eml = EmlFile.create('dwc_eml', species_metadata.output, publisher.output)

    orchestrator = Orchestrator("ala",
                                [
                                    species_list,
                                    species_metadata,
                                    taxon_list,
                                    name_map,
                                    dwc_renamed,
                                    nomenclatural_code_map,
                                    default_codes,
                                    dwc_base,
                                    dwc_taxon,
                                    dwc_taxon_output,
                                    vernacular_list,
                                    dwc_vernacular,
                                    dwc_vernacular_denormalised,
                                    dwc_vernacular_identified,
                                    dwc_vernacular_output,
                                    dwc_meta,
                                    publisher,
                                    dwc_eml
                                ])
    return orchestrator


def vernacular_reader() -> Orchestrator:
    name_file = "vernacularName.csv"
    name_source = CsvSource.create("name_source", name_file, 'excel', VernacularNameSchema())
    name_transform = MapTransform.create("name_transform", name_source.output, VernacularNameSchema(), {
       'vernacularName': MapTransform.capwords('vernacularName'),
       'datasetID': MapTransform.orDefault(MapTransform.choose('datasetID'), 'datasetID'),
       'status': MapTransform.orDefault(MapTransform.choose('status'), 'defaultVernacularStatus')
    }, auto=True)
    name_output = CsvSink.create("name_output", name_transform.output, "vernacularName.csv", "excel", reduce=True)
    dwc_meta = MetaFile.create("dwc_meta", name_output)
    publisher = PublisherSource.create("publisher")
    dwc_eml = EmlFile.create('dwc_eml', publisher.output, publisher.output)
    orchestrator = Orchestrator("ala_vernacular",
                                [
                                    name_source,
                                    name_transform,
                                    name_output,
                                    dwc_meta,
                                    publisher,
                                    dwc_eml
                                ])
    return orchestrator

def vernacular_list_reader() -> Orchestrator:
    with Orchestrator("ala_vernacular_list") as orchestrator:

        vernacular_list = VernacularListSource.create('vernacular_list', aliases={
            'vernacular name': 'vernacularName',
            'Birdlife Common Name': 'vernacularName',
            'Population': 'establishmentMeans',
            'Notes': 'taxonRemarks'
        })
        species_metadata = CollectorySource.create('collectory_source')
        name_transform = MapTransform.create("name_transform", vernacular_list.output, VernacularNameSchema(), {
           'vernacularName': MapTransform.capwords('vernacularName'),
           'datasetID': MapTransform.orDefault(MapTransform.choose('datasetID'), 'datasetID'),
           'status': MapTransform.orDefault(MapTransform.choose('status'), 'defaultVernacularStatus')
        }, auto=True)
        names_unique = DeduplicateTransform.create('names_unique', name_transform.output, ('scientificName', 'vernacularName', 'language'))
        name_output = CsvSink.create("name_output", names_unique.output, "vernacularName.csv", "excel", reduce=True)
        MetaFile.create("dwc_meta", name_output)
        publisher = PublisherSource.create("publisher")
        EmlFile.create('dwc_eml', species_metadata.output, publisher.output)
    return orchestrator

