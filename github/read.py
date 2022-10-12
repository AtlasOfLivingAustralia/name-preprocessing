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
from github.transform import GithubListSource
from processing.orchestrate import Orchestrator
from processing.sink import CsvSink
from processing.source import CsvSource
from processing.transform import LookupTransform, MapTransform, FilterTransform, ProjectTransform, DenormaliseTransform, \
    MergeTransform


def reader() -> Orchestrator:
    species_list = GithubListSource.create('species_list')
    species_metadata = CollectorySource.create('collectory_source')
    species_defaults = MapTransform.create('species_defaults', species_list.output, species_list.output.schema, {
        'datasetID': MapTransform.orDefault(MapTransform.choose('datasetID'), 'datasetID'),
        'taxonomicStatus': MapTransform.orDefault(MapTransform.choose('taxonomicStatus'), 'defaultAcceptedStatus'),
        'source': MapTransform.choose('scientificNameLink')
    }, auto=True)
    taxon_list = ProjectTransform.create("taxon_list", species_defaults.output, TaxonSchema())
    name_map = CsvSource.create('name_map', 'Name_Map.csv', 'ala', NameMapSchema())
    dwc_renamed = DwcRename.create('rename', taxon_list.output, name_map.output)
    nomenclatural_code_map = CsvSource.create('nomenclatural_code_map', 'Nomenclatural_Code_Map.csv', 'ala', NomenclaturalCodeMapSchema())
    default_codes = LookupTransform.create('default_nomenclatural_codes', dwc_renamed.output, nomenclatural_code_map.output, 'kingdom', 'kingdom', overwrite=True)
    dwc_base = DwcTaxonValidate.create("species_validate", default_codes.output, check_names=True, no_errors=True)
    dwc_taxon = DwcSyntheticNames.create("synthetic_names", dwc_base.output, fail_on_exception=True)
    dwc_taxon_output = CsvSink.create("dwc_taxon", dwc_taxon.output, "taxon.csv", "excel", reduce=True)
    vernacular_list = FilterTransform.create("vernacular_list", species_defaults.output, lambda r: r.vernacularName is not None and r.vernacularName != '-')
    dwc_vernacular = MapTransform.create("dwc_vernacular", vernacular_list.output, VernacularSchema(), {
        'vernacularName': MapTransform.capwords('vernacularName'),
        'datasetID': MapTransform.orDefault(MapTransform.choose('datasetID'), 'datasetID'),
        'status': MapTransform.orDefault(MapTransform.choose('status'), 'defaultVernacularStatus')
    }, auto = True)
    dwc_vernacular_denormalised = DenormaliseTransform.create("dwc_vernacular_denormalised", dwc_vernacular.output, 'vernacularName', ',')
    dwc_vernacular_identified = MapTransform.create("dwc_vernacular_identifier", dwc_vernacular_denormalised.output, VernacularSchema(), {
        'nameID': MapTransform.uuid()
    }, auto = True)
    dwc_vernacular_output = CsvSink.create("dwc_vernacular_output", dwc_vernacular_identified.output, "vernacularName.csv", "excel", reduce=True)
    dwc_meta = MetaFile.create('dwc_meta', dwc_taxon_output, dwc_vernacular_output)
    publisher = PublisherSource.create("publisher")
    dwc_eml = EmlFile.create('dwc_eml', species_metadata.output, publisher.output)

    orchestrator = Orchestrator("github",
                                [
                                    species_list,
                                    species_metadata,
                                    species_defaults,
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
