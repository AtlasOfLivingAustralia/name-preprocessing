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

from marshmallow import Schema

from processing import fields


class TaxonSchema(Schema):
    taxonID = fields.URL()
    nameType = fields.String(missing=None)
    acceptedNameUsageID = fields.URL(missing=None)
    acceptedNameUsage = fields.String(missing=None)
    nomenclaturalStatus = fields.String(missing=None)
    nomIlleg = fields.Boolean(missing=False)
    nomInval = fields.Boolean(missing=False)
    taxonomicStatus = fields.String(missing=None)
    proParte = fields.Boolean(missing=False)
    scientificName = fields.String()
    scientificNameID = fields.URL(missing=None)
    canonicalName = fields.String()
    scientificNameAuthorship = fields.String(missing=None)
    parentNameUsageID = fields.URL(missing=None)
    taxonRank = fields.String(missing=None)
    taxonRankSortOrder = fields.Integer(missing=None)
    kingdom = fields.String(missing=None)
    clazz = fields.String(missing=None, data_key='class')
    subclass = fields.String(missing=None)
    family = fields.String(missing=None)
    taxonConceptID = fields.URL(missing=None)
    nameAccordingTo = fields.String(missing=None)
    nameAccordingToID = fields.String(missing=None)
    taxonRemarks = fields.String(missing=None)
    taxonDistribution = fields.String(missing=None)
    higherClassification = fields.String(missing=None)
    firstHybridParentName = fields.String(missing=None)
    firstHybridParentNameID = fields.URL(missing=None)
    secondHybridParentName = fields.String(missing=None)
    secondHybridParentNameID = fields.URL(missing=None)
    nomenclaturalCode = fields.String(missing=None)
    created = fields.DateTime(missing=None)
    modified = fields.DateTime(missing=None)
    datasetName = fields.String(missing=None)
    dataSetID = fields.String(missing=None)
    license = fields.String(missing=None)
    ccAttributionIRI = fields.URL(missing=None)

class NameSchema(Schema):
    scientificNameID = fields.URL()
    nameType = fields.String()
    scientificName = fields.String()
    scientificNameHTML = fields.String()
    canonicalName = fields.String()
    canonicalNameHTML = fields.String()
    nameElement = fields.String()
    nomenclaturalStatus = fields.String(missing=None)
    scientificNameAuthorship = fields.String(missing=None)
    autonym = fields.Boolean(missing=False)
    hybrid = fields.Boolean(missing=False)
    cultivar = fields.Boolean(missing=False)
    formula = fields.Boolean(missing=False)
    scientific = fields.Boolean(missing=False)
    nomInval = fields.Boolean(missing=False)
    nomIlleg = fields.Boolean(missing=False)
    namePublishedIn = fields.String(missing=None)
    namePublishedInID = fields.String(missing=None)
    namePublishedInYear = fields.String(missing=None)
    nameInstanceType = fields.String(missing=None)
    nameAccordingToID = fields.String(missing=None)
    nameAccordingTo = fields.String(missing=None)
    originalNameUsageID = fields.String(missing=None)
    originalNameUsage = fields.String(missing=None)
    originalNameUsageYear = fields.String(missing=None)
    typeCitation = fields.String(missing=None)
    kingdom = fields.String(missing=None)
    family = fields.String(missing=None)
    genericName = fields.String(missing=None)
    specificEpithet = fields.String(missing=None)
    infraspecificEpithet = fields.String(missing=None)
    cultivarEpithet = fields.String(missing=None)
    taxonRank = fields.String(missing=None)
    taxonRankSortOrder = fields.String(missing=None)
    taxonRankAbbreviation = fields.String(missing=None)
    firstHybridParentName = fields.String(missing=None)
    firstHybridParentNameID = fields.URL(missing=None)
    secondHybridParentName = fields.String(missing=None)
    secondHybridParentNameID = fields.URL(missing=None)
    created = fields.DateTime(missing=None)
    modified = fields.DateTime(missing=None)
    nomenclaturalCode = fields.String(missing=None)
    datasetName = fields.String(missing=None)
    taxonomicStatus = fields.String(missing=None)
    statusAccordingTo = fields.String(missing=None)
    license = fields.String(missing=None)
    ccAttributionIRI = fields.URL(missing=None)

class CommonNameSchema(Schema):
    common_name_id = fields.URL()
    common_name = fields.String()
    instance_id = fields.URL()
    citation = fields.String(missing=None)
    scientific_name_id = fields.URL()
    scientific_name = fields.String(missing=None)
    datasetName = fields.String(missing=None)
    license = fields.String(missing=None)
    ccAttributionIRI = fields.URL(missing=None)

class RankMapSchema(Schema):
    term = fields.String()
    taxonRank = fields.String(missing=None)
    taxonRankLevel = fields.Integer(missing=None)
    taxonGroup = fields.String(missing=None)

class TaxonomicStatusMapSchema(Schema):
    Term = fields.String()
    DwC = fields.String(missing=None)
    Accepted = fields.Boolean(missing=False)
    Synonym = fields.Boolean(missing=False)
    Misapplied = fields.Boolean(missing=False)
    Unplaced = fields.Boolean(missing=False)
    Excluded = fields.Boolean(missing=False)

class OrthVarSchema(Schema):
    apc_taxonomic_status = fields.String()
    scientific_name_id = fields.URL()
    scientific_name = fields.String()
    nomenclatural_status = fields.String()
    taxon_rank_abbreviation = fields.String()
    related_name = fields.String()
    syn_name_id = fields.String()
    syn_scientific_name = fields.String()
    syn_usage_id = fields.String()
    syn_citation = fields.String()
    apc_relationship = fields.String()
    accepted_name_usage_id = fields.URL()
    accepted_name_usage = fields.String()
class RelationshipSchema(Schema):
    instance_type = fields.String()
    name_id = fields.String()
    scientific_name = fields.String()
    nomenclatural_status = fields.String()
    name_published_in_year = fields.String()
    apc_taxonomic_status = fields.String()
    relationship = fields.String()
    full_name = fields.String()
    syn_instance_id = fields.String()
    syn_name_id = fields.String()
    apc_relationship = fields.String()
    taxon_id = fields.String()
    name_type = fields.String()
    accepted_name_usage_id = fields.String()
    accepted_name_usage = fields.String()
    nomenclatural_status_2 = fields.String()
    nom_illeg = fields.String()
    nom_inval = fields.String()
    taxonomic_status = fields.String()
    pro_parte = fields.String()
    scientific_name_2 = fields.String()
    scientific_name_id = fields.String()
    canonical_name = fields.String()
    scientific_name_authorship = fields.String()
    parent_name_usage_id = fields.String()
    taxon_rank = fields.String()
    taxon_rank_sort_order = fields.String()
    kingdom = fields.String()
    class_ = fields.String()
    subclass = fields.String()
    family = fields.String()
    taxon_concept_id = fields.String()
    name_according_to = fields.String()
    name_according_to_id = fields.String()
    taxon_remarks = fields.String()
    taxon_distribution = fields.String()
    higher_classification = fields.String()
    first_hybrid_parent_name = fields.String()
    first_hybrid_parent_name_id = fields.String()
    second_hybrid_parent_name = fields.String()
    second_hybrid_parent_name_id = fields.String()
    nomenclatural_code = fields.String()
    created = fields.String()
    modified = fields.String()
    dataset_name = fields.String()
    dataset_id = fields.String()
    license = fields.String()
    cc_attribution_iri = fields.String()
    tree_version_id = fields.String()
    tree_element_id = fields.String()
    instance_id = fields.String()
    name_id_2 = fields.String()
    homotypic = fields.String()
    heterotypic = fields.String()
    misapplied = fields.String()
    relationship_2 = fields.String()
    synonym = fields.String()
    excluded_name = fields.String()
    accepted = fields.String()
    accepted_id = fields.String()
    rank_rdf_id = fields.String()
    name_space = fields.String()
    tree_description = fields.String()
    tree_label = fields.String()