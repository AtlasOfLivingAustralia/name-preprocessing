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
    created = fields.DateTime(missing=None)
    modified = fields.DateTime(missing=None)
    datasetName = fields.String(missing=None)
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
    license = fields.String(missing=None)
    ccAttributionIRI = fields.URL(missing=None)

class NameSchema(Schema):
    scientificName = fields.String()
    scientificNameHTML = fields.String()
    canonicalName = fields.String()
    canonicalNameHTML = fields.String()
    nameElement = fields.String()
    scientificNameID = fields.URL()
    nameType = fields.String()
    taxonomicStatus = fields.String(missing=None)
    nomenclaturalStatus = fields.String(missing=None)
    scientificNameAuthorship = fields.String(missing=None)
    cultivarEpithet = fields.String(missing=None)
    autonym = fields.Boolean(missing=False)
    hybrid = fields.Boolean(missing=False)
    cultivar = fields.Boolean(missing=False)
    formula = fields.Boolean(missing=False)
    scientific = fields.Boolean(missing=False)
    nomInval = fields.Boolean(missing=False)
    nomIlleg = fields.Boolean(missing=False)
    namePublishedIn = fields.String(missing=None)
    namePublishedInYear = fields.String(missing=None)
    nameInstanceType = fields.String(missing=None)
    originalNameUsage = fields.String(missing=None)
    originalNameUsageID = fields.URL(missing=None)
    typeCitation = fields.String(missing=None)
    kingdom = fields.String(missing=None)
    family = fields.String(missing=None)
    genericName = fields.String(missing=None)
    specificEpithet = fields.String(missing=None)
    infraspecificEpithet = fields.String(missing=None)
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