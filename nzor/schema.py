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

import processing.fields as fields


class NzorTaxonSchema(Schema):
    id = fields.String()
    taxonID = fields.String()
    scientificNameID = fields.String(missing=None)
    acceptedNameUsageID = fields.String(missing=None)
    parentNameUsageID = fields.String(missing=None)
    originalNameUsageID = fields.String(missing=None)
    nameAccordingToID = fields.String(missing=None)
    namePublishedInID = fields.String(missing=None)
    taxonConceptID = fields.String(missing=None)
    scientificName = fields.String(missing=None)
    acceptedNameUsage = fields.String(missing=None)
    parentNameUsage = fields.String(missing=None)
    originalNameUsage = fields.String(missing=None)
    nameAccordingTo = fields.String(missing=None)
    namePublishedIn = fields.String(missing=None)
    namePublishedInYear = fields.String(missing=None)
    higherClassification = fields.String(missing=None)
    kingdom = fields.String(missing=None)
    phylum = fields.String(missing=None)
    class_ = fields.String(missing=None, data_key="class")
    order = fields.String(missing=None)
    family = fields.String(missing=None)
    genus = fields.String(missing=None)
    subgenus = fields.String(missing=None)
    specificEpithet = fields.String(missing=None)
    infraspecificEpithet = fields.String(missing=None)
    taxonRank = fields.String(missing=None)
    verbatimTaxonRank = fields.String(missing=None)
    scientificNameAuthorship = fields.String(missing=None)
    vernacularName = fields.String(missing=None)
    nomenclaturalCode = fields.String(missing=None)
    taxonomicStatus = fields.String(missing=None)
    nomenclaturalStatus = fields.String(missing=None)
    taxonRemarks = fields.String(missing=None)
    modified = fields.String(missing=None)
    license = fields.String(missing=None)
    rightsHolder = fields.String(missing=None)
    accessRights = fields.String(missing=None)
    bibliographicCitation = fields.String(missing=None)
    informationWithheld = fields.String(missing=None)
    datasetID = fields.String(missing=None)
    datasetName = fields.String(missing=None)
    references = fields.String(missing=None)
    taxonomicFlags = fields.String(missing=None)

    class Meta:
        ordered = True

class NzorVernacularSchema(Schema):
    id  = fields.String()
    vernacularName  = fields.String()
    source  = fields.String(missing=None)
    language = fields.String(missing=None)
    temporal = fields.String(missing=None)
    locationID = fields.String(missing=None)
    locality = fields.String(missing=None)
    countryCode = fields.String(missing=None)
    sex = fields.String(missing=None)
    lifeStage = fields.String(missing=None)
    isPlural = fields.String(missing=None)
    isPreferredName = fields.String(missing=None)
    organismPart = fields.String(missing=None)
    taxonRemarks = fields.String(missing=None)

    class Meta:
        ordered = True


class NzorDistributionSchema(Schema):
    id  = fields.String()
    locationID  = fields.String()
    locality  = fields.String(missing=None)
    countryCode = fields.String(missing=None)
    lifeStage = fields.String(missing=None)
    occurrenceStatus = fields.String(missing=None)
    threatStatus = fields.String(missing=None)
    establishmentMeans = fields.String(missing=None)
    appendixCITES = fields.String(missing=None)
    eventDate = fields.String(missing=None)
    startDayOfYear = fields.String(missing=None)
    endDayOfYear = fields.String(missing=None)
    source = fields.String(missing=None)
    occurrenceRemarks = fields.String(missing=None)

    class Meta:
        ordered = True


class NzorRankMapSchema(Schema):
    rank = fields.String()
    nomenclaturalCode = fields.String(missing=None)
    taxonRank = fields.String()

    class Meta:
        ordered = True

class NzorLanguageMapSchema(Schema):
    Name = fields.String()
    Code = fields.String()

    class Meta:
        ordered = True
