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


class AusFungiTaxonSchema(Schema):
    taxonID = fields.String(missing=None)
    scientificNameID = fields.String(missing=None)
    acceptedNameUsageID = fields.String(missing=None)
    parentNameUsageID = fields.String(missing=None)
    originalNameUsageID = fields.String(missing=None)
    namePublishedInID = fields.String(missing=None)
    taxonConceptID = fields.String(missing=None)
    scientificName = fields.String(missing=None)
    acceptedNameUsage = fields.String(missing=None)
    parentNameUsage = fields.String(missing=None)
    originalNameUsage = fields.String(missing=None)
    namePublishedIn = fields.String(missing=None)
    namePublishedInYear = fields.String(missing=None)
    nameAccordingTo = fields.String(missing=None)
    higherClassification = fields.String(missing=None)
    kingdom = fields.String(missing=None)
    phylum = fields.String(missing=None)
    subphylum = fields.String(missing=None)
    class_ = fields.String(missing=None, data_key='class')
    subclass = fields.String(missing=None)
    order = fields.String(missing=None)
    family = fields.String(missing=None)
    genus = fields.String(missing=None)
    specificEpithet = fields.String(missing=None)
    infraspecificEpithet = fields.String(missing=None)
    taxonRank = fields.String(missing=None)
    scientificNameAuthorship = fields.String(missing=None)
    nomenclaturalCode = fields.String(missing=None)
    taxonomicStatus = fields.String(missing=None)
    nomenclaturalStatus = fields.String(missing=None)
    occurrenceStatus = fields.String(missing=None)

    class Meta:
        ordered = True
        uri = '"http://rs.tdwg.org/dwc/terms/Taxon'
        namespace = 'http://rs.tdwg.org/dwc/terms/'

class AusFungiIdentifierSchema(Schema):
    coreid = fields.String()
    identifier = fields.String()
    title = fields.String(missing=None)
    format = fields.String(missing=None)

    class Meta:
        ordered = True
        uri = 'http://rs.gbif.org/terms/1.0/Identifier'
        namespace = 'http://purl.org/dc/terms/'
