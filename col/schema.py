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

import csv

from marshmallow import Schema

import processing.fields as fields


class col_dialect(csv.Dialect):
    """Describe the usual properties of CoL tsv files."""
    delimiter = '\t'
    quotechar = None
    doublequote = False
    skipinitialspace = True
    lineterminator = '\n'
    quoting = csv.QUOTE_NONE
    strict = True
csv.register_dialect("col", col_dialect)

class ColTaxonSchema(Schema):
    taxonID = fields.String()
    identifier = fields.String(missing=None)
    datasetID = fields.String(missing=None)
    datasetName = fields.String(missing=None)
    acceptedNameUsageID = fields.String(missing=None)
    parentNameUsageID = fields.String(missing=None)
    taxonomicStatus = fields.String(missing=None)
    taxonRank = fields.String(missing=None)
    verbatimTaxonRank = fields.String(missing=None)
    scientificName = fields.String()
    kingdom = fields.String(missing=None)
    phylum = fields.String(missing=None)
    class_ = fields.String(missing=None, data_key="class")
    order = fields.String(missing=None)
    superfamily = fields.String(missing=None)
    family = fields.String(missing=None)
    genericName = fields.String(missing=None)
    genus = fields.String(missing=None)
    subgenus = fields.String(missing=None)
    specificEpithet = fields.String(missing=None)
    infraspecificEpithet = fields.String(missing=None)
    scientificNameAuthorship = fields.String(missing=None)
    source = fields.String(missing=None)
    namePublishedIn = fields.String(missing=None)
    nameAccordingTo = fields.String(missing=None)
    modified = fields.String(missing=None)
    description = fields.String(missing=None)
    taxonConceptID = fields.String(missing=None)
    scientificNameID = fields.String(missing=None)
    references = fields.String(missing=None)
    isExtinct = fields.Boolean(missing=None)

    class Meta:
        ordered = True

class ColDistributionSchema(Schema):
    taxonID = fields.String()
    locationID = fields.String(missing=None)
    locality = fields.String(missing=None)
    occurrenceStatus = fields.String(missing=None)
    establishmentMeans = fields.String(missing=None)

    class Meta:
        ordered = True

class ColVernacularSchema(Schema):
    taxonID = fields.String()
    vernacularName = fields.String(missing=None)
    language = fields.String(missing=None)
    countryCode = fields.String(missing=None)
    locality = fields.String(missing=None)
    transliteration = fields.String(missing=None)

    class Meta:
        ordered = True

class ColAcceptedKingdomSchema(Schema):
    kingdom = fields.String()
    useDistribution = fields.Boolean()
    useDataset = fields.Boolean()

class ColAcceptedDatasetSchema(Schema):
    datasetID = fields.String()
    datasetName = fields.String()

class ColAcceptedLocationSchema(Schema):
    locationID = fields.String()
    locality = fields.String()
    country = fields.String()

class ColAcceptedLanguageSchema(Schema):
    language = fields.String()
    code = fields.String()
