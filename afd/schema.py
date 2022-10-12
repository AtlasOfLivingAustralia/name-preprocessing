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

from processing import fields


class afd_dialect(csv.Dialect):
    """Describe the usual properties of AFD dsv files."""
    delimiter = '|'
    quotechar = None
    doublequote = False
    skipinitialspace = True
    lineterminator = '\n'
    quoting = csv.QUOTE_NONE
    strict = True
csv.register_dialect("afd", afd_dialect)

class TaxonSchema(Schema):
    TAXON_ID = fields.Integer()
    PARENT_ID = fields.Integer(missing = None)
    PARENT_GUID = fields.UUID(missing = None)
    PARENT_GUID_ID = fields.URL(missing = None)
    TAXON_GUID = fields.UUID()
    TAXON_GUID_ID = fields.URL()
    CITE_AS = fields.String()
    VALID_NAME = fields.String()
    PRIMARY_RANK = fields.String()
    SECONDARY_RANK = fields.String(missing = None)
    UNPLACED = fields.Boolean()
    UNPUBLISHED = fields.Boolean()
    RESTRICTED = fields.Boolean()
    RANK_PREFIX = fields.String()
    ORDER_INDEX = fields.Integer()
    LEGACY_VOLUME_ID = fields.String(missing = None)
    LEGACY_TAXON_ID = fields.String(missing = None)
    DRAFT = fields.Boolean()
    START_DATE = fields.Date(format = "%d-%b-%y")
    END_DATE = fields.Date(missing = None, format = "%d-%b-%y")
    CREATED_FROM_ID = fields.Integer(missing = None)
    STATUS = fields.String()
    ASSIGNED_USER = fields.String()
    DRAFTNAMEONLY = fields.Boolean()
    ASSIGNED_TAXON_ID = fields.String()
    IBRA_REGION_CODES = fields.String(missing = None)
    IBRA_REGIONS = fields.String(missing = None)
    IMCRA_REGIONS = fields.String(missing = None)
    STATE_CODES = fields.String(missing = None)
    STATE = fields.String(missing = None)
    RANK = fields.String(missing = None)

    class Meta:
        ordered = True

class NameSchema(Schema):
    NAME_ID = fields.Integer()
    TAXON_ID = fields.Integer(missing = None)
    NAME_GUID = fields.UUID()
    NAME_GUID_ID = fields.URL()
    CITE_AS = fields.String()
    OBJECT_ID = fields.String(missing = None)
    TYPE = fields.String()
    SUBTYPE = fields.String(missing = None)
    FAMILY = fields.String(missing = None)
    GENUS = fields.String(missing = None)
    SUB_GENUS = fields.String(missing = None)
    SPECIES = fields.String(missing = None)
    SUB_SPECIES = fields.String(missing = None)
    EPHITHET_NAME = fields.String(missing = None)
    YEAR = fields.String(missing = None)
    AUTHOR = fields.String(missing = None)
    CHANGED_COMB = fields.Boolean()
    SUFFIX = fields.String(missing = None)
    QUALIFICATION = fields.String(missing = None)
    PRIMARY_NAME = fields.Boolean()
    UNPUBLISHED = fields.Boolean()
    SCIENTIFIC_NAME = fields.String()
    CAAB_CODE = fields.String(missing = None)
    CAVS_CODE = fields.String(missing = None)
    ORDER_INDEX = fields.Integer(missing = None)
    LEGACY_VOLUME_ID = fields.String(missing = None)
    VISIBLE = fields.Boolean()
    AFF = fields.Boolean()
    CF = fields.Boolean()
    ZOOBANK_NUMBER = fields.String(missing = None)
    START_DATE = fields.Date(format = "%d-%b-%y")
    END_DATE = fields.Date(missing = None, format = "%d-%b-%y")
    CREATED_FROM_ID = fields.String(missing = None)
    EXT_VALUE_1 = fields.String(missing = None)
    NOM_SYNONYM_OF_ID = fields.Integer(missing = None)
    COMB_AUTHOR = fields.String(missing = None)
    LANGUAGE_CODE = fields.String(missing = None)

    class Meta:
        ordered = True

class TaxonomicStatusMapSchema(Schema):
    Type = fields.String()
    Subtype = fields.String(missing = None)
    taxonomicStatus = fields.String()
    nomenclaturalStatus = fields.String(missing=None)
    Accepted = fields.Boolean()
    Synonym = fields.Boolean()
    Misapplied = fields.Boolean()
    Vernacular = fields.Boolean()

    class Meta:
        ordered = True

class RankMapSchema(Schema):
    Key = fields.String()
    Rank = fields.String()
    Level = fields.Integer()

    class Meta:
        ordered = True

class PublicationSchema(Schema):
    PUBLICATION_ID = fields.Integer()
    PUBLICATION_GUID = fields.UUID(missing=None)
    PUBLICATION_GUID_ID = fields.URL(missing=None)
    CITE_AS = fields.String(missing=None)
    PARENT_PUBLICATION_ID = fields.Integer()
    TYPE = fields.String()
    TITLE = fields.String()
    AUTHOR = fields.String(missing=None)
    YEAR = fields.String(missing=None)
    PUBLICATION_DATE = fields.String(missing=None)
    SERIES = fields.String(missing=None)
    VOLUME = fields.String(missing=None)
    PART = fields.String(missing=None)
    EDITION = fields.String(missing=None)
    PUBLISHER = fields.String(missing=None)
    PLACE = fields.String(missing=None)
    ABBREV = fields.String(missing=None)
    PAGES = fields.String(missing=None)
    SOURCE = fields.String(missing=None)
    URL = fields.String(missing=None)
    LEGACY_ID = fields.String(missing=None)
    QUALIFICATION = fields.String(missing=None)
    STRIPPED_TITLE=fields.String(missing=None)
    REPLACEMENT_PUBLICATION_ID = fields.Integer(missing=None)
    ORIGINAL_PARENT_PUBLICATION_ID = fields.Integer(missing=None)
    ZOOBANK_GUID = fields.String(missing=None)
    DOI = fields.String(missing=None)

    class Meta:
        ordered = True

class ReferenceSchema(Schema):
    REFERENCE_ID = fields.Integer()
    OBJECT_ID = fields.Integer()
    PUBLICATION_ID = fields.Integer()
    TYPE = fields.String()
    SUBTYPE = fields.String(missing=None)
    PAGES = fields.String(missing=None)
    QUALIFICATION = fields.String(missing=None)
    REFERENCE_GUID = fields.UUID(missing=None)
    ORIGINAL_PUBLICATION_ID = fields.String(missing=None)

    class Meta:
        ordered = True

class FormattedPublicationSchema(Schema):
    PUBLICATION_ID = fields.Integer()
    namePublishedInID = fields.String(missing=None)
    namePublishedIn = fields.String()
    namePublishedInYear = fields.String(missing=None)
    doi = fields.String(missing=None)
    source = fields.URL()

    class Meta:
        ordered = True

class FormattedReferenceSchema(Schema):
    REFERENCE_ID = fields.Integer()
    OBJECT_ID = fields.Integer()
    PUBLICATION_ID = fields.Integer()
    namePublishedInID = fields.String(missing=None)
    namePublishedIn = fields.String()
    namePublishedInYear = fields.String(missing=None)
    doi = fields.String(missing=None)
    source = fields.String()

    class Meta:
        ordered = True
