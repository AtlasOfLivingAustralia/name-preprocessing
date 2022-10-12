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


class CaabSchema(Schema):
    SPCODE = fields.Integer()
    COMMON_NAME = fields.String(missing=None)
    SCIENTIFIC_NAME = fields.String()
    AUTHORITY = fields.String(missing=None)
    FAMILY = fields.String(missing=None)
    FAMILY_SEQUENCE = fields.Integer(missing=None)
    ASSIGNED_FAMILY_CODE = fields.Integer(missing=None)
    ASSIGNED_FAMILY_SEQUENCE = fields.Integer(missing=None)
    RECENT_SYNONYMS = fields.String(missing=None)
    COMMON_NAMES_LIST = fields.String(missing=None)
    GENUS = fields.String(missing=None)
    SPECIES = fields.String(missing=None)
    SCINAME_INFORMAL = fields.String(missing=None)
    DATE_LAST_MODIFIED = fields.DateTime(missing=None)
    SUBSPECIES = fields.String(missing=None)
    VARIETY = fields.String(missing=None)
    UNDESCRIBED_SP_FLAG = fields.Boolean(missing=False)
    HABITAT_CODE = fields.String(missing=None)
    OBIS_CLASSIFICATION_CODE = fields.String(missing=None)
    SUBGENUS = fields.String(missing=None)
    KINGDOM = fields.String(missing=None)
    PHYLUM = fields.String(missing=None)
    SUBPHYLUM = fields.String(missing=None)
    CLASS = fields.String(missing=None)
    SUBCLASS = fields.String(missing=None)
    ORDER_NAME = fields.String(missing=None)
    SUBORDER = fields.String(missing=None)
    INFRAORDER = fields.String(missing=None)
    DATE_EXTRACTED = fields.Float(missing=None)
    LIST_STATUS_CODE = fields.String(missing=None)
    ITIS_IDENTIFIER = fields.String(missing=None)
    PARENT_ID = fields.Integer(missing=None)
    DISPLAY_NAME = fields.String(missing=None)
    RANK = fields.String(missing=None)
    NON_CURRENT_FLAG = fields.Boolean(missing=False)
    SUPERSEDED_BY = fields.Integer(missing=None)

    class Meta:
        ordered = True