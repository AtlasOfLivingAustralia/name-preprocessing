#  Copyright (c) 2022.  Atlas of Living Australia
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


class InputSchema(Schema):
    """
    Pre-processed locations
    """
    locationID = fields.String()
    parentLocationID = fields.String(missing=None)
    name = fields.String(missing=None)
    preferredName = fields.String(missing=None)
    otherNames = fields.String(missing=None)
    iso2 = fields.String(missing=None)
    iso3 = fields.String(missing=None)
    currency = fields.String(missing=None)
    type = fields.String(missing=None)
    decimalLatitude = fields.Float(missing=None)
    decimalLongitude = fields.Float(missing=None)

    class Meta:
        ordered = True


class GeographyTypeMap(Schema):
    """
    Map TGN geography type onto
    """
    type = fields.String()
    include = fields.String()
    parent = fields.String()
    geographyType = fields.String(missing=None)


class AreaSchema(Schema):
    """
    Area lookup table
    """
    name = fields.String()
    area = fields.Float(missing=None)
    landArea = fields.Float(missing=None)


class DefaultAreaSchema(Schema):
    """
    Default area lookup table
    """
    geographyType = fields.String()
    area = fields.Float()
    locationComments = fields.String(missing=None)


class TypeMapSchema(Schema):
    """
    Geography type lookup table
    """
    locationID = fields.String()
    name = fields.String()
    geographyType = fields.String()


class NameSchema(Schema):
    """
    (Invalid) name lookup table
    """
    name = fields.String()


class LocationWeightSchema(Schema):
    """
    Weight lookup table
    """
    locationID = fields.String()
    weight = fields.Float()
