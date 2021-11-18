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


class CollectorySchema(Schema):
    """Metadata in the form provided by the ALA collectory"""
    uid = fields.String()
    name = fields.String()
    acronym = fields.String(missing=None)
    pubShortDescription = fields.String(missing=None)
    pubDescription = fields.String(missing=None)
    techDescription = fields.String(missing=None)
    websiteUrl = fields.String(missing=None)
    alaPublicUrl = fields.String(missing=None)
    organisation = fields.String(missing=None)
    street = fields.String(missing=None)
    city = fields.String(missing=None)
    state = fields.String(missing=None)
    postcode = fields.String(missing=None)
    country = fields.String(missing=None)
    postBox = fields.String(missing=None)
    phone = fields.String(missing=None)
    email = fields.String(missing=None)
    rights = fields.String(missing=None)
    license = fields.String(missing=None)
    citation = fields.String(missing=None)
    lastUpdated = fields.DateTime(missing=None)
    doi = fields.String(missing=None)

    class Meta:
        ordered = True
