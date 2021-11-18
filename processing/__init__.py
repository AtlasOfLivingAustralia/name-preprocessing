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

class ala_dialect(csv.Dialect):
    """Describe the usual properties of common ala CSV files."""
    delimiter = ','
    escapechar = '\\'
    quotechar = '"'
    doublequote = False
    skipinitialspace = True
    lineterminator = '\n'
    quoting = csv.QUOTE_MINIMAL
    strict = True
csv.register_dialect("ala", ala_dialect)
