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
