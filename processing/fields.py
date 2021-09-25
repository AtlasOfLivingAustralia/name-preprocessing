from marshmallow import fields
"""
Replaces marshmallow fields with fields where an empty string maps onto None
"""

class _NoneMixin(object):
    def _deserialize(self, value, attr, data, **kwargs):
        if value == '':
            return None
        return super(_NoneMixin, self)._deserialize(value, attr, data, **kwargs)

    def _validate(self, value):
        if value is None:
            return
        return super(_NoneMixin, self)._validate(value)

class Boolean(_NoneMixin, fields.Boolean):
    pass

class String(_NoneMixin, fields.String):
    pass

class Integer(_NoneMixin, fields.Integer):
    pass

class Date(_NoneMixin, fields.Date):
    pass

class DateTime(_NoneMixin, fields.DateTime):
    pass

class Float(_NoneMixin, fields.Float):
    pass

class List(_NoneMixin, fields.List):
    pass

class Nested(_NoneMixin, fields.Nested):
    pass

class URL(_NoneMixin, fields.URL):
    pass

class UUID(_NoneMixin, fields.UUID):
    pass
