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
from attr import attr

from processing.dataset import Port, Keys
from processing.transform import Transform

attr.s
class AncestorTransform(Transform):
    """
    Build a list of ancesotor identifiers for a taxon
    """
    input: Port = attr.ib()
    full: Port = attr.ib()
    output: Port = attr.ib()
    input_keys: Keys = attr.ib()
    ancesotor_keys: Keys = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, full: Port, taxon_keys, ancestor_keys, **kwargs):
        taxon_keys = Keys.make_keys(input.schema, taxon_keys)
        ancestor_keys = Keys.make_keys(full.schema, ancestor_keys)
        os = input.schema.fields.copy()
        os.update({ key.name: key for key in link_keys.keys })
        schema = Port.schema_from_dict(os, ordered=True)()
        output = Port.port(schema)
        return ParentTransform(id, input, full, output, input_keys, link_keys, kingdom_rank, rank_keys, default_kingdom_name, name_keys, **kwargs)

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['input'] = self.input
        inputs['full'] = self.full
        return inputs

    def outputs(self) -> Dict[str, Port]:
        outputs = super().outputs()
        outputs['output'] = self.output
        return outputs

    def execute(self, context: ProcessingContext):
        super().execute(context)
        data = context.acquire(self.input)
        data_index = Index.create(data, self.input_keys)
        kingdoms = [record for record in data.rows if self.rank_keys.get(record) == self.kingdom_rank]
        default_kingdom = [record for record in kingdoms if self.name_keys.get(record) == self.default_kingdom_name]
        default_kingdom = default_kingdom[0] if len(default_kingdom) > 0 else None
        self.logger.info("Default kingdom is %s", default_kingdom)
        table = context.acquire(self.full)
        index = Index.create(table, self.input_keys)
        result = Dataset.for_port(self.output)
        errors = Dataset.for_port(self.error)
        additional = self.build_additional(context)
        for record in data.rows:
            try:
                parent = record
                trail = set()
                while True:
                    kv = self.link_keys.get(parent)
                    if kv in trail:
                        self.logger.warning("Circular trail at %s in %s", kv, trail)
                        errors.add(Record.error(record, None, "Circular history reference at " + str(kv)))
                        self.count(self.ERROR_COUNT, record, context)
                        parent = default_kingdom
                        break
                    trail.add(kv)
                    parent = index.find(parent, self.link_keys)
                    if parent is None:
                        if record not in kingdoms:
                            self.logger.warning("No parent found for %s, defaulting to kingdom", record)
                            self.count(self.ERROR_COUNT, record, context)
                            parent = default_kingdom
                        break
                    valid = data_index.find(parent, self.input_keys)
                    if valid is not None:
                        break
                composed = self.compose(record, parent, context, additional)
                if composed is not None:
                    result.add(composed)
                    self.count(self.ACCEPTED_COUNT, composed, context)
            except Exception as err:
                if self.fail_on_exception:
                    raise err
                errors.add(Record.error(record, err))
                self.count(self.ERROR_COUNT, record, context)
            self.count(self.PROCESSED_COUNT, record, context)
        context.save(self.output, result)
        context.save(self.error, errors)

    def compose(self, record: Record, parent: Record, context: ProcessingContext, additional) -> Record:
        """
        Make an updated version of the record with the accepted parent

        :param record: The original record
        :param parent: The parent record (null for none)
        :param context: The processing context
        :param additional: Any additional context

        :return: A composed record, or null for no record
        """
        linked_data = record.data.copy()
        linked_data.update(self.input_keys.make_key_map(parent, self.link_keys))
        return Record(record.line, linked_data, record.issues)
