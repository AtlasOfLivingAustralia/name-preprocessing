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

import os.path
from typing import Dict, List, Set

import attr

from processing.dataset import Port, Keys, Record
from processing.node import Node, ProcessingContext, ProcessingException


@attr.s
class Orchestrator(Node):
    nodes: List[Node] = attr.ib(factory=list)
    completed: List[None] = attr.ib(factory=list)

    def report(self, context: ProcessingContext):
        self.logger.info("Executed")

    def dangling_nodes(self) -> Set[Port]:
        """
        Input ports that have not been linked to nodes that are in the orchestrator

        :return: Any ports
        """
        seen_nodes = set()
        dangling_nodes = []
        seen = set()
        for node in self.completed:
            seen.update(node.outputs().values())
        for node in self.nodes:
            seen.update(node.outputs().values())
            seen.update(node.errors().values())
        for node in self.nodes:
            for portid, port in node.inputs().items():
                if port in seen:
                    continue
                source = self.source_node(port)
                if source is None and node.id not in seen_nodes:
                    self.logger.warning("Node '%s' has dangling input '%s'", node.id, portid)
                    seen_nodes.add(node.id)
                    dangling_nodes.append(node)
                seen.add(port)
        return dangling_nodes

    def source_node(self, port: Port) -> Node:
        """
        Look for a node that is the source of a port, either as an output or an error.

        :param port: The port to search for

        :return: The resulting node or none for not found
        """
        for node in self.nodes:
            if port in node.outputs().values() or port in node.errors().values():
                return node
        for node in self.completed:
            if port in node.outputs().values() or port in node.errors().values():
                return node
        return None

    def add(self, node: Node):
        if node in self.nodes:
            raise ValueError("Node " + node.id + " is already present")
        self.nodes.append(node)

    def create_dangling_nodes(self, dsc, node: Node, ports: Dict[str, Port], context: ProcessingContext, inputs: Set[Port]) -> List[Node]:
        dangling = []
        for key, port in ports.items():
            if port not in inputs and context.has_data(port):
                id = node.id + "_" + key
                sink = dsc.create_in_context(id, port, context, tags={'generated': True}, no_errors=False)
                self.logger.debug("Created sink " + sink.id)
                self.add(sink)
                dangling.append(sink)
        return dangling

    def execute_dangling_ports(self, context: ProcessingContext):
        dsc = context.dangling_sink_class
        if dsc is None:
            self.logger.info("Ignoring dangling ports")
            return
        self.logger.debug("Processing dangling ports")
        inputs = set()
        for node in self.nodes:
            inputs.update(node.inputs().values())
        dangling = []
        for node in self.nodes:
            dangling.extend(self.create_dangling_nodes(dsc, node, node.outputs(), context, inputs))
            dangling.extend(self.create_dangling_nodes(dsc, node, node.errors(), context, inputs))
        for node in dangling:
            node.run(context)

    def dump_graph(self, context: ProcessingContext):
        graph_file = context.locate_output_file(context.id + "_graph.dot", True)
        with open(graph_file, "w") as g:
            g.write("strict digraph {id} {{\n".format(id=self.id))
            for node in self.nodes:
                fillcolour = node.vertex_color(context)
                label = ''
                ports = [ "<{name}> {name}".format(name=key) for key in node.inputs().keys() ]
                if len(ports) > 0:
                    label = label + '{ ' + '|'.join(ports) + ' } | '
                label = label + node.id
                ports = []
                for key, port in node.outputs().items():
                    dataset = context.acquire(port) if context.has_data(port) else None
                    count = str(len(dataset.rows)) if dataset is not None else None
                    if count:
                        ports.append("<{name}> {name} ({count})".format(name=key, count=count))
                    else:
                        ports.append("<{name}> {name}".format(name=key))
                for key, port in node.errors().items():
                    dataset = context.acquire(port) if context.has_data(port) else None
                    count = str(len(dataset.rows)) if dataset is not None else None
                    if count:
                        ports.append("<{name}> {name} ({count})".format(name=key, count=count))
                    else:
                        ports.append("<{name}> {name}".format(name=key))
                if len(ports) > 0:
                    label = label + ' | { ' + '|'.join(ports) + ' }'
                if not node.is_executable(context):
                    fillcolour = "lightred"
                label = '{ ' + label + ' }'
                g.write('  "{id}" [ shape=record label="{label}" style=filled fillcolor={fillcolour} ]\n'.format(id=node.id, label=label, fillcolour=fillcolour))
            for node in self.nodes:
                for predecessor in node.predecessors():
                    g.write(' "{f}" -> "{t}"\n'.format(f=predecessor.id, t=node.id))
                for key, input in node.inputs().items():
                    source = self.source_node(input)
                    source_key = source.label_for(input)
                    g.write('  "{f}":"{fp}" -> "{t}":"{tp}"\n'.format(f=source.id, fp=source_key, t=node.id, tp=key))
            g.write("}\n")

    def begin(self, context: ProcessingContext):
        super().begin(context)
        context.clear()
        for cd in context.config_dirs:
            self.logger.debug("Configuraration directory " + str(cd))
        self.logger.debug("Input directory " + str(context.input_dir))
        self.logger.debug("Output directory " + str(context.output_dir))
        self.logger.debug("Work directory " + str(context.work_dir))
        dangling_nodes = self.dangling_nodes()
        if dangling_nodes:
            self.logger.error("Nodes %s have dangling inputs", [node.id for node in dangling_nodes])
            raise ProcessingException("Dangling inputs")

    def execute(self, context: ProcessingContext):
        """
        Execute by repeatedly executing any sub-node that can be satisified.

        :param context: The processing context

        :return:
        """
        completed = False
        done = []
        while not completed:
            completed = True
            ready = [node for node in self.nodes if node.is_executable(context) and node not in done]
            for node in ready:
                try:
                    node.run(context)
                    context.completed.add(node.id)
                    done.append(node)
                    if node.no_errors and context.has_errors(node):
                        self.logger.warning("Halting on errors from %s", node)
                        self.execute_dangling_ports(context)
                        return
                    completed = False
                except Exception as err:
                    self.logger.error("Error processing node %s - %s", node.id, err)
                    raise err
        self.execute_dangling_ports(context)
        self.dump_graph(context)
        invalid = [node.id for node in self.nodes if not node.is_executable(context)]
        if completed and len(invalid) > 0:
            raise ProcessingException("Unable to complete nodes " + str(invalid))

@attr.s
class Selector(Node):
    """
    Select a node, based on the input data from a data source
    """
    input: Port = attr.ib()
    nodes: Dict[str, Node] = attr.ib()
    selector_key: Keys = attr.ib()
    directory_key: Keys = attr.ib()
    input_dir_key: Keys = attr.ib()
    output_dir_key: Keys = attr.ib()
    config_dir_key: Keys = attr.ib()
    work_dir_key: Keys = attr.ib()

    @classmethod
    def create(cls, id: str, input: Port, selector_key, directory_key, input_dir_key, output_dir_key, config_dir_key, work_dir_key, *args, **kwargs):
        nodes = { node.id: node for node in args }
        selector_key = Keys.make_keys(input.schema, selector_key)
        directory_key = Keys.make_keys(input.schema, directory_key)
        input_dir_key = Keys.make_keys(input.schema, input_dir_key) if input_dir_key is not None else None
        output_dir_key = Keys.make_keys(input.schema, output_dir_key) if output_dir_key is not None else None
        config_dir_key = Keys.make_keys(input.schema, config_dir_key) if config_dir_key is not None else None
        work_dir_key = Keys.make_keys(input.schema, work_dir_key) if work_dir_key is not None else None
        return Selector(id, input, nodes, selector_key, directory_key, input_dir_key, output_dir_key, config_dir_key, work_dir_key)

    def inputs(self) -> Dict[str, Port]:
        inputs = super().inputs()
        inputs['input'] = self.input
        return inputs

    def locate_directory(self, record: Record, key: Keys, base: str, default: str):
        dir = key.get(record) if key is not None else None
        if dir is None:
            dir = default
        return os.path.join(base, dir)

    def locate_directories(self, record: Record, key: Keys, default: str):
        dir = key.get(record) if key is not None else None
        if dir is None:
            dir = default
        return [choice.strip() for choice in dir.split(',')]

    def execute(self, context: ProcessingContext):
        input = context.acquire(self.input)
        for record in input.rows:
            self.count(self.PROCESSED_COUNT, record, context)
            key = self.selector_key.get(record)
            node = self.nodes.get(key)
            if node is None:
                raise ProcessingException("No matching node for " + key)
            sub_defaults = { field.name: record.data[field.name] for field in input.schema.fields.values() if record.data[field.name] is not None }
            sub_directory = self.directory_key.get(record)
            sub_input = self.locate_directory(record, self.input_dir_key, context.input_dir, sub_directory)
            sub_output = self.locate_directory(record, self.output_dir_key, context.output_dir, sub_directory)
            sub_config = []
            for dir in self.locate_directories(record, self.config_dir_key, sub_directory):
                sub_config += [os.path.join(cd, dir) for cd in context.config_dirs]
            sub_config += context.config_dirs
            sub_work = self.locate_directory(record, self.work_dir_key, context.work_dir, sub_directory)
            sub_context = ProcessingContext.subcontext(context, input_dir=sub_input, config_dirs=sub_config, work_dir=sub_work, output_dir=sub_output, defaults=sub_defaults)
            node.run(sub_context)
            self.count(self.ACCEPTED_COUNT, record, context)