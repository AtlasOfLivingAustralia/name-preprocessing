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

import datetime
import gc
import logging
import os
import tempfile
from typing import List, Set, Dict

import attr

from processing.dataset import Port, Dataset, Record


class ProcessingException(Exception):
    pass

class ProcessingContext:
    pass

class Node:
    pass

@attr.s
class Node:
    PROCESSED_COUNT = "processed"
    ERROR_COUNT = "error"
    ACCEPTED_COUNT = "accepted"

    """
    An abstract node in a transformation process.

    Nodes maintain statistics and logging information for a specific piece of information.
    """
    id: str = attr.ib()
    description: str = attr.ib(default=None, kw_only=True)
    tags: Dict[str, object] = attr.ib(factory=dict, kw_only=True)
    logger = attr.ib(default=None, kw_only=True)
    no_errors: bool = attr.ib(default=True, kw_only=True)
    break_begin: bool = attr.ib(default=False, kw_only=True)
    break_commit: bool = attr.ib(default=False, kw_only=True)
    fail_on_exception: bool = attr.ib(default=False, kw_only=True)
    post_gc: bool = attr.ib(default=False, kw_only=True)
    counts: Dict[str, int] = attr.ib(factory=dict, kw_only=True)

    def __attrs_post_init__(self):
        self.init_logger()
        self.label_ports()

    def init_logger(self):
        if (self.logger is None):
            self.logger = logging.getLogger(self.id)
            self.logger.setLevel(logging.INFO)

    def label_ports(self):
        for name, port in self.inputs().items():
            port.roles.append(self.id + "." + name)
        for name, port in self.outputs().items():
            port.id = self.id + "." + name
            port.roles.append(port.id)
        for name, port in self.errors().items():
            port.id = self.id + "." + name
            port.roles.append(port.id)

    def report(self, context: ProcessingContext):
        """
        Report progress.

        This method is called at regular intervals during processing to provide updates on progress.

        :param context: The processing context
         """
        elapsed = (datetime.datetime.utcnow() - self._started).total_seconds()
        processed = self.counts.get(self.PROCESSED_COUNT, 0)
        speed = "(" + str(round(processed / elapsed)) + "/s) " if elapsed > 0 and processed > 0 else ""
        count_order = [self.PROCESSED_COUNT, self.ACCEPTED_COUNT, self.ERROR_COUNT]
        additional_keys = [key for key in self.counts.keys() if key not in count_order]
        count_order.extend(additional_keys)
        message = [str(self.counts.get(key, 0)) + (" records " + speed if key == self.PROCESSED_COUNT else " ") + key for key in count_order]
        self.logger.info(", ".join(message))

    def count(self, key: str, record: Record, context: ProcessingContext, increment: int = 1):
        """
        Count the number of records processed

        :param key: The type of count
        :param record:The record processed
        :param context: The processing context
        """
        val = self.counts.get(key, 0)
        val += increment
        self.counts[key] = val
        if (key == self.PROCESSED_COUNT and val % context.log_interval == 0):
            self.report(context)


    def begin(self, context: ProcessingContext):
        """
        Begin processing
        :param context: The processing context
        """
        self.logger.setLevel(context.log_level)
        self.logger.addHandler(context.handler)
        self.logger.debug("Starting %s", self.id)
        self._started = datetime.datetime.utcnow()
        if self.break_begin:
            self.logger.info("Break at begin") # Put a breakpoint here if you want to break during debugging for this node

    def _post_gc(self):
        """
        If enabled, do a post-execution GC
        """
        if self.post_gc:
            self.logger.info(f"Garbage collecting - {gc.get_count()}")
            gc.collect()
            self.logger.info(f"Post garbage collecting - {gc.get_count()}")

    def commit(self, context: ProcessingContext):
        """
        Finish processing
        :param context: The processing context
        :return:
        """
        self.report(context)
        self.logger.debug("Committing %s", self.id)
        if context.dump:
            from processing.sink import CsvSink
            for (oid, output) in self.outputs().items():
                did = self.id + "_" + oid
                self.logger.info("Dumping %s", did);
                dump = CsvSink.create(did, output, did + '.csv', 'excel', work=True, reduce=False)
                dump.execute(context)
        if context.parent is not None:
            context.parent.merge(context)
        if self.break_commit:
            self.logger.info("Break at commit") # Put a breakpoint here if you want to break during debugging for this node
        self._post_gc()
        self.logger.removeHandler(context.handler)

    def rollback(self, context: ProcessingContext):
        """
        Finish processing with a roll-back

        TODO - handle commits

        :param context: The processing context
        :return:
        """
        self.report(context)
        self.logger.warning("Rolling back %s", self.id)
        self._post_gc()
        self.logger.removeHandler(context.handler)

    def execute(self, context: ProcessingContext):
        """
        Process this transform.

        By default, this does nothing.

        :param context: The processing context for arguments, configuration details etc.
        """
        pass

    def run(self, context: ProcessingContext):
        """
        Run the transform through a begin - execute - commit/rollback sequence

        :param context: The processing context
        """
        subcontext = ProcessingContext.subcontext(context)
        self.begin(subcontext)
        try:
            self.execute(subcontext)
            self.commit(subcontext)
        except Exception as err:
            self.logger.error("Encountered exception %s", err)
            self.rollback(subcontext)
            raise err

    def inputs(self) -> Dict[str, Port]:
        """
        Get the labelled list of inputs associated with the transform.

        :return: The ports acting as inputs
        """
        return {}

    def outputs(self) -> Dict[str, Port]:
        """
        Get the labelled list of outputs associated with the transform.
        :return:
        """
        return {}

    def errors(self) -> Dict[str, Port]:
        """
        Get the labelled list of error ports associated with the transform.
        :return:
        """
        return {}

    def predecessors(self) -> List[Node]:
        """
        Get the list of nodes that must run before this node.
        :return:
        """
        return []

    def label_for(self, port:Port) -> str:
        for key, candidate in self.inputs().items():
            if candidate is port:
                return key
        for key, candidate in self.outputs().items():
            if candidate is port:
                return key
        for key, candidate in self.errors().items():
            if candidate is port:
                return key
        return None

    def vertex_color(self, context: ProcessingContext):
        """
        The colour to use when displayign a graph of this object
        :param context: The processing context for state queries
        :return: An X11 colour name or other colour suitable for the dot language
        """
        return 'white'

    def is_executable(self, context: ProcessingContext):
        if not all(node.id in context.completed for node in self.predecessors()):
            return False
        return all((context.available(port) for port in self.inputs().values()))

@attr.s
class ProcessingContext(Node):
    """
    A standard context for processing parameters.
    """
    parent: ProcessingContext = attr.ib(default=None, kw_only=True)
    dangling_sink_class = attr.ib(default=None, kw_only=True)
    log_interval: int = attr.ib(default = 100000, kw_only=True)
    log_level: int = attr.ib(default=logging.INFO, kw_only=True)
    handler = attr.ib(kw_only=True)
    defaults = attr.ib(factory=dict, kw_only=True)
    datasets = attr.ib(factory=dict, kw_only=True)
    completed: Set[str] = attr.ib(factory=set, kw_only=True)
    work_dir = attr.ib(kw_only=True)
    config_dirs = attr.ib(kw_only=True)
    input_dir = attr.ib(default=".", kw_only=True)
    output_dir = attr.ib(default=".", kw_only=True) # If set to None then output is sent to the work directory
    clear_work_dir: bool = attr.ib(default=True, kw_only=True)
    fail_on_error: bool = attr.ib(default=True)
    sub_context_count: int = attr.ib(default=0, kw_only=True)
    dump: bool = attr.ib(default=False, kw_only=True)

    @handler.default
    def _default_handler(self):
        handler = logging.StreamHandler()
        handler.setLevel(self.log_level)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        return handler

    @work_dir.default
    def _default_work_dir(self):
        return tempfile.mktemp(prefix='work')

    @config_dirs.default
    def _default_config_dirs(self):
        return ['.']

    @classmethod
    def create(cls, id: str, **kwargs):
        return ProcessingContext(id, **kwargs)

    @classmethod
    def subcontext(cls, parent: ProcessingContext, **kwargs):
        """
        Create a sub-context of another context.

        Unless overridden in the keywords, the subcontext inherits context information from the
        parent context.

        :param parent: The parent context
         :return:
        """
        dangling_sink_class = kwargs.pop('dangling_sink_class', parent.dangling_sink_class)
        log_interval = kwargs.pop('log_interval', parent.log_interval)
        log_level = kwargs.pop('log_level', parent.log_level)
        work_dir = kwargs.pop("work_dir", parent.work_dir)
        config_dirs = kwargs.pop('config_dirs', parent.config_dirs)
        input_dir = kwargs.pop('input_dir', parent.input_dir)
        output_dir = kwargs.pop('output_dir', parent.output_dir)
        clear_work_dir = kwargs.pop('clear_work_dir', parent.clear_work_dir)
        dump = kwargs.pop('dump', parent.dump)
        return cls.create(
            parent.subid(),
            parent=parent,
            dangling_sink_class=dangling_sink_class,
            log_interval=log_interval,
            log_level=log_level,
            work_dir=work_dir,
            config_dirs=config_dirs,
            input_dir=input_dir,
            output_dir=output_dir,
            clear_work_dir=clear_work_dir,
            dump=dump,
            **kwargs
        )

    def subid(self):
        self.sub_context_count += 1
        return self.id + "_" + str(self.sub_context_count)

    def merge(self, subcontext: ProcessingContext):
        """
        Merge a subcontext into this context.

        :param subcontext: The sub-context
        """
        for id, dataset in subcontext.datasets.items():
            self.datasets[id] = dataset

    def save(self, port: Port, dataset: Dataset):
        """
        Put a dataset into the dataset store

        :param port: The port associated with the dataset
        :param dataset: The dataset
        """
        id = port.id
        if id in self.datasets:
            self.logger.error("Dataset for port %s - %s is already set", id, str(port.roles))
            self.count(self.ERROR_COUNT, None, self)
            if self.fail_on_error:
                raise ValueError("Dataset for " + id + " with roles " + str(port.roles) + " is already set")
        self.datasets[id] = dataset

    def available(self, port: Port):
        """
        Is a dataset available?

        :param port: The port for the dataset

        :return: True if that dataset has been computed
        """
        id = port.id
        return id in self.datasets or (self.parent is not None and self.parent.available(port))

    def acquire(self, port: Port) -> Dataset:
        id = port.id
        if id not in self.datasets:
            if self.parent is not None:
                return self.parent.acquire(port)
            raise ProcessingException("Unable to get dataset for " + id)
        return self.datasets[id]

    def has_errors(self, node: Node) -> bool:
        """
        Has this node produced errors?

        :param node: The node

        :return: True if the node has some waiting errors, False otherwise
        """
        for port in node.errors().values():
            dataset = self.datasets.get(port.id)
            if dataset is not None and len(dataset.rows) != 0:
                return True
        if self.parent is not None:
            return self.parent.has_errors(node)
        return False

    def has_data(self, port: Port):
        if not self.available(port):
            return False
        dataset = self.acquire(port)
        return dataset is not None and dataset.rows

    def locate_input_file(self, name: str):
        """
        Locate an input file from possible locations.

        Tried in order: the input directory, the config directory, the work directory.
        If not found, an exception is raised.

        :param name: The file name

        :return: The path to the file
        """
        paths = [self.input_dir] + self.config_dirs + [self.work_dir]
        for dir in paths:
            test = os.path.join(dir, name)
            if os.path.exists(test):
                return test
        self.logger.error("Can't find " + name + " in " + str(paths))
        raise FileNotFoundError(name)

    def locate_output_file(self, name: str, work: bool = False):
        """
        Locate an output file from possible locations


        :param name: The file name
        :param work: True if this is a work file

        :return: The path to the
        """
        base = self.work_dir if work or self.output_dir is None else self.output_dir
        file = os.path.join(base, name)
        dir = os.path.dirname(file)
        if not os.path.exists(dir):
            os.makedirs(dir)
        return file

    def clear(self):
        """Clear the context before working"""
        if self.clear_work_dir and os.path.exists(self.work_dir):
            self.logger.info("Clearing " + self.work_dir)
            for root, dirs, files in os.walk(self.work_dir, False):
                for file in files:
                    os.unlink(os.path.join(root, file))
                for dir in dirs:
                    os.rmdir(os.path.join(root, dir))

    def get_default(self, key: str, default: str=None):
        """
        Get a default value.
        If this context has a parent then the parent is also searched for defaults.

        :param key: The default key
        :param default: The default value if not specified
        :return: The default value or none for missing
        """
        value = self.defaults.get(key)
        if value is None:
            if self.parent is not None:
                value = self.parent.get_default(key, default)
            else:
                value = default
        return value

class NullNode(Node):
    """
    A placeholder node that does nothing
    """
    @classmethod
    def create(cls, id):
        return NullNode(id)

    def execute(self, context: ProcessingContext):
        pass