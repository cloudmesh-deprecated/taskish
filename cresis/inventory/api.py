from abc import ABCMeta, abstractmethod, abstractproperty
import json
import uuid


class ToJSON:

    __metaclass__ = ABCMeta

    @abstractmethod
    def to_json_repr(self):
        raise NotImplementedError

    def to_json(self):
        "Return a json representation of the object"
        return json.dumps(self.to_json_repr())


class FromJSON:

    @classmethod
    def from_json(cls, json):
        "Construct an object from json representation"
        raise NotImplementedError



class FileType:

    input = 'input'
    output = 'output'


class File(ToJSON, object):

    def __init__(self, localpath, remotepath, type=None, cache=True):
        self.localpath = localpath
        self.remotepath = remotepath
        assert type is not None
        self.type = type
        self.cache = cache
        self.uuid = uuid.uuid1().urn

    def to_json_repr(self):
        return self.__dict__

    def to_json(self):
        return json.dumps(self.to_json_repr())


class FileBlob(object):
    def __init__(self, path, uuid):
        self.path = path
        self.uuid = uuid
        with open(path, 'rb') as fd:
            self.blob = b64encode(fd.read())

class Task(ToJSON, object):

    def __init__(self, command):
        self.command = command
        self.files = list()
        self._uuid = uuid.uuid1().urn

    @property
    def uuid(self):
        "The UUID of the job"
        return self._uuid

    def add_file(self, file):
        self.files.append(file)

    def to_json_repr(self):
        return dict(command=self.command,
                    files=map(File.to_json_repr, self.files),
                    uuid=self.uuid)

    def to_json(self):
        return json.dumps(self.to_json_repr())


class RunnableTask(ToJSON, object):
    def __init__(self, task, uuids):
        self.task = task
        self.uuids = set(uuids)
        self.input_files = list()
        for file in task.files:
            if file.uuid in self._uuids:
                blob = FileBlob(file.localpath, file.uuid)
                self.input_files.append(blob)


class Job:

    __metaclass__ = ABCMeta

    @abstractproperty
    def id(self):
        "ID of the job in the inventory"

    @abstractproperty
    def status(self):
        "Status of the task"

    @abstractproperty
    def location(self):
        "Resource on which the task is assigned"

    @abstractproperty
    def created(self):
        "When the task was created"

    @abstractproperty
    def modified(self):
        "When the task was updated"

    @abstractproperty
    def task(self):
        "The task to run"


class Status:
    init = 'init'
    registered = 'registered'
    offered = 'offered'
    scheduled = 'scheduled'
    running = 'running'
    fail = 'fail'
    success = 'success'


class Inventory:

    __metaclass__ = ABCMeta

    @abstractmethod
    def insert_tasks(self, collection):
        """Insert a collection of :class:`Job`s

        :param collection: an iterable
        :returns: task ids
        :rtype: iterable of int
        """
        

    @abstractmethod
    def query_status(self, status, limit=None):
        """Retreive task ids with matching status

        :param status: the :class:`Status`
        :param limit: maximum number of results to return [None=all]
        :returns: task ids
        :rtype: iterable of int

        """
        
    @abstractmethod
    def update_status(self, jobids, status):
        """Set the status of the tasks

        :param jobids: the job ids
        :type jobids: iterable of int
        :param status: the :class:`Status`
        """

    @abstractmethod
    def get_jobs(self, jobids):
        """Get the jobs

        :param jobids: the  ids
        :returns: the tasks
        :rtype: iterable of tasks
        """
