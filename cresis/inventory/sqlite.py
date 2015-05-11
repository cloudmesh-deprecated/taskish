import sqlite3
import api
import datetime
from collections import OrderedDict
import copy
from textwrap import dedent
from operator import itemgetter
import json


class RowIDFetcher(object):

    """sqlite does support a way of retrieving the primary keys of items
    modified using `executemany.` This context manager add support for
    automatically doing so. Upon exit of the context manager, the row
    ids are stored in the `rowids` attribute.
    """

    def __init__(self, connection, table_name, key='id'):
        self._db = connection
        self._table = table_name
        self._oldmax = 0
        self.key = key
        self.rowids = list()

    def __enter__(self):
        c = self._db.cursor()
        stmt = 'SELECT max({key}) from {table}'.format(
            key=self.key,
            table=self._table)
        print stmt
        c.execute(stmt)
        self._oldmax = c.fetchone()[0] or 0
        return self

    def __exit__(self, *args, **kws):
        c = self._db.cursor()
        stmt = 'SElECT {key} FROM {table} WHERE {key} > {oldmax} ORDER BY {key}'\
               .format(table=self._table,
                       oldmax=self._oldmax,
                       key=self.key)
        print stmt
        c.execute(stmt)
        ids = c.fetchall()
        self.rowids = map(itemgetter(self.key), ids)


class CursorCtx(object):

    def __init__(self, connection):
        self._db = connection

    def __enter__(self):
        return self._db.cursor()

    def __exit__(self, *args, **kws):
        print 'COMMIT'
        self._db.commit()


def fetchmany_generator(cursor, fetchsize=10000):
    while True:
        results = cursor.fetchmany(size=fetchsize)
        if not results: break
        for row in results:
            yield row


class TableDef(object):

    def __init__(self, name, column_definitions,
                 primary_keys=None,
                 auto_keys=None,
                 indices=None):
        self._name = name
        self._columns = OrderedDict(column_definitions)
        self._primary_keys = primary_keys or list()
        self._auto_keys = auto_keys or list()
        self._indices = indices or list()

    @property
    def name(self):
        return self._name

    @property
    def columns(self):
        return copy.copy(self._columns)

    @property
    def primary_keys(self):
        return self._primary_keys

    @property
    def auto_keys(self):
        return self._auto_keys

    @property
    def indices(self):
        return self._indices

    def create_table_stmt(self):
        return dedent("""\
        CREATE TABLE {table_name} ({columns})\
        """.format(
            table_name=self.name,
            columns=', '.join(map(' '.join, self.columns.iteritems()))))

    def create_indices_stmt(self):
        return dedent("""\
        CREATE INDEX {table_name}_index ON {table_name} ({cols})\
        """.format(
            table_name=self.name,
            cols=', '.join(self.indices)))

    def create_stmts(self):
        return [
            self.create_table_stmt(),
            self.create_indices_stmt()
            ]

    def insert_keys(self):
        keys = []
        for k in self._columns.iterkeys():
            if k not in self.auto_keys:
                keys.append(k)
        return keys


class File(api.File):

    __table__ = TableDef('file',
                         [('id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
                          ('type', 'TEXT NOT NULL'),
                          ('localpath', 'TEXT NOT NULL'),
                          ('remotepath', 'TEXT NOT NULL'),
                          ('cache', 'INTEGER NOT NULL DEFAULT 1'),
                          ('checksum', 'TEXT')],
                         primary_keys=['id'],
                         auto_keys=['id'],
                         indices=['id'])

class Job(api.Job):

    __table__ = TableDef('job',
                         [('id', 'INTEGER PRIMARY KEY AUTOINCREMENT'),
                         ('status', 'TEXT NOT NULL'),
                         ('location', 'TEXT'),
                         ('created', 'DATETIME'),
                         ('modified', 'DATETIME'),
                         ('task', 'BLOB')],
                         primary_keys=['id'],
                         auto_keys=['id'],
                         indices=['id'])

    def __init__(self, **kws):
        self._id = kws.get('id', None)
        self._status = kws.get('status', api.Status.init)
        self._location = kws.get('location', None)
        self._created = kws.get('created', datetime.datetime.now())
        self._modified = kws.get('modified', self._created)
        self._task = kws.get('task')

    @classmethod
    def from_row(cls, row):
        """Create a job from 

        :param row: :class:`sqlite3.Row`
        :rtype: instance of :class:`Job`
        """
        task = json.loads(row['task'])

        return cls(id=row['id'],
                   status=row['status'],
                   location=row['location'],
                   created=row['created'],
                   modified=row['modified'],
                   task=task)

    @property
    def id(self): return self._id

    @property
    def status(self): return self._status

    @property
    def location(self): return self._location

    @property
    def created(self): return self._created

    @property
    def modified(self): return self._modified

    @property
    def task(self): return self._task

    def vals(self):
        cols = self.__table__.insert_keys()
        vals = []
        for n in cols:
            v = getattr(self, n)
            vals.append(v)
        return vals

    @classmethod
    def create_table(cls, connection):
        tdef = cls.__table__
        with connection:
            for stmt in tdef.create_stmts():
                print stmt
                connection.execute(stmt)


class Inventory(api.Inventory):

    def __init__(self, connection):
        self._connection = connection
        self._connection.row_factory = sqlite3.Row

    def create(self):
        for cls in [Job]:
            cls.create_table(self._connection)

    def insert_tasks(self, tasks):
        def job_values():
            for task in tasks:
                job = Job(task=task.to_json())
                yield tuple(job.vals())

        with self._connection as db, RowIDFetcher(db, Job.__table__.name) as fetch_rowids:
            stmt = dedent("""\
            INSERT INTO {table}({columns}) VALUES ({qstmt})\
            """.format(
                table=Job.__table__.name,
                columns=','.join(Job.__table__.insert_keys()),
                qstmt=','.join(len(Job.__table__.insert_keys())*'?')))
            print stmt
            db.executemany(stmt, job_values())
        return fetch_rowids.rowids

    def query_status(self, status, limit=None, fetchsize=10000):
        stmt = dedent("""\
        SELECT id FROM {table} WHERE status=:status\
        """.format(table=Job.__table__.name))

        if limit > 0:
            stmt = "{select} LIMIT {limit}".format(select=stmt,
                                                   limit=limit)

        with CursorCtx(self._connection) as cursor:
            print stmt
            cursor.execute(stmt, dict(status=status))
            for row in fetchmany_generator(cursor, fetchsize=fetchsize):
                yield row['id']

    def get_jobs(self, jobids, fetchsize=10000):
        stmt = dedent("""\
        SELECT * FROM {table} WHERE id=:id\
        """.format(table=Job.__table__.name))

        with CursorCtx(self._connection) as cursor:
            print stmt
            for id in jobids:
                cursor.execute(stmt, {'id':id})
                for row in fetchmany_generator(cursor, fetchsize=fetchsize):
                    yield Job.from_row(row)
            

    def update_status(self, jobids, status):
        stmt = dedent("""\
        UPDATE {table} SET status=:status,modified=:now where id=:id\
        """.format(table=Job.__table__.name))

        with CursorCtx(self._connection) as cursor:
            now = datetime.datetime.now()
            print stmt
            cursor.executemany(stmt, [{'status':status, 'now':now, 'id':id,}
                                      for id in jobids])


def test():

    def handler():
        print '.',

    conn = sqlite3.connect('test.db')
    sqlite3.enable_callback_tracebacks(True)
    # conn.set_progress_handler(handler, 1)
    i = Inventory(conn)

    try:
        i.create()
    except sqlite3.OperationalError:
        # table already exists
        pass

    def tasks():
        for i in xrange(10):
            t = api.Task('echo hello {} >out'.format(i))
            t.add_file(api.File('out', 'out', api.FileType.output))
            yield t
    print i.insert_tasks(tasks())

    jobids = list(i.query_status(api.Status.init, limit=10))
    for j in i.get_jobs(jobids):
        print j.task['command']
    i.update_status(jobids, api.Status.registered)


if __name__ == '__main__':
    test()
