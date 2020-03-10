#!/usr/bin/env python3

from . import settings
from .handle import Handle

import mysql.connector as mysql
import pandas as pd

class Handle_MySQL(Handle):

    def __init__(self, name):
        super().__init__(name)

        server = settings['mysql-servers'][self.conf['server']]

        self.conn = mysql.connect(
            host = server['hostname'],
            user = server['username'],
            password = server['password'],
            database = self.conf['dbname']
        )

    def read(self, request, **args):

        if request not in self.conf['requests']:
            raise Exception('Error: unknown request [%s:%s].' % (self.name, request))
            
        _args = self.conf['requests'][request]['args'].copy()
        
        for arg in args:
            if arg not in _args:
                raise Exception('Error: unknown argument [%s] for [%s:%s].' % (arg, self.name, request))

            _args[arg][0] = args[arg]

        sql = self.conf['requests'][request]['sql']

        for arg in _args:
            sql = sql.replace('%' + arg + '%', str(_args[arg][0]))

        return pd.read_sql(sql, self.conn)
