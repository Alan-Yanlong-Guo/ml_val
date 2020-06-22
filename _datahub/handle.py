#!/usr/bin/env python3

from . import settings

class Handle:
    
    def __init__(self, name):
        
        self.name = name
        self.conf = settings['handles'][name].copy()
        
    
    @staticmethod
    def create(name):
        if name not in settings['handles']:
            raise Exception("Error: unknown handle [%s]" % (name))

        if settings['handles'][name]['type'] == 'mysql':
            return Handle_MySQL(name)

from .handle_mysql import *



