#!/usr/bin/env python
"""Stampa in JSON la gerarchia delle sottocartelle di una directory."""

import os
import errno
from operator import itemgetter

def child(path):
    hierarchy = {
        'name': os.path.basename(path),
         #'path': path,
    }
    return hierarchy

def path_hierarchy(path):
    hierarchy = {
        'type': 'folder',
        #'name': os.path.basename(path),
        #'path': path,
    }

    try:
        hierarchy['children'] = [
            child(os.path.join(path, contents))
            for contents in (contents for contents in sorted(os.listdir(path)) if os.path.isdir(os.path.join(path, contents)))
        ]
    except OSError as e:
        if e.errno != errno.ENOTDIR:
            raise
        hierarchy['type'] = 'file'
    return hierarchy

if __name__ == '__main__':
    import json
    import sys

    try:
        directory = sys.argv[1]
    except IndexError:
        directory = "../out/"

    #print(path_hierarchy(directory))

    print(json.dumps(path_hierarchy(directory), indent=2, sort_keys=True))
