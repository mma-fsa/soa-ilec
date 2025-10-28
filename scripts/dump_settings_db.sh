#!/bin/bash

python -c "import sqlite3, pprint; pprint.pp(sqlite3.connect('/home/mike/workspace/soa-ilec/soa-ilec/data/settings.db').execute('SELECT * FROM settings').fetchall())"