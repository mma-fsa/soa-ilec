#!/bin/bash

python -c "import sqlite3; c=sqlite3.connect('settings.db'); c.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT, value TEXT)'); c.commit(); c.close()"