#!/bin/bash

pkill -9 -f trytond
dropdb tryton_nan
createdb -T tryton_nan0 tryton_nan
./kafka --make-config
rm -rf /tmp/output

./kafka --migrate
