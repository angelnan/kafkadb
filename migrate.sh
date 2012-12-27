#!/bin/bash

pkill -9 -f trytond
dropdb tryton_nan
createdb -T tryton_nan0 tryton_nan
./kafkadb --make-config

./kafkadb --migrate
