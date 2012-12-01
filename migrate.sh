#!/bin/bash

pkill -9 -f trytond
dropdb tryton
createdb -T tryton_backup tryton

./kafkadb --migrate
