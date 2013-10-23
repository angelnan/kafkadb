#!/bin/bash

pkill -9 -f trytond
dropdb nan
createdb -T nan0 nan
./kafka --make-config
rm -rf /tmp/output
rm .kettle/db.cache


pushd kettle;
sh -C pan.sh -file:"/home/angel/projectes/kafkadb/kafkadb_openerpv5_tryton/bank/party_party.ktr" -level:Minimal
popd


./kafka --migrate
