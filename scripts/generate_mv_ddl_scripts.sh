#!/bin/bash

#
# generate_mv_ddl_scripts.sh <KEYSPACE>
#

# because of https://issues.apache.org/jira/browse/CASSANDRA-14315
# we need to drop all MVs before loading data with sstableloader,
# and recreate the MVs again afterwards
#
# this script takes a <KEYSPACE>.cql file for a keyspace and generates
# <KEYSPACE>_drop_mvs.cql
# and
# <KEYSPACE>_create_non_mvs.cql
# and
# <KEYSPACE>_create_mvs.cql

cat "${1}.cql" | ./filter_drop_mvs > "${1}_drop_mvs.cql"
cat "${1}.cql" | ./filter_create_non_mvs > "${1}_create_non_mvs.cql"
cat "${1}.cql" | ./filter_create_mvs > "${1}_create_mvs.cql"
