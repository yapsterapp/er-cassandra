#!/bin/bash

# requires GNU parallel and pssh

# - snapshot a cassandra keyspace, SOURCE_KS, on SOURCE_HOSTS nodes
# - retrieve the snapshots to TARGET_DIR and rename so that
#   the SSTables can be loaded to keyspace TARGET_KS
# - run sstableloader in parallel against nodes in the cluster
#   seeded from TARGET_HOSTS
#
# NOTE: thanks to https://issues.apache.org/jira/browse/CASSANDRA-14315
# sstableloader borks on some tables with MVs, so to get around this
# use generate_mv_ddl_scripts.sh on a <KEYSPACE>.cql DDL script
# which will generate scripts to drop/create all the MVs in the keyspace,
# as well as a script which creates all non-mv objects. use the
# scripts to drop all MVs in the keyspace before restoring
#
# the full <KEYSPACE>.cql DDL script must be in the same dir as this script
# because it is needed to filter the list of SSTables so that only
# non-MV SSTables get restored (since MVs have been dropped)
#
# once the restore is complete, recreate the MVs with the generated script

# on a cassandra node (the sstableloader needs to be run as a cassandra user)
# - dump the desired restore schema to a <keyspace>.cql file
# - use the generate_mv_ddl_scripts.sh script to split out non-MV creation DDL
#   and MV create/drop DDL
# - drop all MVs from the keyspace
# - set the env-vars below
# - run the script below

# these will need changing
SOURCE_USER=centos
SOURCE_HOSTS=10.0.6.135,10.0.6.104,10.0.5.160
SOURCE_KS=yapster_20180514
TARGET_HOSTS=10.0.6.135
IGNORE_TARGET_HOSTS=
TARGET_KS=yapdev_20181106

# these may not need changing
SNAPSHOT=`date "+%Y%m%d%H%M"`
CP=cp
SOURCE_DATA_DIR=/var/lib/cassandra/data
SOURCE_CASSANDRA_USER=cassandra
SOURCE_COPY_DIR=/tmp
TARGET_DIR=${SNAPSHOT}
TARGET_CASSANDRA_USER=cassandra
TARGET_CASSANDRA_YAML=/etc/dse/cassandra/cassandra.yaml

# prepare a pssh/hosts file for pssh login across all the SOURCE_HOSTS
mkdir -p pssh
rm -f pssh/hosts
IFS=',' read -r -a __HOSTS__ <<< "${SOURCE_HOSTS}"
for __H__ in "${__HOSTS__[@]}"
do
    printf "${SOURCE_USER}@${__H__}\n" >> pssh/hosts
done

# executes on multiple sources (cassandra nodes) with pssh
# snapshots and copies/hardlinks the snapshot files to a temporary place away from the cassandra data dir
echo "nodetool snapshot -t ${SNAPSHOT} ${SOURCE_KS}" | pssh -I -i -h pssh/hosts
echo "cd ${SOURCE_DATA_DIR} ; find ${SOURCE_KS} -type d -name ${SNAPSHOT} | xargs -I % sh -c \"sudo -u ${SOURCE_CASSANDRA_USER} mkdir -p \\\$(dirname ${SOURCE_COPY_DIR}/${SNAPSHOT}/% )\"" | pssh -I -i -h pssh/hosts
echo "cd ${SOURCE_DATA_DIR} ; find ${SOURCE_KS} -type d -name ${SNAPSHOT} | xargs -I % sh -c \"sudo -u ${SOURCE_CASSANDRA_USER} ${CP} -al % ${SOURCE_COPY_DIR}/${SNAPSHOT}/% \"" | pssh -I -i -h pssh/hosts

# now scp the snapshot sstables over here
for __H__ in "${__HOSTS__[@]}"
do
    mkdir -p ${TARGET_DIR}/${__H__}/${TARGET_KS}
    scp -r ${SOURCE_USER}@${__H__}:${SOURCE_COPY_DIR}/${SNAPSHOT}/${SOURCE_KS}/\* ${TARGET_DIR}/${__H__}/${TARGET_KS}
    # remove the MV dumps to save some space
    find ${TARGET_DIR}/${__H__}/${TARGET_KS} -mindepth 1 -maxdepth 1 -type d | ./filter_sstables_mvs ${TARGET_KS} | xargs rm -rf
done

# now move the snapshot sstables a couple of directories up the hierarchy, which puts them
# directly in the keyspace/table directory, ready for sstableloader
find ${TARGET_DIR} -type f -path \*/snapshots/\* | xargs -I % sh -c "mv % \$(dirname %)/../.."

## BEFORE loading the snapshots, remove all the MVs
cat "${TARGET_KS}.cql" | ./filter_ddl_drop_mvs | cqlsh ${TARGET_HOSTS}

## and disable autocompaction
echo "nodetool disableautocompaction" | pssh -I -i -h pssh/hosts

# now run the sstableloader
for __H__ in "${__HOSTS__[@]}"
do
    find ${TARGET_DIR}/${__H__}/${TARGET_KS} -mindepth 1 -maxdepth 1 -type d | ./filter_sstables_non_mvs ${TARGET_KS} | parallel --jobs 4 -I % "CMD=\"sudo -u ${TARGET_CASSANDRA_USER}  sstableloader -f ${TARGET_CASSANDRA_YAML} -d ${TARGET_HOSTS} % \" ; echo \${CMD} ; \${CMD}"
done

## recreate all the MVs
cat "${TARGET_KS}.cql" | ./filter_ddl_create_mvs | cqlsh ${TARGET_HOSTS}

## and disable autocompaction
echo "nodetool enableautocompaction" | pssh -I -i -h pssh/hosts
