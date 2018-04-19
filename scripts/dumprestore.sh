#!/bin/bash

# requires GNU parallel and pssh

SNAPSHOT=`date "+%Y%m%d%H%M"`
SOURCE_HOSTS=52.215.83.105,52.18.63.227
SOURCE_USER=mccraig
CP=cp
SOURCE_KS=yapster_1_18_0
SOURCE_DATA_DIR=/var/lib/cassandra/data
SOURCE_CASSANDRA_USER=cassandra
SOURCE_COPY_DIR=/tmp
TARGET_DIR=${SNAPSHOT}
TARGET_KS=yapstaging
TARGET_CASSANDRA_USER=cassandra
TARGET_CASSANDRA_YAML=/etc/dse/cassandra/cassandra.yaml
TARGET_HOSTS=10.0.6.135
IGNORE_TARGET_HOSTS=10.0.6.2,10.0.5.199,10.0.6.49

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
done

# now move the snapshot sstables a couple of directories up the hierarchy, which puts them
# directly in the keyspace/table directory, ready for sstableloader
find ${TARGET_DIR} -type f -path \*/snapshots/\* | xargs -I % sh -c "mv % \$(dirname %)/../.."

# now run the sstableloader
for __H__ in "${__HOSTS__[@]}"
do
    find ${TARGET_DIR}/${__H__}/${TARGET_KS} -mindepth 1 -maxdepth 1 -type d | ./filter_non_mv_sstables ${TARGET_KS} | parallel --jobs 10 -I % "CMD=\"sudo -u ${TARGET_CASSANDRA_USER}  sstableloader -f ${TARGET_CASSANDRA_YAML} -d ${TARGET_HOSTS} -i ${IGNORE_TARGET_HOSTS} %\" ; echo \${CMD} ; \${CMD}"
done
