# er-cassandra

an ER cassandra connector

* migrations (from drift)
* low-level async API (built on alia)
* higher-level async API dealing with unique keys, secondary and lookup tables

## Usage

### Drift migrations

To use _drift_ migrations you will need to install _er-cassandra_ as a
dependency and the _drift_ plugin into your
project

```
[employeerepublic/er-cassandra "0.2.8" ]
[drift "1.5.3"]
```

and create a _drift_ config namespace like this one, which puts the migrations in
`src/api/migrations_cassandra`

```
(ns api.migrate-config
  (:require [er-cassandra.drift.migrate-config :as mc]
            [api.main :as a]))

(defn cassandra []

  (mc/cassandra {:alia-session (s/get-an-er-cassandra-session)
                 :keyspace "a_keyspace"
                 :namespace "er-api.migrate-config/cassandra"
                 :directory "src/api/migrations_cassandra"}))
```

then migrations can be created and run like this :

```
lein create-migration -c api.migrate-config/cassandra create-migration create-users
lein migrate -c api.migrate-config/cassandra
lein migrate -c api.migrate-config/cassandra --version 20150804151456

```

migrations are created with empty `up` and `down` functions - fill
these in to implement the migration like this -

```
(ns api.migrations-cassandra.20150804151456-create-users
  (:require [qbits.alia :as alia]
            [qbits.hayt :as h]
            [er-cassandra.drift.migration-helpers :refer :all]))

(defn up
  "Migrates the database up to version 20150804151456."
  []
  (println "er-api.migrations-cassandra.20150804151456-create-users up...")
  (execute
   "create table users (id timeuuid,
                        username text,
                        name text,
                        created_at timestamp,
                        email set<text>,
                        primary key (id))"))

(defn down
  "Migrates the database down from version 20150804151456."
  []
  (println "er-api.migrations-cassandra.20150804151456-create-users down...")
  (execute
   "drop table users"))
```
### simple client

The simple client is simple

```
(require '[er-cassandra.session.alia :as alia-session])
(require '[er-cassandra.record :as r])

(def session (alia-session/create-session {:contact-points ["localhost"]
                                           :keyspace "a_keyspace"}))

(r/insert session :users {:id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f"
                          :username "foo"
                          :name "Foo McFoo"
                          :email #{"foo@foo.com" "foo.mcfoo@foo.com"}})

@(r/select session :users :id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f")
;; => ({:id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f", :created_at nil, :device_id #{}, :email #{"foo.mcfoo@foo.com" "foo@foo.com"}, :name "Foo McFoo", :username "foo"})

@(r/select-one session :users :id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f")
;; => {:id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f", :created_at nil, :device_id #{}, :email #{"foo.mcfoo@foo.com" "foo@foo.com"}, :name "Foo McFoo", :username "foo"}

(r/update session :users :id {:username "foo.mcfoo"} {:key-value #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f"})

(r/delete session :users :id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f")

```
### higher-level client

The higher-level client requires a model to be defined, and manages unique-keys,
secondary tables and lookup tables

## License

Copyright © 2015 Employee Republic Limited

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
