# er-cassandra

an asynchronous cassandra connector

* migrations (from [drift](https://github.com/macourtney/drift))
* low-level async API (built on [alia](https://github.com/mpenet/alia))
* higher-level async API dealing with unique keys, secondary and lookup tables
* support for streaming / paging queries
* materialized view support

## Usage

### Drift migrations

To use _drift_ migrations you will need to install _er-cassandra_ as a
dependency and the _drift_ plugin into your
project

```
{:dependencies [...
                [employeerepublic/er-cassandra "0.3.0" ]
                ...}
{:plugins      [...
                [drift "1.5.3"]
                ...}
```

and create a _drift_ config namespace like this one, which puts the migrations in
`migrations/api/migrations_cassandra`

```
(ns config.migrate-config
  (:require [er-cassandra.drift.migrate-config :as mc]
            [api.main :as a]))

(defn migrate-config []

  (mc/cassandra {:alia-session (s/get-an-er-cassandra-session)
                 :keyspace "a_keyspace"
                 :namespace "api.migrate-config/cassandra"
                 :directory "migrations/api/migrations_cassandra"}))
```

then migrations can be created and run like this :

```
lein create-migration create-users
lein migrate
lein migrate --version 20150804151456

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
                        primary key (id))")
  (execute
   "create table users_by_username (id timeuuid,
                                    username text,
                                    name text,
                                    created_at timestamp,
                                    email set<text>,
                                    primary key (username))")
  (execute
   "create table users_by_email (email varchar,
                                 id timeuuid,
                                 primary key (email))"))

(defn down
  "Migrates the database down from version 20150804151456."
  []
  (println "er-api.migrations-cassandra.20150804151456-create-users down...")
  (execute "drop table users")
  (execute "drop table users_by_username")
  (execute "drop table users_by_email"))
```
### record API

The record API operations directly correspond to record-oriented cassandra statements.

You can use the session to directly execute CQL strings or [Hayt](https://github.com/mpenet/hayt) queries

All the operations apart from the `-buffered` operations return a `Deferred` promise of the response.

The `-buffered` operations return a `Stream` of the response records.

```
(require '[er-cassandra.session :as s])
(require '[er-cassandra.session.alia :as alia-session])
(require '[er-cassandra.record :as r])
(require '[qbits.hayt :as h])

(def session (alia-session/create-session {:contact-points ["localhost"]
                                           :keyspace "a_keyspace"}))

(r/insert session :users {:id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f"
                          :username "foo"
                          :name "Foo McFoo"
                          :email #{"foo@foo.com" "foo.mcfoo@foo.com"}})

@(r/select session :users :id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f")
;; => ({:id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f",
;;      :created_at nil, :device_id #{}, :email #{"foo.mcfoo@foo.com" "foo@foo.com"},
;;      :name "Foo McFoo", :username "foo"})

(r/select-buffered session :users :id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f")
;; like select, but returns a Stream of the result records

@(r/select-one session :users :id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f")
;; => {:id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f", :created_at nil,
;;     :device_id #{}, :email #{"foo.mcfoo@foo.com" "foo@foo.com"},
;;     :name "Foo McFoo", :username "foo"}

(r/update session :users :id {:username "foo.mcfoo"} {:key-value #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f"})

(r/delete session :users :id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f")

(s/execute session "select * from users where id=f11c0190-40de-11e5-bb66-c37b19130f2f")
;; returns a Deferred<record-map-seq>

(s/execute-buffered session (h/select :users (h/where [[:= :id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f"]])))
;; returns a Stream of record maps

```
### Model API

The model API requires a model to be defined and will assist you
both with denormalizing data across multiple CQL tables associated with
a single entity, and with querying entities from denormalized tables.

A model must be defined, and then the high-level API will look after secondary
tables, lookup tables and unique-key acquisition with lightweight-transactions

```
(require '[er-cassandra.model :as m])

(m/defmodel Users
  {:primary-table {:name :users
                   :key :id}
   ;; materialized views are supported
   :secondary-tables [{:name :users_by_username
                       :key [:username :id]
                       :view? true}]
   :unique-key-tables [{:name :users_by_email
                        :key :email
                        :collection :list}]
   :lookup-key-tables [{:name :users_by_search_key
                        :key :search_key
                        :collection :set}]})

@(m/upsert session Users  {:id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f"
                           :username "foo"
                           :name "Foo McFoo"
                           :search_key #{"foo" "mcfoo"}
                           :email ["foo@foo.com" "foo.mcfoo@foo.com"]})

;; => << #er_cassandra.model.model_session.ModelInstance{
;;      [{:id #uuid "f11c0190-40de-11e5-bb66-c37b19130f2f",
;;        :created_at nil, :device_id #{},
;;        :search_key #{"foo" "mcfoo"}
;;        :email #{"foo.mcfoo@foo.com" "foo@foo.com"},
;;        :name "Foo McFoo", :username "foo"}
;;       nil]} >>

@(m/upsert session Users {:id #uuid "47d3a3a0-40ec-11e5-84fa-b7ba3fcfbef9"
                           :username "foo-too"
                           :name "Foo Too McFoo"
                           :search_key #{"foo" "too" "mcfoo"}
                           :email #{"foo@foo.com" "foo.too.mcfoo@foo.com"}})

;; => << #er_cassandra.model.model_session.ModelInstance{
;;       [{:id #uuid "47d3a3a0-40ec-11e5-84fa-b7ba3fcfbef9",
;;         :created_at nil, :device_id #{},
;;         :email #{"foo.too.mcfoo@foo.com"},
;;         :search_key #{"foo" "too" "mcfoo"}
;;         :name "Foo Too McFoo", :username "foo-too"}
;;         {[:email] [#{"foo@foo.com"}]}]}>>

;; shows that the second upsert failed to acquire the
;; "foo@foo.com" [:email] unique key

;; select automatically uses lookup or secondary tables to find
;; the primary entity records (if all you want is the secondary table
;; record you can use a Hayt query)
@(m/select-one session Users :email "foo@foo.com")
@(m/select-one session Users :email "foo.too.mcfoo@foo.com")
@(m/select-one session Users :username "foo")

;; select-buffered also uses lookup or secondary tables and
;; joins to return a stream of primary entity records
```

The high-level API calls (apart from the `-buffered` fns) are processed within
the manifold promise monad from
[cats](https://github.com/funcool/cats/blob/master/src/cats/labs/manifold.clj)
and all return a `Deferred` result.

The `-buffered` fns return a Manifold `Stream` of records

## License

Copyright © 2016 Employee Republic Limited

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
