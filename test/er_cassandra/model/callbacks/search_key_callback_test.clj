(ns er-cassandra.model.callbacks.search-key-callback-test
  (:require
   [clojure.test :as test :refer [deftest is are use-fixtures testing]]
   [schema.test :as st]
   [er-cassandra.model.callbacks.search-key-callback :as cb]
   [er-cassandra.model.types :as t]
   [er-cassandra.util.string :refer [normalize-string]]
   [er-cassandra.model.util.test :as tu
    :refer [fetch-record insert-record delete-record upsert-instance]]))

(use-fixtures :once st/validate-schemas)
(use-fixtures :each (tu/with-model-session-fixture))

(deftest test-normalize-string
  (is (= "foo" (normalize-string "foo")))
  (is (= "aaeeiioooouuuu aaeeiioooouuuu"
         (normalize-string "aáeéiíoóöőuúüű AÁEÉIÍOÓÖŐUÚÜŰ"))))

(deftest test-prepare-string
  (is (= [] (@#'cb/prepare-string "")))
  (is (= ["foo"] (@#'cb/prepare-string "foo")))
  (is (= ["foo" "bar" "foo bar"] (@#'cb/prepare-string "foo bar"))))

(deftest test-value->search-keys
  (is (= [] (@#'cb/value->search-keys nil)))
  (is (= (set ["foo" "bar" "foo bar"])
         (set (@#'cb/value->search-keys "foo bar"))))
  (is (= (set ["foo" "bar" "foo bar" "baz"])
         (set (@#'cb/value->search-keys ["foo bar" "baz"]))))
  (is (= (set ["foo" "bar" "foo bar" "baz"])
         (set (@#'cb/value->search-keys #{"foo bar" "baz"}))))
  (is (= (set ["foo" "bar" "foo bar" "baz"])
         (set (@#'cb/value->search-keys {:a "foo bar" :b "baz"})))))

(defn create-simple-entity
  []
  (tu/create-table :search_key_callback_test
                   "(id timeuuid primary key, nick text)")
  (t/create-entity
   {:primary-table {:name :search_key_callback_test :key [:id]}}))

(deftest test-create-search-keys-callback
  (let [m (create-simple-entity)]
    (testing "a single source col"
      (let [cb (cb/create-search-keys-callback :sk :foobar)]

        (testing "does nothing if no source-cols and no search-key col"
          (is (= {:baz "bloop"}
                 (t/-before-save
                  cb
                  m
                  {:baz "blah"}
                  {:baz "bloop"}
                  {}))))

        (testing "does nothing if source-col in old-record but not new-record"
          (is (= {:baz "bloop"}
                 (t/-before-save
                  cb
                  m
                  {:baz "blah" :foobar "blimp"}
                  {:baz "bloop"}
                  {}))))

        (testing "does nothing if search-key in old-record but not new-record"
          (is (= {:baz "bloop"}
                 (t/-before-save
                  cb
                  m
                  {:baz "blah" :sk #{}}
                  {:baz "bloop"}
                  {}))))

        (testing "merges if source-col in new-record not in old-record"
          (is (=
               {:baz "bloop" :foobar "blimp" :sk #{"blimp"}}
               (t/-before-save
                cb
                m
                {:baz "blah" }
                {:baz "bloop" :foobar "blimp"}
                {})))
          (is (=
               {:baz "bloop" :foobar "blimp" :sk #{"blargh" "blimp"}}
               (t/-before-save
                cb
                m
                {:baz "blah" :sk #{"blargh"}}
                {:baz "bloop" :foobar "blimp"}
                {}))))

        (testing "sets search-keys col"
          (is (= {:sk #{"foo" "bar" "foo bar"} :foobar "foo bar"}
                 (t/-before-save
                  cb
                  m
                  nil
                  {:foobar "foo bar"}
                  {})))
          (is (= {:sk #{"foo" "bar" "foo bar"} :foobar "foo bar"}
                 (t/-before-save
                  cb
                  m
                  {:foobar nil :sk #{}}
                  {:foobar "foo bar"}
                  {}))))))

    (testing "multiple source cols"
      (let [cb (cb/create-search-keys-callback :sk :foo :bar)]

        (testing "does nothing if no source-cols and no search-key col"
          (is (= {:baz "bloop"}
                 (t/-before-save
                  cb
                  m
                  {:baz "blah"}
                  {:baz "bloop"}
                  {}))))

        (testing "merges if source-col not in old-record"
          (is (=
               {:baz "bloop" :foo "bloo" :bar "groo" :sk #{"bloo" "groo"}}
               (t/-before-save
                cb
                m
                {:baz "blah" :foo "bloo"}
                {:baz "bloop" :foo "bloo" :bar "groo"}
                {}))))


        (testing "sets search-keys col"
          (is (= {:sk #{"foo" "bar" "foo bar"} :foo "foo" :bar "foo bar"}
                 (t/-before-save
                  cb
                  m
                  nil
                  {:foo "foo" :bar "foo bar"} {})))
          (is (= {:sk #{"foo" "bar" "foo bar"} :foo "foo" :bar "foo bar"}
                 (t/-before-save
                  cb
                  m
                  {:foo nil :bar nil :sk #{}}
                  {:foo "foo" :bar "foo bar"} {}))))))))
