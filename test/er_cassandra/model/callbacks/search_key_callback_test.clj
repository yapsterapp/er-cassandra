(ns er-cassandra.model.callbacks.search-key-callback-test
  (:require
   [clojure.test :as test :refer [deftest is are use-fixtures testing]]
   [schema.test :as st]
   [er-cassandra.model.callbacks.search-key-callback :as cb]
   [er-cassandra.util.string :refer [normalize-string]]))

(use-fixtures :once st/validate-schemas)

(deftest test-normalize-string
  (is (= "foo" (normalize-string "foo")))
  (is (= "aaeeiioooouuuu aaeeiioooouuuu"
         (normalize-string "aáeéiíoóöőuúüű AÁEÉIÍOÓÖŐUÚÜŰ"))))

(deftest test-prepare-string
  (is (= [] (@#'cb/prepare-string "")))
  (is (= ["foo"] (@#'cb/prepare-string "foo")))
  (is (= ["foo" "bar" "foo bar"] (@#'cb/prepare-string "foo bar"))))

(deftest test-extract-search-keys
  (is (= [] (@#'cb/extract-search-keys nil)))
  (is (= (set ["foo" "bar" "foo bar"])
         (set (@#'cb/extract-search-keys "foo bar"))))
  (is (= (set ["foo" "bar" "foo bar" "baz"])
         (set (@#'cb/extract-search-keys ["foo bar" "baz"]))))
  (is (= (set ["foo" "bar" "foo bar" "baz"])
         (set (@#'cb/extract-search-keys #{"foo bar" "baz"}))))
  (is (= (set ["foo" "bar" "foo bar" "baz"])
         (set (@#'cb/extract-search-keys {:a "foo bar" :b "baz"})))))

(deftest test-create-search-keys-callback
  (is (= {:sk #{"foo" "bar" "foo bar"} :foobar "foo bar"}
         ((@#'cb/create-search-keys-callback :sk :foobar)
          {:foobar "foo bar"}))))

(deftest test-create-search-keys-callback-requires-all-source-cols
  (let [callback (@#'cb/create-search-keys-callback :sk :foo :bar)
        r {:foo "foo" :bar "bar"}
        r+sk {:foo "foo" :bar "bar" :sk #{"foo" "bar"}}]
    (testing "sets search key when all source cols are present"
      (is (= r+sk (callback r)))
      (let [r-bar (assoc r+sk :bar nil)
            r-bar+sk (update r-bar :sk disj "bar")]
        (is (= r-bar+sk (callback r-bar)))))
    (testing "doesn't set search key unless all source cols are present"
      (is (= {:foo "foo"} (callback {:foo "foo"}))))
    (testing "preserves search key when not all source cols are present"
      (is (= (dissoc r+sk :foo) (callback (dissoc r+sk :foo)))))))
