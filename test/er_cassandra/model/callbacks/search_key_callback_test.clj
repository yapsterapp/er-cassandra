(ns er-cassandra.model.search-key-callback-test
  (:require
   [clojure.test :as test :refer [deftest is are]]
   [er-cassandra.model.callbacks.search-key-callback :as cb]))

(deftest test-normalize-string
  (is (= "foo" (cb/normalize-string "foo")))
  (is (= "aaeeiioooouuuu aaeeiioooouuuu"
         (cb/normalize-string "aáeéiíoóöőuúüű AÁEÉIÍOÓÖŐUÚÜŰ"))))

(deftest test-prepare-string
  (is (= [] (@#'cb/prepare-string "")))
  (is (= ["foo"] (@#'cb/prepare-string "foo")))
  (is (= ["foo" "bar" "foo bar"] (@#'cb/prepare-string "foo bar"))))

(deftest test-extract-search-keys
  (is (= [] (@#'cb/extract-search-keys nil)))
  (is (= ["foo" "bar" "foo bar"]) (@#'cb/extract-search-keys "foo bar"))
  (is (= ["foo" "bar" "foo bar" "baz"])
      (@#'cb/extract-search-keys ["foo bar" "baz"]))
  (is (= ["foo" "bar" "foo bar" "baz"])
      (@#'cb/extract-search-keys #{"foo bar" "baz"}))
  (is (= ["foo" "bar" "foo bar" "baz"])
      (@#'cb/extract-search-keys {:a "foo bar" :b "baz"})))

(deftest test-create-search-keys-callback
  (is (= {:sk #{"foo" "bar" "foo bar"} :foobar "foo bar"}
         ((@#'cb/create-search-keys-callback :sk :foobar)
          {:foobar "foo bar"}))))
