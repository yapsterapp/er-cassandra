(ns er-cassandra.model.alia.relationship-test
  (:require
   [er-cassandra.model.util.test :as tu
    :refer [fetch-record
            insert-record
            upsert-instance
            record-stream
            instance-stream
            upsert-instance-stream
            sync-consume-stream]]
   [clojure.test :as test :refer [deftest is are testing use-fixtures]]
   [schema.test :as st]
   [clj-uuid :as uuid]
   [manifold.stream :as s]
   [manifold.deferred :as d]
   [er-cassandra.record :as r]
   [er-cassandra.model.util.timestamp :as ts]
   [er-cassandra.model.types :as t]
   [er-cassandra.model.alia.relationship :as rel]))

(use-fixtures :once st/validate-schemas)
(use-fixtures :each (tu/with-model-session-fixture))

(defn create-simple-relationship
  ([] (create-simple-relationship nil))
  ([cascade-op]
   (tu/create-table :simple_relationship_test
                    "(id timeuuid primary key, nick text)")
   (tu/create-table :simple_relationship_test_target
                    "(id timeuuid primary key, parent_id uuid, nick text)")
   (tu/create-table :simple_relationship_test_target_by_parent_id
                    "(id timeuuid, parent_id uuid, nick text, primary key (parent_id, id))")
   (let [target (t/create-entity
                 {:primary-table {:name :simple_relationship_test_target
                                  :key [:id]}
                  :secondary-tables [{:name :simple_relationship_test_target_by_parent_id
                                      :key [:parent_id :id]}]})
         source (t/create-entity
                 {:primary-table {:name :simple_relationship_test
                                  :key [:id]}
                  :denorm-targets {:test {:target target
                                          :denormalize {:nick :nick}
                                          :cascade (or cascade-op :none)
                                          :foreign-key [:parent_id]}}})]
     [source target])))

(deftest update-simple-relationship-test
  (let [[s t] (create-simple-relationship)
        [sid tid] [(uuid/v1) (uuid/v1)]
        sr {:id sid :nick "foo"}
        _ (insert-record :simple_relationship_test sr)
        _ (upsert-instance t {:id tid :parent_id sid :nick "bar"})

        resp @(rel/denormalize tu/*model-session* s sr :upsert (ts/default-timestamp-opt))
        potr (fetch-record :simple_relationship_test_target [:id] [tid])
        potri (fetch-record :simple_relationship_test_target_by_parent_id
                            [:parent_id] [sid])
        ]
    (is (= [[:test [:ok]]] resp))
    (is (= "foo" (:nick potr)))
    (is (= "foo" (:nick potri)))))

(deftest update-simple-relationship-multiple-records-test
  (let [[s t] (create-simple-relationship)
        [sid tida tidb] [(uuid/v1) (uuid/v1) (uuid/v1)]
        sr {:id sid :nick "foo"}
        _ (insert-record :simple_relationship_test sr)
        _ (upsert-instance t {:id tida :parent_id sid :nick "bar"})
        _ (upsert-instance t {:id tidb :parent_id sid :nick "bar"})

        resp @(rel/denormalize tu/*model-session* s sr :upsert (ts/default-timestamp-opt))
        potra (fetch-record :simple_relationship_test_target [:id] [tida])
        potrai (fetch-record :simple_relationship_test_target_by_parent_id
                             [:parent_id :id] [sid tida])
        potrb (fetch-record :simple_relationship_test_target [:id] [tidb])
        potrbi (fetch-record :simple_relationship_test_target_by_parent_id
                             [:parent_id :id] [sid tidb])
        ]
    (is (= [[:test [:ok]]] resp))
    (is (= "foo" (:nick potra)))
    (is (= "foo" (:nick potrai)))
    (is (= "foo" (:nick potrb)))
    (is (= "foo" (:nick potrbi)))))

(defn create-two-source-relationship
  "creates two source entities denormalizing to the same target entity"
  ([cascade-op]
   (tu/create-table :tpr_source_a
                    "(ida uuid primary key, factor int)")

   (tu/create-table :tpr_source_b
                    "(idb uuid primary key, value int)")

   (tu/create-table :tpr_target
                    (str "(idt uuid, "
                         " source_a_id uuid, "
                         " source_b_id uuid, "
                         " factor int, "
                         " value int, "
                         " primary key ((source_a_id, source_b_id, idt)))"))

   (tu/create-table :tpr_target_by_source_a_id
                    (str "(idt timeuuid, "
                         " source_a_id uuid, "
                         " source_b_id uuid, "
                         " factor int, "
                         " value int, "
                         " primary key (source_a_id, idt))"))

   (tu/create-table :tpr_target_by_source_b_id
                    (str "(idt timeuuid, "
                         "source_a_id uuid, "
                         "source_b_id uuid, "
                         "factor int, "
                         "value int, "
                         "primary key (source_b_id, idt))"))


   (let [target (t/create-entity
                 {:primary-table {:name :tpr_target
                                  :key [[:source_a_id :source_b_id :idt]]}
                  :secondary-tables [{:name :tpr_target_by_source_a_id
                                      :key [:source_a_id :idt]}
                                     {:name :tpr_target_by_source_b_id
                                      :key [:source_b_id :idt]}]})
         source_a (t/create-entity
                   {:primary-table {:name :tpr_source_a
                                    :key [:ida]}
                    :denorm-targets {:test {:target target
                                            :denormalize {:factor :factor}
                                            :cascade (or cascade-op :none)
                                            :foreign-key [:source_a_id]}}})

         source_b (t/create-entity
                   {:primary-table {:name :tpr_source_b
                                    :key [:idb]}
                    :denorm-targets {:test {:target target
                                            :denormalize {:value :value}
                                            :cascade (or cascade-op :none)
                                            :foreign-key [:source_b_id]}}})]
     [source_a source_b target])))


(deftest update-two-sources-many-records-test
  (let [[sa sb t] (create-two-source-relationship :none)

        acnt 10
        ;; all source-a records start out with :factor 1
        saids (repeatedly acnt uuid/v1)
        sars (for [said saids] {:ida said :factor 1})


        bcnt 1000
        ;; initial source-b record :values are drawn from 1..bcnt
        sbids (repeatedly bcnt uuid/v1)
        sbrs (map (fn [sbid v] {:idb sbid :value v}) sbids (iterate inc 1))
        pstep (+ (quot bcnt acnt) (if (> (mod bcnt acnt) 0) 1 0))
        ;; bcnt records divided into acnt partitions
        sbrs-parts (partition pstep pstep nil sbrs)]

    (testing "write initial records"
      (let [trs (flatten
                 (for [{ida :ida factor :factor :as sar} sars]
                   (for [sbrs sbrs-parts]
                     (for [{idb :idb value :value :as sbr} sbrs]
                       {:idt (uuid/v1) :source_a_id ida :source_b_id idb :factor factor :value value}))))

            ;; write initial records
            _ (sync-consume-stream (upsert-instance-stream t trs))

            tr-str (instance-stream t {:buffer-size 10})
            ftr @(s/reduce conj [] tr-str)]

        (is (= (* acnt bcnt) (count ftr)))
        (is (= (* acnt bcnt) (->> ftr (map :factor) (reduce +))))
        (is (= (* acnt (* (/ bcnt 2) (inc bcnt))) (->> ftr (map :value) (reduce +))))))

    (testing "inc the factors"
      (let [isars (->> sars
                       (map (fn [sar]
                              (assoc sar :factor 2))))
            cassandra tu/*model-session*
            a-denorms (->> isars
                           (s/buffer 5)
                           (s/map (fn [isar]
                                    (rel/denormalize cassandra sa isar :upsert (ts/default-timestamp-opt))))
                           (s/realize-each)
                           (s/reduce conj [])
                           deref)

            tr-str (instance-stream t {:buffer-size 10})
            ftr @(s/reduce conj [] tr-str)]

        (is (= (* acnt bcnt) (count ftr)))
        (is (= (* acnt bcnt 2) (->> ftr (map :factor) (reduce +))))
        (is (= (* acnt (* (/ bcnt 2) (inc bcnt))) (->> ftr (map :value) (reduce +))))))

    (testing "inc the values"
      (let [isbrs (->> sbrs
                       (map (fn [{value :value :as sbr}]
                              (assoc sbr :value (inc value)))))
            cassandra tu/*model-session*
            b-denorms (->> isbrs
                           (s/buffer 5)
                           (s/map (fn [isbr]
                                    (rel/denormalize cassandra sb isbr :upsert (ts/default-timestamp-opt))))
                           (s/realize-each)
                           (s/reduce conj [])
                           deref)

            tr-str (instance-stream t {:buffer-size 10})
            ftr @(s/reduce conj [] tr-str)]

        (is (= (* acnt bcnt) (count ftr)))
        (is (= (* 2 acnt bcnt) (->> ftr (map :factor) (reduce +))))
        (is (= (* acnt (- (* (/ (inc bcnt) 2) (+ 2 bcnt)) 1)) (->> ftr (map :value) (reduce +))))))

    (testing "inc the factors and the values together"
      (let [isars (->> sars
                       (map (fn [sar]
                              (assoc sar :factor 3))))
            isbrs (->> sbrs
                       (map (fn [{value :value :as sbr}]
                              (assoc sbr :value (+ 2 value)))))
            cassandra tu/*model-session*

            ;; do both denormalizations together
            a-denorms-d (->> isars
                             (s/buffer 5)
                             (s/map (fn [isar]
                                      (rel/denormalize cassandra sa isar :upsert (ts/default-timestamp-opt))))
                             (s/realize-each)
                             (s/reduce conj []))
            b-denorms-d (->> isbrs
                             (s/buffer 5)
                             (s/map (fn [isbr]
                                      (rel/denormalize cassandra sb isbr :upsert (ts/default-timestamp-opt))))
                             (s/realize-each)
                             (s/reduce conj []))

            ;; sync before retrieval
            _ @(d/zip a-denorms-d b-denorms-d)


            tr-str (instance-stream t {:buffer-size 10})
            ftr @(s/reduce conj [] tr-str)]


        (is (= (* acnt bcnt) (count ftr)))
        (is (= (* 3 acnt bcnt) (->> ftr (map :factor) (reduce +))))
        (is (= (* acnt (- (* (/ (+ 2 bcnt) 2) (+ 3 bcnt)) 3)) (->> ftr (map :value) (reduce +))))))

    ))



(deftest cascade-simple-relationship-multiple-records-test

  (testing "cascading null"
    (let [[s t] (create-simple-relationship :null)
          [sid tida tidb] [(uuid/v1) (uuid/v1) (uuid/v1)]
          sr {:id sid :nick "foo"}
          _ (insert-record :simple_relationship_test sr)
          _ (upsert-instance t {:id tida :parent_id sid :nick "foo"})
          _ (upsert-instance t {:id tidb :parent_id sid :nick "foo"})

          resp @(rel/denormalize tu/*model-session* s sr :delete (ts/default-timestamp-opt))
          potra (fetch-record :simple_relationship_test_target [:id] [tida])
          potrai (fetch-record :simple_relationship_test_target_by_parent_id
                               [:parent_id :id] [sid tida])
          potrb (fetch-record :simple_relationship_test_target [:id] [tidb])
          potrbi (fetch-record :simple_relationship_test_target_by_parent_id
                               [:parent_id :id] [sid tidb])]
      (is (= [[:test [:ok]]] resp))
      (is (= {:id tida :parent_id sid :nick nil} potra))
      (is (= {:id tida :parent_id sid :nick nil} potrai))
      (is (= {:id tidb :parent_id sid :nick nil} potrb))
      (is (= {:id tidb :parent_id sid :nick nil} potrbi))))

  (testing "cascading none"
    (let [[s t] (create-simple-relationship :none)
          [sid tida tidb] [(uuid/v1) (uuid/v1) (uuid/v1)]
          sr {:id sid :nick "foo"}
          _ (insert-record :simple_relationship_test sr)

          _ (upsert-instance t {:id tida :parent_id sid :nick "foo"})
          _ (upsert-instance t {:id tidb :parent_id sid :nick "foo"})

          resp @(rel/denormalize tu/*model-session* s sr :delete (ts/default-timestamp-opt))
          potra (fetch-record :simple_relationship_test_target [:id] [tida])
          potrai (fetch-record :simple_relationship_test_target_by_parent_id
                               [:parent_id :id] [sid tida])
          potrb (fetch-record :simple_relationship_test_target [:id] [tidb])
          potrbi (fetch-record :simple_relationship_test_target_by_parent_id
                               [:parent_id :id] [sid tidb])]
      (is (= [[:test [:ok]]] resp))
      (is (= {:id tida :parent_id sid :nick "foo"} potra))
      (is (= {:id tida :parent_id sid :nick "foo"} potrai))
      (is (= {:id tidb :parent_id sid :nick "foo"} potrb))
      (is (= {:id tidb :parent_id sid :nick "foo"} potrbi))))

  (testing "cascade delete"
    (let [[s t] (create-simple-relationship :delete)
          [sid tida tidb] [(uuid/v1) (uuid/v1) (uuid/v1)]
          sr {:id sid :nick "foo"}
          _ (insert-record :simple_relationship_test sr)
          _ (upsert-instance t {:id tida :parent_id sid :nick "foo"})
          _ (upsert-instance t {:id tidb :parent_id sid :nick "foo"})

          resp @(rel/denormalize tu/*model-session* s sr :delete (ts/default-timestamp-opt))
          potra (fetch-record :simple_relationship_test_target [:id] [tida])
          potrai (fetch-record :simple_relationship_test_target_by_parent_id
                               [:parent_id :id] [sid tida])
          potrb (fetch-record :simple_relationship_test_target [:id] [tidb])
          potrbi (fetch-record :simple_relationship_test_target_by_parent_id
                               [:parent_id :id] [sid tidb])]
      (is (= [[:test [:ok]]] resp))
      (is (= nil potra))
      (is (= nil potrai))
      (is (= nil potrb))
      (is (= nil potrbi)))))

(defn create-composite-key-relationship
  []
  (tu/create-table :ck_relationship_test
                   "(ida uuid, idb uuid, nick text, nock text, primary key ((ida, idb)))")
  (tu/create-table :ck_relationship_test_target
                   "(id uuid primary key, source_ida uuid, source_idb uuid, target_nick text, target_nock text)")
  (tu/create-table :ck_relationship_test_target_by_source_ids
                   "(id uuid, source_ida uuid, source_idb uuid, target_nick text, target_nock text, primary key ((source_ida, source_idb)))")
  (let [target (t/create-entity
                {:primary-table {:name :ck_relationship_test_target
                                 :key [:id]}
                 :secondary-tables [{:name :ck_relationship_test_target_by_source_ids
                                     :key [[:source_ida :source_idb]]}]})
        source (t/create-entity
                {:primary-table {:name :ck_relationship_test
                                 :key [[:ida :idb]]}
                 :denorm-targets {:test {:target target
                                         :denormalize {:nick :target_nick
                                                       :nock :target_nock}
                                         :cascade :none
                                         :foreign-key [:source_ida :source_idb]}}})]
    [source target]))

(deftest update-composite-key-relationship-test
  (let [[s t] (create-composite-key-relationship)
        [sida sidb tid] [(uuid/v1) (uuid/v1) (uuid/v1)]
        ckr {:ida sida :idb sidb :nick "foo" :nock "foofoo"}
        _ (insert-record :ck_relationship_test ckr)
        _ (upsert-instance t {:id tid :source_ida sida :source_idb sidb
                              :target_nick "bar" :target_nock "barbar"})

        resp @(rel/denormalize tu/*model-session* s ckr :upsert (ts/default-timestamp-opt))
        potr (fetch-record :ck_relationship_test_target [:id] [tid])
        potri (fetch-record :ck_relationship_test_target_by_source_ids
                            [:source_ida :source_idb] [sida sidb])
        ]
    (is (= [[:test [:ok]]] resp))
    (is (= {:id tid :source_ida sida :source_idb sidb
            :target_nick "foo" :target_nock "foofoo"} potr))
    (is (= {:id tid :source_ida sida :source_idb sidb
            :target_nick "foo" :target_nock "foofoo"} potri))))

(defn create-multi-relationship
  []
  (tu/create-table :multi_relationship_test
                   "(ida uuid, idb uuid, nick text, nock text, primary key ((ida, idb)))")
  (tu/create-table :multi_relationship_test_target_a
                   "(tida uuid primary key, source_ida uuid, source_idb uuid, target_nick text)")
  (tu/create-table :multi_relationship_test_target_a_by_source_ids
                   "(tida uuid, source_ida uuid, source_idb uuid, target_nick text, primary key ((source_ida, source_idb)))")
  (tu/create-table :multi_relationship_test_target_b
                   "(tidb uuid primary key, source_ida uuid, source_idb uuid, target_nock text)")
  (tu/create-table :multi_relationship_test_target_b_by_source_ids
                   "(tidb uuid, source_ida uuid, source_idb uuid, target_nock text, primary key ((source_ida, source_idb)))")
  (let [target-a (t/create-entity
                  {:primary-table {:name :multi_relationship_test_target_a
                                   :key [:tida]}
                   :secondary-tables [{:name :multi_relationship_test_target_a_by_source_ids
                                       :key [[:source_ida :source_idb]]}]})
        target-b (t/create-entity
                  {:primary-table {:name :multi_relationship_test_target_b
                                   :key [:tidb]}
                   :secondary-tables [{:name :multi_relationship_test_target_b_by_source_ids
                                       :key [[:source_ida :source_idb]]}]})
        source (t/create-entity
                {:primary-table {:name :multi_relationship_test
                                 :key [[:ida :idb]]}
                 :denorm-targets {:test-a {:target target-a
                                           :denormalize {:nick :target_nick}
                                           :cascade :none
                                           :foreign-key [:source_ida :source_idb]}
                                  :test-b {:target target-b
                                           :denormalize {:nock :target_nock}
                                           :cascade :none
                                           :foreign-key [:source_ida :source_idb]}}})]
    [source target-a target-b]))

(deftest update-multi-relationship-test
  (let [[s ta tb] (create-multi-relationship)
        [sida sidb tida tidb] [(uuid/v1) (uuid/v1) (uuid/v1) (uuid/v1)]
        sr {:ida sida :idb sidb :nick "foo" :nock "foofoo"}
        _ (insert-record :multi_relationship_test sr)
        _ (upsert-instance ta {:tida tida :source_ida sida :source_idb sidb
                               :target_nick "bar"})
        _ (upsert-instance tb {:tidb tidb :source_ida sida :source_idb sidb
                               :target_nock "bar"})

        resp @(rel/denormalize tu/*model-session* s sr :upsert (ts/default-timestamp-opt))
        potra (fetch-record :multi_relationship_test_target_a [:tida] [tida])
        potrai (fetch-record :multi_relationship_test_target_a_by_source_ids
                             [:source_ida :source_idb] [sida sidb])
        potrb (fetch-record :multi_relationship_test_target_b [:tidb] [tidb])
        potrbi (fetch-record :multi_relationship_test_target_b_by_source_ids
                             [:source_ida :source_idb] [sida sidb])
        ]
    (is (= [[:test-a [:ok]]
            [:test-b [:ok]]] resp))
    (is (= {:tida tida :source_ida sida :source_idb sidb
            :target_nick "foo"} potra))
    (is (= {:tida tida :source_ida sida :source_idb sidb
            :target_nick "foo"} potrai))
    (is (= {:tidb tidb :source_ida sida :source_idb sidb
            :target_nock "foofoo"} potrb))
    (is (= {:tidb tidb :source_ida sida :source_idb sidb
            :target_nock "foofoo"} potrbi))))
