(ns couchdb-spec
  (:require
   [thi.ng.ldk.core.api :as api]
   [thi.ng.ldk.store.couchdb :refer :all]
   [com.ashafa.clutch :as db]
   [speclj.core :refer :all]
   [environ.core :refer [env]]))

(def url (str (env :couch-url) "/spec-" (System/currentTimeMillis)))

(describe
 "tests overall CouchDB storage adapter..."

 (with st1 (map api/make-resource ["s1" "p1" "o1"]))
 (with st2 (map api/make-resource ["s2" "p2" "o2"]))

 (describe "make-store"
           (it (str "creating a new store succeeds if no db defined at: " url)
               (should-not-throw (make-store url))
               (should-not-be-nil (db/database-info url)))
           (it "creating a store succeeds if db is already defined"
               (make-store url)))

 (describe "add-statement"
           (with ds (make-store url))

           (it "adding a single statement first time results in it being added"
               (should= @st1 (-> @ds
                                 (api/add-statement @st1)
                                 (api/select)
                                 first)))

           (it "confirm total statement count"
               (should= 1 (count (api/select @ds))))

           (it "adding same statement is indempotent"
               (should= 1 (-> @ds
                              (api/add-statement @st1)
                              (api/select)
                              count))))

 (describe "add-many"
           (with ds (make-store url))
           (it "adding same statements is indempotent and only st2 should be added once"
               (should= 2 (-> @ds
                              (api/add-many (take 10000 (cycle [@st1 @st2])))
                              (api/select)
                              count))))

 (describe "delete-store"
           (it "tests that deleting a store actually removes the CouchDB endpoint"
               (should-not-throw (delete-store url))
               (should-be-nil (db/database-info url))))

 )

(run-specs)
