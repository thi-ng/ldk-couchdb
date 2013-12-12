(ns thi.ng.ldk.store.couchdb
  (:require
   [thi.ng.ldk.core.api :as api]
   [thi.ng.common.data.core :refer [vec-conj]]
   [com.ashafa.clutch :as db])
  (:import
   [com.google.common.hash Hashing]
   [java.nio.charset Charset]))

(def ^:const VERSION "0.1.0-SNAPSHOT")
(def ^:const DDOC-ID (str "delta-" VERSION))
(def ^:const BN-PREFIX "bn_:")
(def ^:const BN-LEN 4)

(defn murmur-hash
  ([] (murmur-hash "UTF-8"))
  ([charset]
     (let [murmur (Hashing/murmur3_128)
           cs (Charset/forName charset)]
       #(.. murmur
            (hashString (api/index-value %) cs)
            (toString)))))

(def ^:private spo-template
  "function(doc) {
  for (var p in doc.po) {
    var obj = doc.po[p];
    for (var o in obj) {
      var o = obj[o], val;
      if (typeof o == 'object') {
        val = o.h || o.id;
      } else {
        val = o;
      }
      emit([%s, %s, %s], o);
    }
  }
}")

(def ^:private view-subjects
  {:map "function(doc) { emit(doc._id, 1); }" :reduce "_sum"})

(def ^:private view-preds
  {:map "function(doc) { for (var p in doc.po) emit(p, 1); }" :reduce "_sum"})

(def ^:private view-objects
  {:map "function(doc) {
  for(var p in doc.po) {
    var obj = doc.po[p];
    for(var o in obj) {
      emit(obj[o], 1);
    }
  }
}" :reduce "_sum"})

(def ^:private view-vars {\s "doc._id" \p "p" \o "val"})

(defn- make-view
  "Takes a view ID and returns map of couchdb view template with
  correctly injected vars"
  [id]
  (let [[a b c] (name id)]
    {id {:map (format spo-template (view-vars a) (view-vars b) (view-vars c))}}))

(defn- subject-id
  "Produces a couchdb document ID from given NodeURI or NodeBlank"
  [s]
  (if (api/blank? s)
    (str BN-PREFIX (api/label s))
    (api/index-value s)))

(defn- subject->node
  "Converts a couchdb subject into a NodeURI or NodeBlank"
  [^String s]
  (if (.startsWith s BN-PREFIX)
    (api/make-blank-node (subs s BN-LEN))
    (api/make-resource s)))

(defn- object->node
  "Converts a couchdb map into a NodeLiteral or NodeBlank"
  [l]
  (if (map? l)
    (if (:h l)
      (api/make-literal (:v l) (:l l) (:t l))
      (api/make-blank-node (:id l)))
    (api/make-resource l)))

(defn- literal->map
  "Converts a NodeLiteral into a couchdb map (incl. its hashed value)"
  [hashfn l]
  {:v (api/label l)
   :h (hashfn l)
   :l (api/language l)
   :t (api/datatype l)})

(defn- blank->map
  "Converts a NodeBlank into a couchdb map"
  [b]
  {:id (api/label b)})

(defn- object-value
  "If o is a PNode returns hashed literal value or label. If o is a
  couchdb literal returns o's hash or else o itself."
  [hashfn o]
  (if (satisfies? api/PNode o)
    (if (api/literal? o) (hashfn o) (api/label o))
    (if (map? o) (or (:h o) (:id o)) o)))

(defn- indexed-po?
  "Takes a couchdb doc, hash fn, pred & object nodes. Returns true if
  the given PO relation is present in the given document."
  [doc hashfn p o]
  (when-let [obj (get-in doc [:po p])]
    (let [oi (object-value hashfn o)]
      (some #(= oi %) (map #(object-value hashfn %) obj)))))

(defn- unindex-po
  "Attempts to remove the given PO relation from a document, but
  doesn't update doc serverside. Instead fn returns 2-elem vector of
  [updated-doc and edit?]."
  [doc hashfn p o]
  (let [p (keyword (api/label p))]
    (if-let [obj (get-in doc [:po p])]
      (let [oi (object-value hashfn o)
            po (reduce
                (fn [po o]
                  (if-not (= oi (object-value hashfn o))
                    (conj po o)
                    po))
                [] obj)]
        (if (seq po)
          [(assoc-in doc [:po p] po) true]
          [(update-in doc [:po] dissoc p) true]))
      [doc false])))

(defn- add-statement*
  "Adds the given PO relation to a couchdb doc, but doesn't update
  serverside. Returns updated document."
  [doc hashfn p o]
  (let [p (keyword (api/label p))]
    (if-not (indexed-po? doc hashfn p o)
      [(update-in
        doc [:po p] vec-conj
        (cond
         (api/literal? o) (literal->map hashfn o)
         (api/blank? o) (blank->map o)
         :default (api/label o))) true]
      [doc false])))

(defn- entity-view
  "Returns seq of (reduced) view results mapped with fn."
  [url view fn]
  (->> (db/get-view url DDOC-ID view {:group true})
       (map fn)))

(defn- as-coll
  "Ensures given arg is a collection."
  [xs]
  (if (sequential? xs) xs [xs]))

(defrecord CouchDBStore
    [url hashfn]
  api/PModel
  (add-statement
    [this [s p o]]
    ;; TODO (api/ensure-triple s p o)
    (let [id (subject-id s)
          doc (or (db/get-document url id) {:_id id :po {}})
          [doc edit?] (add-statement* doc hashfn p o)]
      (when edit? (db/put-document url doc))
      this))
  (add-many [this statements]
    ;; TODO (doseq [[s p o] statements] (api/ensure-triple t))
    (doseq [chunk (partition-all 1000 statements)]
      (let [subjects (map (comp subject-id first) chunk)
            docs (reduce
                  (fn [idx s]
                    (if-not (idx s)
                      (assoc idx s (or (db/get-document url s) {:_id s :po {}}))
                      idx))
                  {} subjects)
            [docs edits] (reduce
                          (fn [[idx edits :as state] [s p o]]
                            (let [s (api/label s)
                                  [doc edit?] (add-statement* (idx s) hashfn p o)]
                              (if edit?
                                [(assoc idx s doc) (conj edits s)]
                                state)))
                          [docs #{}] chunk)]
        (when (seq edits)
          (db/bulk-update url (vals (select-keys docs edits))))))
    this)
  (remove-statement
    [this [s p o]]
    ;; TODO (api/ensure-triple s p o)
    (when-let [doc (db/get-document url (subject-id s))]
      (let [[doc edit?] (unindex-po doc hashfn p o)]
        (when edit?
          (if (seq (:po doc))
            (db/put-document url doc)
            (db/delete-document url doc)))))
    this)
  (update-statement
    [this [s p o] [s* p* o* :as s2]]
    ;; TODO (api/ensure-triple s p o)
    ;; TODO (api/ensure-triple s* p* o*)
    (let [s (subject-id s) s* (subject-id s*)]
      (when-let [doc (db/get-document url s)]
        (let [[doc edit?] (unindex-po doc hashfn p o)]
          (when edit?
            (if (seq (:po doc))
              (if (= s s*)
                (let [[doc edit?] (add-statement* doc hashfn p* o*)]
                  (when edit? (db/put-document url doc)))
                (do
                  (db/put-document url doc)
                  (api/add-statement this s2)))
              (db/delete-document url doc))))))
    this)
  (subjects
    [this] (entity-view url "subjects" #(subject->node (:key %))))
  (predicates
    [this] (entity-view url "preds" #(api/make-resource (:key %))))
  (objects
    [this] (entity-view url "objects" #(object->node (:key %))))
  (subject?
    [this x]
    (when (and (satisfies? api/PNode x) (db/get-document url (subject-id x))) x))
  (predicate?
    [this x]
    (when (api/uri? x)
      (let [p (api/label x)]
        (-> url
            (db/get-view DDOC-ID "preds" {:startkey p :endkey (str p " ") :group true})
            (first)
            (when x)))))
  (object?
    [this x]
    (let [o (object-value hashfn x)]
      (-> url
          (db/get-view DDOC-ID "ops" {:startkey [o] :endkey [(str o " ")]})
          (first)
          (when x))))
  (indexed?
    [this x]
    (some #(% this x) [api/subject? api/predicate? api/object?]))
  (remove-subject
    [this s]
    (when-let [doc (db/get-document url (subject-id s))]
      (db/delete-document url doc))
    this)
  (select [this]
    (api/select this nil nil nil))
  (select
    [this s p o]
    (let [s (subject-id s)
          p (api/index-value p)
          o (object-value hashfn o)
          [view s e si pi] (cond
                            (and s p o) ["spo" [s p o] [s p (str o " ")] 0 1]
                            (and s p) ["spo" [s p] [s (str p " ")] 0 1]
                            (and s o) ["sop" [s o] [s (str o " ")] 0 2]
                            (and p o) ["pos" [p o] [p (str o " ")] 2 0]
                            s ["spo" [s] [(str s " ")] 0 1]
                            p ["pos" [p] [(str p " ")] 2 0]
                            o ["ops" [o] [(str o " ")] 2 1]
                            :default ["spo" nil nil 0 1])]
      (->> (when (and s e) {:startkey s :endkey e})
           (db/get-view url DDOC-ID view)
           (map
            (fn [{k :key v :value}]
              [(subject->node (k si))
               (api/make-resource (k pi))
               (object->node v)])))))
  ;; TODO add isomorphic normalization
  (union [this xs]
    (reduce
     #(api/add-many % (api/select %2)) ;; TODO prefix map handling
     this (as-coll xs)))
  (intersection [this xs]
    (let [xs (as-coll xs)
          subjects (set (api/subjects this))
          keep (reduce
                #(reduce
                  (fn [keep s]
                    (if (subjects s) (conj keep s) keep))
                  % (api/subjects %2))
                #{} xs)]
      (doseq [s subjects]
        (when-not (keep s) (api/remove-subject this (api/label s))))
      (doseq [sk keep
              [s p o :as st] (api/select this sk nil nil)]
        (when-not (some #(seq (api/select % s p o)) xs)
          (api/remove-statement this st)))
      this))
  (prefix-map [this] {}))

(defn make-store
  ([url] (make-store url (murmur-hash)))
  ([url hashfn]
     (db/get-database url)
     (let [db (CouchDBStore. url hashfn)
           ddocid (str "_design/" DDOC-ID)
           ddoc (db/get-document url ddocid)]
       (when-not ddoc
         (db/put-document
          url {:_id ddocid
               :language "javascript"
               :views (->> (map make-view [:spo :sop :ops :pos])
                           (concat
                            [{:subjects view-subjects
                              :preds    view-preds
                              :objects  view-objects}])
                           (apply merge))}))
       db)))

(defn delete-store
  [url]
  (db/delete-database url))
