(ns thi.ng.ldk.store.couchdb
  (:require
   [thi.ng.ldk.core.api :as api]
   [thi.ng.common.data.core :refer [vec-conj]]
   [byte-transforms :as bt]
   [com.ashafa.clutch :as db]))

(def ^:const VERSION "0.1.0-SNAPSHOT")
(def ^:const DDOC-ID (str "delta-" VERSION))

(def ^:dynamic *hashimpl* (comp str #(bt/hash % :murmur64) api/index-value))

(def ^:private view-template
  "function(doc) {
  for(var p in doc.po) {
    var obj = doc.po[p];
    for(var o in obj) {
      var o = obj[o], val;
      if (typeof o == 'object') {
        val = o.hash || o.blank;
      } else {
        val = o;
      }
      emit([%s, %s, %s], o);
    }
  }
}")

(def  ^:private view-vars {\s "doc._id" \p "p" \o "val"})

(defn- make-view
  [id]
  (let [[a b c] (name id)]
    {id {:map (format view-template (view-vars a) (view-vars b) (view-vars c))}}))

(defn- map->node
  "Converts a couchdb map into a NodeLiteral or NodeBlank"
  [m]
  (if (:hash m)
    (api/make-literal (:literal m) (:lang m) (:xsd m))
    (api/make-blank-node (:blank m))))

(defn- literal->map
  "Converts a NodeLiteral into a couchdb map (incl. hashed value)"
  [hashfn l]
  {:literal (api/label l)
   :hash (hashfn l)
   :lang (api/language l)
   :xsd (api/datatype l)})

(defn- blank->map
  "Converts a NodeBlank into a couchdb map"
  [b]
  {:blank (api/label b)})

(defn- object-value
  "If o is a PNode returns hashed literal value or label. If o is a
  couchdb literal returns o's hash or else o itself."
  [hashfn o]
  (if (satisfies? api/PNode o)
    (if (api/literal? o) (hashfn o) (api/label o))
    (if (map? o) (or (:hash o) (:blank o)) o)))

(defn- indexed-po?
  [doc hashfn p o]
  (when-let [obj (get-in doc [:po p])]
    (let [oi (object-value hashfn o)]
      (some #(= oi %) (map #(object-value hashfn %) obj)))))

(defn- unindex-po
  [doc hashfn p o]
  (let [p (keyword (api/label p))]
    (if-let [obj (get-in doc [:po p])]
      (let [oi (object-value hashfn o)
            po (reduce
                (fn [po o]
                  ;;(prn :oi oi :ov (object-value hashfn o) :o o)
                  (if-not (= oi (object-value hashfn o))
                    (conj po o)
                    po))
                [] obj)]
        (if (seq po)
          [(assoc-in doc [:po p] po) true]
          [(update-in doc [:po] dissoc p) true]))
      [doc false])))

(defn add-statement*
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

(defrecord CouchDBStore
    [url hashfn]
  api/PModel
  (add-statement
    [this [s p o]]
    (let [s (api/label s)
          doc (or (db/get-document url s) {:_id s :po {}})
          [doc edit?] (add-statement* doc hashfn p o)]
      (when edit? (db/put-document url doc))
      this))
  (add-many [this statements]
    (doseq [chunk (partition-all 1000 statements)]
      (let [subjects (map (comp api/label first) chunk)
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
    (when-let [doc (db/get-document url (api/label s))]
      (let [[doc edit?] (unindex-po doc hashfn p o)]
        (when edit?
          (if (seq (:po doc))
            (db/put-document url doc)
            (db/delete-document url doc)))))
    this)
  (select
    [this s p o]
    (let [s (api/index-value s)
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
                            :default ["spo" nil nil 0 1])
          res (db/get-view url DDOC-ID view (when (and s e) {:startkey s :endkey e}))]
      (->> res
           (map (fn [{:keys [key value]}]
                  [(api/make-resource (key si))
                   (api/make-resource (key pi))
                   (if (map? value)
                     (map->node value)
                     (api/make-resource value))]))))))

(defn make-store
  ([url] (make-store url *hashimpl*))
  ([url hashfn]
     (let [db (CouchDBStore. url hashfn)
           ddocid (str "_design/" DDOC-ID)
           ddoc (db/get-document url ddocid)]
       (when-not ddoc
         (db/put-document
          url {:_id ddocid
               :language "javascript"
               :views (apply merge (map make-view [:spo :sop :ops :pos]))}))
       db)))
