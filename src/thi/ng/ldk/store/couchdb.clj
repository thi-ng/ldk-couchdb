(ns thi.ng.ldk.store.couchdb
  (:require
   [thi.ng.ldk.core.api :as api]
   [thi.ng.common.data.core :refer [vec-conj]]
   [com.ashafa.clutch :as db]))

(def ^:const VERSION "0.1.0-SNAPSHOT")
(def ^:const DDOC-ID (str "delta-" VERSION))

(def ^:private view-template
  "function(doc) {
  for(var p in doc.po) {
    var obj = doc.po[p];
    for(var o in obj) {
      var o = obj[o], val;
      if (typeof o == 'object') {
        val = o.literal;
        if (o.lang != undefined) {
          val += '@' + o.lang;
        } else if (o.xsd != undefined) {
          val += '^^<' + o.xsd + '>';
        }
      } else val = o;
      emit([%s, %s, %s], o);
    }
  }
}")

(def  ^:private view-vars {\s "doc._id" \p "p" \o "val"})

(defn- make-view
  [id]
  (let [[a b c] (name id)]
    {id {:map (format view-template (view-vars a) (view-vars b) (view-vars c))}}))

(defn- map->literal
  [m] (api/make-literal (:literal m) (:lang m) (:xsd m)))

(defn- literal->map
  [l]
  {:literal (api/label l)
   :lang (api/language l)
   :xsd (api/datatype l)})

(defn- object-value
  [o] (if (map? o) (api/index-value (map->literal o)) o))

(defn- indexed-po?
  [doc p o]
  (when-let [obj (get-in doc [:po (keyword (api/index-value p))])]
    (let [oi (api/index-value o)]
      (some #(= oi %) (map object-value obj)))))

(defn- unindex-po
  [doc p o]
  (let [p (keyword (api/index-value p))]
    (if-let [obj (get-in doc [:po p])]
      (let [oi (api/index-value o)
            po (reduce
                (fn [po o]
                  (prn :oi oi :ov (object-value o) :o o)
                  (if-not (= oi (object-value o))
                    (conj po o)
                    po))
                [] obj)]
        (if (seq po)
          [(assoc-in doc [:po p] po) true]
          [(update-in doc [:po] dissoc p) true]))
      [doc false])))

(defrecord CouchDBStore
    [url]
  api/PModel
  (add-statement
    [this [s p o]]
    (let [s (api/label s)
          doc (or (db/get-document url s) {:_id s :po {}})]
      (when-not (indexed-po? doc p o)
        (let [doc (update-in
                   doc [:po (api/label p)] vec-conj
                   (if (api/literal? o)
                     (literal->map o)
                     (api/label o)))]
          (db/put-document url doc)))
      this))
  (add-many [this statements]
    (reduce api/add-statement this statements))
  (remove-statement
    [this [s p o]]
    (when-let [doc (db/get-document url (api/label s))]
      (let [[doc edit?] (unindex-po doc p o)]
        (when edit?
          (if (seq (:po doc))
            (db/put-document url doc)
            (db/delete-document url doc)))))
    this)
  (select
    [this s p o]
    (let [[s p o] (map api/index-value [s p o])
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
                     (map->literal value)
                     (api/make-resource value))]))))))

(defn make-store
  [url]
  (let [db (CouchDBStore. url)
        ddocid (str "_design/" DDOC-ID)
        ddoc (db/get-document url ddocid)]
    (when-not ddoc
      (let [ddoc {:_id ddocid
                  :language "javascript"
                  :views (apply merge (map make-view [:spo :sop :ops :pos]))}]
        (db/put-document url ddoc)))
    db))
