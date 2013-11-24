(ns thi.ng.ldk.store.couchdb
  (:require
   [thi.ng.ldk.core.api :as api]
   [com.ashafa.clutch :as db]))

(defn couch-literal-value
  [o]
  (if (map? o)
    (api/index-value (api/make-literal (:literal o) (:lang o) (:xsd o)))
    o))

(defn indexed-po?
  [doc p o]
  (when-let [obj (get-in doc [:po (keyword p)])]
    (let [oi (api/index-value o)]
      (some #(= oi %) (map couch-literal-value obj)))))

(defrecord CouchDBStore
    [url]
  api/PModel
  (add-statement
    [this [s p o]]
    (let [s (api/label s)
          doc (or (db/get-document url s) {:_id s :po {}})]
      (when-not (indexed-po? doc p o)
        (let [doc (update-in
                   doc [:po (api/label p)] (fnil conj [])
                   (if (api/literal? o)
                     {:literal (api/label o)
                      :lang (api/language o)
                      :xsd (api/datatype o)}
                     (api/label o)))]
          (db/put-document url doc)))
      this))
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
          res (db/get-view url "estuary" view (when (and s e) {:startkey s :endkey e}))]
      (->> res
           (map (fn [{:keys [key value]}]
                  [(api/make-resource (key si))
                   (api/make-resource (key pi))
                   (if (map? o)
                     (api/make-literal (:literal value) (:lang value) (:xsd value))
                     (api/make-resource value))]))))))

(def view-template
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

(defn make-store
  [url]
  (let [db (CouchDBStore. url)
        ddoc (db/get-document url "_design/estuary")]
    (when-not ddoc
      (let [ddoc {:_id "_design/estuary"
                  :language "javascript"
                  :views {:spo {:map (format view-template "doc._id" "p" "val")}
                          :sop {:map (format view-template "doc._id" "val" "p")}
                          :ops {:map (format view-template "val" "p" "doc._id")}
                          :pos {:map (format view-template "p" "val" "doc._id")}}}]
        (db/put-document url ddoc)))
    db))
