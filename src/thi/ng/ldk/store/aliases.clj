(def empty-component-graph {:canonical {}, :components {}})

(defn unite
  [{:keys [canonical components] :as graph} [k1 k2]]
  (let [canon1 (canonical k1 k1)
        canon2 (canonical k2 k2)]
    (if (= canon1 canon2)
      graph
      (let [comp1 (components canon1 [canon1])
            comp2 (components canon2 [canon2])
            [canon1 canon2 comp1 comp2] (if (<= (count comp1) (count comp2))
                                          [canon1 canon2 comp1 comp2]
                                          [canon2 canon1 comp2 comp1])
            canonical' (into canonical (for [item comp1] [item canon2]))
            components' (-> components
                            (dissoc canon1)
                            (assoc canon2 (into comp2 comp1)))]
        {:canonical canonical'
         :components components'}))))

(defn same-as? [{:keys [canonical]} k1 k2]
  (= (canonical k1 k1) (canonical k2 k2)))

(time
 (dotimes [i 1]
   (def g
     (reduce
      (fn [a [x y]] (unite a [x y]))
      empty-component-graph
      '[[x d] [a b] [b d] [e f] [d y] [c y] [y b] [g e]]))))

(time (dotimes [i 1] (def g (reduce unite empty-component-graph pairs))))

;; http://www.cs.princeton.edu/~rs/AlgsDS07/01UnionFind.pdf

(defn root
  "Takes a node map `nodes` and node `x`, returns root node of `x`."
  [nodes x] (if (= x (nodes x x)) x (recur nodes (nodes x))))

(defn root
  "Takes a node map `nodes` and node `x`, returns root node of `x`."
  [nodes x]
  (loop [x x, nx (nodes x)]
    (if (= x (or nx x)) x (recur nx nx))))

(defn root*
  "Like `root`, but compresses path while walking tree.
  Returns [updated-tree root-node]."
  [nodes x]
  (let [tx (nodes x x)]
    (if (= x tx)
      [nodes x]
      (let [tx' (nodes tx tx)
            nodes' (assoc nodes x tx')]
        (recur nodes' tx')))))

(defn root*
  "Like `root`, but compresses path while walking tree.
  Returns [updated-tree root-node]."
  [nodes x]
  (loop [nodes nodes, x x, tx (nodes x x)]
    (if (= x tx)
      [nodes x]
      (let [tx' (nodes tx tx)
            nodes' (assoc nodes x tx')]
        (recur nodes' tx' tx')))))

(defn same-as?
  "Takes a graph structure and two node keys, returns true if keys
  have same root."
  [{:keys [nodes]} p q]
  (= (root nodes p) (root nodes q)))



(defn unite
  "Take a graph structure and two keys and joins them under a common
  root node and compresses paths. Returns updated graph."
  [{:keys [nodes sizes] :or {nodes {} sizes {}} :as g} [p q]]
  (let [[nodes i] (root* nodes p)
        [nodes j] (root* nodes q)
        di (sizes i 1)
        dj (sizes j 1)
        [i j] (if (< di dj) [i j] [j i])
        g (assoc g :nodes (assoc nodes i j))
        g (update-in g [:nodes] #(reduce-kv (fn [n k v] (if (= v i) (assoc n k j) n)) % %))
        g (if (= i j)
            g
            (assoc g :depth (-> sizes (dissoc i) (assoc j (+ di dj)))))]
    g))

(defn unite
  "Take a graph structure and two keys and joins them under a common
  root node and compresses paths. Returns updated graph."
  [{:keys [nodes sizes] :or {nodes {} sizes {}} :as g} [p q]]
  (let [[nodes i] (root* nodes p)
        [nodes j] (root* nodes q)
        di (sizes i 1)
        dj (sizes j 1)
        [i j] (if (< di dj) [i j] [j i])
        nodes (assoc nodes i j)
        g (assoc g :nodes (reduce-kv (fn [n k v] (if (= v i) (assoc n k j) n)) nodes nodes))
        g (if (= i j) g
              (assoc g :depth (-> sizes (dissoc i) (assoc j (+ di dj)))))]
    ;;(prn i j g)
    g))

(defprotocol PUnionFind
  (root [this p])
  (root-compress [this p])
  (all-same [this p])
  (same-as? [this p q])
  (unite [this [p q]]))

(defrecord UDGraph [nodes sizes]
  PUnionFind

  (root [this x]
    (loop [x x, nx (nodes x)]
      (if (= x (or nx x)) x (recur nx nx))))

  (root-compress [this x]
    (loop [nodes nodes, x x, tx (nodes x x)]
      (if (= x tx)
        [(assoc this :nodes nodes) x]
        (let [tx' (nodes tx tx)
              nodes' (assoc nodes x tx')]
          (recur nodes' tx' tx')))))

  (all-same [this k]
    (let [k  (root this k)
          ks (->> nodes
                  (filter #(= k (val %)))
                  (map first)
                  (set))]
      (conj ks k)))

  (same-as? [this p q]
    (= (root this p) (root this q)))

  (unite [this [p q]]
    (let [[this i] (root-compress this p)
          [this j] (root-compress this q)
          di (sizes i 1)
          dj (sizes j 1)
          [i j] (if (< di dj) [i j] [j i])
          nodes (assoc (:nodes this) i j)
          this (assoc this :nodes (reduce-kv (fn [n k v] (if (= v i) (assoc n k j) n)) nodes nodes))]
      (if (= i j)
        this
        (assoc this :sizes (-> sizes (dissoc i) (assoc j (+ di dj))))))))

(def pairs
  '[[x d] [a b] [b d] [e f] [d y] [c y] [y b] [g e] [h i] [i l] [j k] [k l] [d h]])

(def pairs
  (let [ids (vec (map #(->> % (str)) (range 10000)))
        n (count ids)]
    (repeatedly 10000 (fn [] [(ids (rand-int n)) (ids (rand-int n))]))))

(time (dotimes [i 1] (def g (reduce unite {} pairs))))

(time (dotimes [i 1] (def g (reduce unite (UDGraph. {} {}) pairs))))

(time (dotimes [i 100] (doseq [[a b] pairs] (same-as? g a b))))

(def g (reduce unite {} '[[3 4] [4 9] [8 0] [2 3] [5 6] [5 9] [7 3] [4 8] [6 1]]))


(defn unite
  [aliases [x y]]
  (let [matches (filter #(or (% x) (% y)) aliases)]
    (if (seq matches)
      (let [a' (reduce disj aliases matches)
            m' (reduce #(into % %2) matches)]
        (conj a' (conj m' x y)))
      (conj aliases (hash-set x y)))))

(defn same-as?
  [aliases x y] (some #(and (% x) (% y)) aliases))
