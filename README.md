# SparkER
_An Entity Resolution framework developed in Scala for Apache Spark._

---

If use this library, please cite:

- **G. Simonini, S. Bergamaschi, & H. V. Jagadish (2016). BLAST: a Loosely Schema-aware Meta-blocking Approach for Entity Resolution. PVLDB, 9(12), 1173–1184.**

- **L. Gagliardelli, G. Simonini, D. Beneventano & S. Bergamaschi. SparkER: Scaling Entity Resolution in Spark. EDBT 2019.**

- **G. Simonini, L. Gagliardelli, S. Bergamaschi & H. V. Jagadish. Scaling entity resolution: A loosely schema-aware approach. Information Systems (2019).**

---

### Entity Resolution
Entity Resolution (*ER*) is the task of identifying different records (a.k.a. entity *profiles*) that pertain to the same real-world entity. Comparing all the possible pairs of records in a data set may be very inefficient (quadratic complexity), in particular in the context of Big Data, e.g., when the records to compare are hundreds of millions. To reduce this complexity, usually ER uses different blocking techniques (e.g. token blocking, n-grams, etc.) to create clusters of profiles (called blocks). The goal of this process is to reduce the global number of comparisons, because will be compared only the records that are in the same blocks.

Unfortunately, in the Big Data context the blocking techniques still produces too many comparisons to be managed in a reasonable time, to reduce more the number of comparison the meta-blocking techniques was introduced [2]. The idea is to create a graph using the information learned from the blocks: the profiles in the blocks represents the nodes of the graph, and the comparisons between them represents the edges. Then is possible to calculate some metrics on the graph and use them to pruning the less significant edges.

### Meta-Blocking for Spark
SparkER implements for Spark the Meta-Blocking techniques described in Simonini et al. [1] and Papadakis et al. [2].

[![stages](https://github.com/Gaglia88/sparker/raw/master/img/stages.png)](#stages)

The process is composed by different stages
1. ***Profile loading***: loads the data (supports csv, json and serialized formats) into entity profiles;
2. ***Blocking***: performs the blocking, token blocking or Loose Schema Blocking [1];
4. ***Block purging***: removes the biggest blocks that are, usually, stopwords or very common tokens that do not provide significant relations [4];
5. ***Block filtering***: for each entity profile, filters out the biggest blocks [3];
6. ***Meta-blocking***: performs the meta-blocking, producing as results the list of candidates pairs that could be matches.

### Datasets
To test SparkER we provide a set of datasets that can be downloaded [here](https://sourceforge.net/projects/sparker/files/). It is also possible to use the [datasets](https://sourceforge.net/projects/erframework/files/) proposed in [2].

### Contacts
For any questions about SparkER write us at name.surname@unimore.it
* Luca Gagliardelli
* Giovanni Simonini
* Song Zhu

### References
[1] Simonini, G., Bergamaschi, S., & Jagadish, H. V. (2016). BLAST: a Loosely Schema-aware Meta-blocking Approach for Entity Resolution. PVLDB, 9(12), 1173–1184. [link](http://www.vldb.org/pvldb/vol9/p1173-simonini.pdf)

[2] Papadakis, G., Koutrika, G., Palpanas, T., & Nejdl, W. (2014). Meta-blocking: Taking entity resolution to the next level. IEEE TKDE.

[3] Papadakis, G., Papastefanatos, G., Palpanas, T., Koubarakis, M., & Green, E. L. (2016). Scaling Entity Resolution to Large , Heterogeneous Data with Enhanced Meta-blocking, 221–232. IEEE TKDE.

[4] Papadakis, G., Ioannou, E., Niederée, C., & Fankhauser, P. (2011). Efficient entity resolution for large heterogeneous information spaces. Proceedings of the Fourth ACM International Conference on Web Search and Data Mining - WSDM ’11, 535.

[5] Gagliardelli, L., Zhu, S., Simonini, G., & Bergamaschi, S. (2018). Bigdedup: a Big Data integration toolkit for duplicate detection in industrial scenarios. In 25th International Conference on Transdisciplinary Engineering (TE2018) (Vol. 7, pp. 1015-1023). [link](https://iris.unimore.it/retrieve/handle/11380/1165040/201434/ATDE7-1015.pdf)
