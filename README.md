# SparkER
_An Entity Resolution framework developed in Scala for Apache Spark._

### Entity Resolution for Spark
SparkER implements for Spark the Meta-Blocking techniques described in Simonini et al. [1] and Papadakis et al. [2].

[![stages](https://github.com/Gaglia88/sparker/raw/master/img/stages.png)](#stages)

The process is composed by different stages
1. ***Profile loading***: loads the data (supports csv, json and serialized formats) into entity profiles;
2. ***Blocking***: performs the blocking, token blocking or Loose Schema Blocking [2];
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
[1] Simonini, G., Bergamaschi, S., & Jagadish, H. V. (2016). BLAST: a Loosely Schema-aware Meta-blocking Approach for Entity Resolution. Pvldb, 9(12), 1173–1184.
[2] Papadakis, G., Koutrika, G., Palpanas, T., & Nejdl, W. (2014). Meta-blocking: Taking entity resolutionto the next level. IEEE.
[3] Papadakis, G., Papastefanatos, G., Palpanas, T., Koubarakis, M., & Green, E. L. (2016). Scaling Entity Resolution to Large , Heterogeneous Data with Enhanced Meta-blocking, 221–232. Transactions on Knowledge and Data Engineering, 26(8), 1946–1960.
[4] Papadakis, G., Ioannou, E., Niederée, C., & Fankhauser, P. (2011). Efficient entity resolution for large heterogeneous information spaces. Proceedings of the Fourth ACM International Conference on Web Search and Data Mining - WSDM ’11, 535.
