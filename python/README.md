# SparkER for pySpark
Through this library it is possible to use the blocking/meta-blocking with PySpark. 
Both schema-agnostic blocking and Loose-Schema-Aware blocking are implemented.

## Usage
First import the sparker library
```python
import py_sparker as sparker
```
Then, load some data, at this time it is possible to load JSON or CSV files. In this example a clean-clean ER task is performed.
```python
#realIDField is the record identifier
profiles1 = sparker.CSVWrapper.loadProfiles('fodors.csv', header = True, realIDField = "id")
#Max profile id in the first dataset, used to separate the profiles in the next phases
separatorID = profiles1.map(lambda profile: profile.profileID).max()
profiles2 = sparker.CSVWrapper.loadProfiles('zagats.csv', header = True, realIDField = "id", startIDFrom = separatorID+1, sourceId=1)
separatorIDs = [separatorID]
profiles = profiles1.union(profiles2)
```

To analyze the blocking performances it is possible to load a groundtruth
```python
groundtruth = sparker.CSVWrapper.loadGroundtruth("matches_fodors_zagats.csv", id1="fodors_id", id2="zagats_id")
```
The ids inside the groundtruth have to be translated in the profiles ids automatically assigned by Spark

```python
realIdIds1 = sc.broadcast(profiles1.map(lambda p:(p.originalID, p.profileID)).collectAsMap())
realIdIds2 = sc.broadcast(profiles2.map(lambda p:(p.originalID, p.profileID)).collectAsMap())

def convert(gtEntry):
    if gtEntry.firstEntityID in realIdIds1.value and gtEntry.secondEntityID in realIdIds2.value:
        first = realIdIds1.value[gtEntry.firstEntityID]
        second = realIdIds2.value[gtEntry.secondEntityID]
        if (first < second):
            return (first, second)
        else:
            return (second, first)
    else:
        return (-1L, -1L)



newGT = sc.broadcast(set(groundtruth.map(convert).filter(lambda x: x[0] >= 0).collect()))
realIdIds1.unpersist()
realIdIds2.unpersist()
```

Now it is possible to perform the blocking.
There are two kind of blocking implemented: schema-agnostic blocking and loose-schema-aware blocking. The first one do not consider the schema, while the latter tries to automatically align the schema.

### Schema-agnostic blocking
```python
blocks = sparker.TokenBlocking.createBlocks(profiles, separatorIDs)
```

### Loose-schema-aware blocking
```python
#First align the attributes
clusters = sparker.AttributeClustering.clusterSimilarAttributes(profiles, 128, 0.3, computeEntropy=True)
#Then perform the blocking
blocks = sparker.TokenBlocking.createBlocksClusters(profiles, clusters, separatorIDs)
```

### Block cleaning
After the blocking, it is possible to apply the block purging and filtering

```python
blocksPurged = sparker.BlockPurging.blockPurging(blocks, 1.005)
(profileBlocks, profileBlocksFiltered, blocksAfterFiltering) = sparker.BlockFiltering.blockFilteringQuick(blocksPurged, 0.8, separatorIDs)
```
