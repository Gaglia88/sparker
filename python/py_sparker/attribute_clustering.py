import re
import sys
import random
import networkx as nx
from collections import Counter
import math
from objects import Profile, KeyValue, KeysCluster


class Attr(object):
	"""
		Represents an attribute with the source from which belongs
		Attributes
		----------
		sourceName : int
			identifier of the source from which the attribute belongs
		attribute : str
			name of the attribute
	"""
	def __init__(self, sourceName, attribute):
		self.sourceName = sourceName
		self.attribute = attribute

	def __str__(self):
		return str(self.__dict__)

	def __eq__(self, other): 
		return self.sourceName == other.sourceName and self.attribute == other.attribute
	
	def __hash__(self):
		return hash((self.sourceName, self.attribute))

class AttributeClustering(object):
	@staticmethod
	def getHashes(strHash, numHashes, seed = 1234):
		"""
		Given an integer, generates numHashes hashes.
		
		Parameters
		----------
		strHash : int
			integer number to hash
		numHashes : int
			number of hashes to generate
		"""
		random.seed(seed)
		def getPart():
			a = long(random.randint(0, sys.maxint-1))
			b = long(random.randint(0, sys.maxint-1))
			return int(((a * long(strHash) + b) % 2147495899L) % sys.maxint)
		return [getPart() for i in range(0, numHashes)]

	@staticmethod
	def getNumBands(targetThreshold, sigNum):
		b = sigNum
		
		while (pow(1.0 / b, 1.0 / (float(sigNum) / float(b)))) < targetThreshold and b > 1:
			b -= 1

		return b + 1

	@staticmethod
	def getNumRows(targetThreshold, sigNum):
		"""
		Given the targetThreshold and the number of hashes, returns the number of rows for LSH
		
		Parameters
		----------
		targetThreshold : float
			Similarity threshold ]0,1]
		sigNum : int
			Number of hashesh
		"""
		bands = AttributeClustering.getNumBands(targetThreshold, sigNum)
		nrows = sigNum / bands
		if nrows < 1:
			return 1
		else:
			return nrows

	@staticmethod
	def sliding(l, w):
		"""
		Given a list l, split the list in chunks of w size.
		E.g. l=[1,2,3,4,5] w=2, returns [[1,2],[3,4], [5]]
		
		Parameters
		----------
		l : []
			A list 
		w : int
			Chunk size
		"""
		for i in range(0, len(l), w):
			yield l[i:i+w]
	
	@staticmethod	
	def calcSimilarity(sig1, sig2):
		"""
		Given two signatures of the same length compute the Jaccard similarity.
		
		Parameters
		----------
		sig1 : [int]
			First signature
		sig2 : [int]
			Second signature
		"""
		common = 0
		for i in range (0, len(sig1)):
			if sig1[i] == sig2[i]:
				common += 1
		return float(common) / float(len(sig1))

	@staticmethod
	def clusterSimilarAttributes(profiles, numHashes, targetThreshold, maxFactor=1.0, numBands = -1, keysToExclude = [], computeEntropy = False, separator = "_", normalizeEntropy = False):
		"""
			Clusters together similar attributes.
			Returns a list of KeysCluster objects.
			
			Parameters
			----------
			profiles : RDD[Profile]
				RDD of profiles
			numHashes : int
				Number of hashesh to use for LSH
			targetThreshold : float
				Similarity threshold for LSH (]0,1])
			maxFactor : float, optional
				maxFactor
			numBands : int, optional
				Number of bands for LSH, if it is not set, it is computed automatically
			keysToExclude : [string], optional
				List of attributes that are excluded from the clustering process
			computeEntropy : boolean, optional
				If true, compute the entropy for each cluster
			separator : str, optional
				The id of the source is associated to each attribute. The id is separated from the attribute name with this separator.
				E.g. if there is an attribute "name" coming from source 1, and the separator is "_", the attribute is inserted in a cluster as "1_name"
			normalizeEntropy : boolean, optional
				if True the entropy is normalized
				
		"""
		def getTokens(profile):
			attributes = filter(lambda x: x.key not in keysToExclude, profile.attributes)
			split = map(lambda a : (a.key, filter(lambda t: len(t) > 0, map(lambda t: t.strip(), re.split('\W+', a.value.lower())))), attributes)
			d = map(lambda t: [(Attr(profile.sourceId, t[0]), token) for token in t[1]], split)
			return [y for x in d for y in x]
	
		#For each profile produces an RDD of (attribute, token)
		attributesToken = profiles.flatMap(getTokens)
		#For each attribute produces an RDD of (token, [attributes]), to each token is assigned an unique id, so the final RDD is (tokenID, [attribute])
		attributesPerToken = attributesToken.map(lambda a: (a[1], a[0])).groupByKey().zipWithIndex().map(lambda x: (x[1], x[0][1]))
		
		sc = profiles.context

		#Hashes each token, producing a map (token, hashes)
		hashes = sc.broadcast(attributesPerToken.map(lambda a: (a[0], AttributeClustering.getHashes(a[0], numHashes))).collectAsMap())
		
		#Generates an RDD of (attribute, [tokenIDs])
		tokensPerAttribute = attributesPerToken.flatMap(lambda a: [(p, a[0]) for p in a[1]]).groupByKey().map(lambda x: (x[0], set(x[1])))
		
		#Exctract all the attributes
		allAttributes = tokensPerAttribute.map(lambda x: x[0])
		
		def makeSignature(data):
			attribute = data[0]
			tokens = data[1]
			signature = [sys.maxint for x in range(0, numHashes)]
			for t in tokens:
				h = hashes.value[t]
				for i in range(0, numHashes):
					if(h[i] < signature[i]):
						signature[i] = h[i]
			return (attribute, signature)		
		
		#Generates the signature for each attribute, producing an RDD of (attribute, signature)
		attributeWithSignature = tokensPerAttribute.map(makeSignature)

		#Computes the number of rows for each band
		numRows = AttributeClustering.getNumRows(targetThreshold, numHashes)

		def getBuckets(data):
			attribute = data[0]
			signature = data[1]
			buckets = AttributeClustering.sliding(signature, numRows)
			ids = map(lambda b: hash(tuple(b)), buckets)
			return (attribute, ids)

		#Generate the bucket from each signature, the result is (attribute, [buckets])
		buckets = attributeWithSignature.map(getBuckets)

		#Produce an RDD of (bucket, [attributes])
		attributesPerBucket = buckets.flatMap(lambda b: [(x, b[0]) for x in b[1]]).groupByKey()\
							  .map(lambda b: set(b[1]))\
							  .filter(lambda b: len(b) > 1 and len(b) < 101)\
							  .map(lambda b: tuple(b))\
							  .distinct()

		#Collects and sends in broadcast the signatures
		attributeSignatures = attributeWithSignature.collectAsMap()
		hashes.unpersist()
		attributeSignaturesBroadcast = sc.broadcast(attributeSignatures)

		def getEdges(cluster):
			pairs = [(cluster[i], cluster[j]) for i in range(0, len(cluster)) for j in range (i+1, len(cluster))]
			gpairs = filter(lambda p: p[0].sourceName != p[1].sourceName, pairs)
			return map(lambda p: (p[0], (p[1], AttributeClustering.calcSimilarity(attributeSignaturesBroadcast.value[p[0]], attributeSignaturesBroadcast.value[p[1]]))), gpairs)

		#Generate all the possible pairs (attribute1, attribute2, sim(attribute1, attribute2)) from the buckets
		edges = attributesPerBucket.flatMap(getEdges)
		#Now the pairs are grouped by the first attribute, obtaining an RDD of (attribute1, [(attribute1, attribute2, sim(a1,a2)), ..., (attribute1, attributeN, sim(a1, aN))])
		edgesPerKey = edges.union(edges.map(lambda e: (e[1][0], (e[0], e[1][1])))).groupByKey().map(lambda x: (x[0], set(x[1])))

		def getTopEdges(data):
			key1 = data[0]
			keys2 = data[1]
			maxV = max(map(lambda x: x[1], keys2))*maxFactor
			topKeys2 = map(lambda x: x[0], filter(lambda k: k[1] >= maxV, keys2))
			return (key1, topKeys2)
		
		#For each attribute keeps only the top N*maxFactor most similar attributes
		topEdges = edgesPerKey.map(getTopEdges)

		#Attributes kept after the top edges selection
		graphVertices = topEdges.map(lambda x: x[0]).union(topEdges.flatMap(lambda x: x[1])).distinct().collect()
		graphEdges = topEdges.collect()

		#Generates a graph with the attributes as vertex, with the edges the retained top edges
		G = nx.Graph()
		for v in graphVertices:
			G.add_node(v)
			
		for fromV in graphEdges:
			for toV in fromV[1]:
				G.add_edge(fromV[0], toV)

		#Applies the connected components on the graph, obtaining the cluster of attributes.
		clusters = filter(lambda c: len(c) > 1, list(nx.connected_components(G)))
		clusteredAttributes = [y for x in clusters for y in x]
		#Lists of attributes that do not appears in any cluster
		nonClusteredAttributes = map(lambda k: str(k.sourceName) + str(separator) + str(k.attribute), filter(lambda a: a not in clusteredAttributes, allAttributes.collect()))
		clusters = [(clusters[i], i) for i in range(0, len(clusters))]
		#Id of the default cluster (contains the nonClusteredAttributes)
		defaultClusterID = len(clusters)
		attributeSignaturesBroadcast.unpersist()
			
		#List of final clusters
		finalClusters = []

		#Compute the entropy
		if computeEntropy:
			#Map that associate each attribute to the cluster id in which is contained
			keyClusterMap = dict([(attribute, cluster[1]) for cluster in clusters for attribute in cluster[0]])
						
			def getAttributeEntropy(data):
				attribute = data[0]
				tokens = data[1]
				numberOfTokens = float(len(tokens))
				c = Counter(tokens)
				tokensCount = [c[t] for t in set(tokens)]
				tokensP1 = map(lambda tokenCount: float(tokenCount) / numberOfTokens, tokensCount)
				tokensP = map(lambda p_i: p_i * (math.log10(p_i) / math.log10(2.0)), tokensP1)
				entropy = 1.0
				if normalizeEntropy:
					entropy = - sum(tokensP) / (math.log10(numberOfTokens) / math.log10(2.0))
				else:
					entropy = - sum(tokensP)

				return (attribute, entropy)

			#Computes the entropy of each attribute, producing an RDD of (attribute, entropy)
			entropyPerAttribute = attributesToken.groupByKey().map(getAttributeEntropy)
			attributesToken.unpersist()
			
			def associateEntropyToCluster(data):
				attribute = data[0]
				entropy = data[1]
				clusterID = keyClusterMap.get(attribute, defaultClusterID)
				return (clusterID, entropy)

			#Associate each attribute to its cluster, producing an RDD of (clusterID, entropy)
			#then groups together the entropies of the same cluster, and computes the average. This is the cluster' entropy.
			entropyPerCluster = entropyPerAttribute.map(associateEntropyToCluster).groupByKey().map(lambda x: (x[0], sum(x[1])/len(x)))
			entropyMap = entropyPerCluster.collectAsMap()
			defaultEntropy = entropyMap.get(defaultClusterID, 1.0)
			
			def createClusterEntropy(cluster):
				keys = cluster[0]
				clusterID = cluster[1]
				entropy = entropyMap.get(clusterID, 1.0)
				attributes = map(lambda k: str(k.sourceName) + separator + str(k.attribute), keys)
				return KeysCluster(clusterID, attributes, entropy)
			
			#Generates the default cluster
			defaultCluster = KeysCluster(defaultClusterID, nonClusteredAttributes, entropyMap.get(defaultClusterID, 1.0))			
			
			#Put all together, and generates the final clusters.
			finalClusters = list(map(createClusterEntropy, clusters))
			finalClusters.append(defaultCluster)
		else:
			def createCluster(cluster):
				keys = cluster[0]
				clusterID = cluster[1]
				attributes = map(lambda k: str(k.sourceName) + separator + str(k.attribute), keys)
				return KeysCluster(clusterID, attributes)
			
			#Generates the default cluster
			defaultCluster = KeysCluster(defaultClusterID, nonClusteredAttributes)
			
			#Put all together, and generates the final clusters.
			finalClusters = list(map(createClusterEntropy, clusters))
			finalClusters.append(defaultCluster)
			
		return finalClusters