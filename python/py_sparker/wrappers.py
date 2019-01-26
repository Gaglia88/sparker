from  pyspark.sql import SparkSession
from objects import Profile, MatchingEntities, KeyValue

class CSVWrapper(object):
	"""
	Wrapper to load the profiles from a CSV
	"""
    
	@staticmethod
	def loadProfiles(filePath, startIDFrom = 0, separator = ",", header = True, realIDField = "", sourceId = 0):
		"""
		Load the profiles from a CSV file.
		Returns an RDD of profiles.
		
		Parameters
		----------
		
		filePath : str
			path of the CSV file
		startIDFrom : int, optional
			value from which start the profile identifiers
		separator : string, optional
			CSV column separator
		header : boolean, optional
			if true means that the CSV has an header row
		realIDField : str, optional
			name of the attribute that contain the profile identifier in the CSV
		sourceId : int, optional
			source identifier
		"""
		sqlContext = SparkSession.builder.getOrCreate()
		df = sqlContext.read.option("header", header).option("sep", separator).option("delimiter", "\"").csv(filePath)
		columnNames = df.columns
		
		def rowToAttributes(row):
			return filter(lambda k: not k.isEmpty(), [KeyValue(col, row[col]) for col in columnNames])
		
		def getProfile(data):
			profileID = data[1] + startIDFrom
			attributes = data[0]
			realID = ""
			if len(realIDField) > 0:
				realID = "".join(map(lambda a: a.value, filter(lambda a: a.key == realIDField, attributes))).strip()
			
			return Profile(profileID, list(filter(lambda a: a.key != realIDField, attributes)), realID, sourceId)
		
		return df.rdd.map(lambda row : rowToAttributes(row)).zipWithIndex().map(lambda x: getProfile(x))
		
		
	@staticmethod
	def loadGroundtruth(filePath, id1='id1', id2='id2', separator = ",", header = True):
		"""
		Load the groundtruth from a csv file
		Return an RDD of MatchingEntities
		
		Parameters
		----------
		
		filePath : str
			path of the CSV file
		id1 : str, optional
			name of the column that contains the first profile identifier
		id2 : str, optional
			name of the column that contains the second profile identifier
		separator : string, optional
			CSV column separator
		header : boolean, optional
			if true means that the CSV has an header row
		"""
		sqlContext = SparkSession.builder.getOrCreate()
		df = sqlContext.read.option("header", header).option("sep", separator).csv(filePath)
		return df.rdd.map(lambda row: MatchingEntities(row[id1], row[id2]))