################################# Programme Spark en Python ####################################
from pyspark import SparkContext

datas = "datas/datas.csv"
sc = SparkContext("local", "first app")

# lecture et distribution du fichier
data = sc.textFile(datas).map(lambda line: line.split(",")).map(lambda record: (record[0], record[1], record[2]))

# nombre total de commandes
NbCommande = data.count()

print("Nb de commandes: %d" % NbCommande)
# Nombre de clients uniques

ClientUnique = data.map(lambda record: record[0]).distinct().count()
print("Nb clients: %d" % ClientUnique)

# Total des commandes
TotalCom = data.map(lambda record: float(record[2])).sum()
print("Total des commandes: %2.2f" % TotalCom)

# Produit le plus commandé
produits = data.map(lambda record: (record[1], 1.0)).reduceByKey(lambda a, b: a + b).collect()
plusFreq = sorted(produits, key=lambda x: x[1], reverse=True)[0]
print("Produit le plus populaire: %s avec %d commandes" % (plusFreq[0], plusFreq[1]))

################################# Type de données ####################################
from numpy import array
from pyspark.mllib.linalg import Vectors

# vecteur "dense"
# à partir de numpy
denseVec1 = array([1.0, 0.0, 2.0, 4.0, 0.0])
# en utilisant la classe Vectors
denseVec2 = Vectors.dense([1.0, 0.0, 2.0, 4.0, 0.0])
sparseVec1 = Vectors.sparse(5, {0: 1.0, 2: 2.0, 3: 4.0})
sparseVec2 = Vectors.sparse(5, [0, 2, 3], [1.0, 2.0, 4.0])

################################# Utilisations de MLlib - k-means ####################################
from pyspark.mllib.clustering import KMeans
from math import sqrt

# Lire et "distribuer" les données
data = sc.textFile("datas/datas2.txt")
parsedData = data.map(lambda line: array([float(x) for x in line.split(' ')]))
parsedData.collect()

# Recherche des 2 classes
clusters = KMeans.train(parsedData, 2, maxIterations=10, initializationMode="random")


# Qualité de la classification
def error(point):
    center = clusters.centers[
        clusters.predict(point)]
    return sum([x ** 2 for x in (point - center)])


Inert = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
varIntra = Inert / parsedData.count()
print("Variance intraclasse = " + str(varIntra))
clusters.predict([9., 9., 9.])
clusters.predict([0.1, 0.1, 0.1])

# fonction lambda dans map pour "prédire"
# tous les vecteurs
parsedData.map(lambda point: clusters.predict(point)).collect()

################################# Utilisations de MLlib - Régression logistique ####################################
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD

spam = sc.textFile("spam.txt")
normal = sc.textFile("ham.txt")

tf = HashingTF(numFeatures=10000)
spamFeatures = spam.map(lambda email: tf.transform(email.split(" ")))
normalFeatures = normal.map(lambda email: tf.transform(email.split(" ")))
posExamples = spamFeatures.map(lambda features: LabeledPoint(1, features))
negExamples = normalFeatures.map(lambda features: LabeledPoint(0, features))
trainingData = posExamples.union(negExamples)
trainingData.cache()
model = LogisticRegressionWithSGD.train(trainingData)
posTest = tf.transform("O M G GET cheap stuff by sending money to ...".split(" "))
negTest = tf.transform("Hi Dad, I started studying Spark the other ...".split(" "))
print("Prediction for positive test example: %g" % model.predict(posTest))
print("Prediction for negative test example: %g" % model.predict(negTest))

################################# Arbre de discrimination ####################################
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree

# Construction de l’échantillon d’apprentissage
# Format de vecteur avec étiquette
data = [
    LabeledPoint(0.0, [0.0]),
    LabeledPoint(1.0, [1.0]),
    LabeledPoint(1.0, [2.0]),
    LabeledPoint(1.0, [3.0])]
sc.parallelize(data).collect()

# estimation de l’arbre
model = DecisionTree.trainClassifier(
    sc.parallelize(data), 2, {})

# "affichage" de l’arbre
print(model)
print(model.toDebugString())

# Prévision d’une observation de R2
model.predict(array([1.0]))

# Prévision de toute une table
rdd = sc.parallelize([[1.0], [0.0]])
model.predict(rdd).collect()

#################################### Forêt aléatoire ####################################

# iportations des fonctions
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
# construction des données
data = [
LabeledPoint(0.0, [0.0]),
LabeledPoint(0.0, [1.0]),
LabeledPoint(1.0, [2.0]),
LabeledPoint(1.0, [3.0])]
# distribution de la table
trainingData=sc.parallelize(data)
trainingData.collect()
# Estimation du modèle
model = RandomForest.trainClassifier(
trainingData, 2, {}, 3, seed=42)
model.numTrees()
model.totalNumNodes()
# "Affichage" de la forêt
print (model.toDebugString())
# Prévision d’u échantillon
rdd = sc.parallelize([[3.0], [1.0]])
model.predict(rdd).collect()

