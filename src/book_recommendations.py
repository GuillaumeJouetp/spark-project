import sys

import itertools
import random
from math import sqrt
from operator import add
from os.path import join, isfile, dirname

from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS

def convertToNumber (s):
    return int.from_bytes(s.encode(), 'little')

def parseRating(line):
    """
    Parses a rating record in books format userId;bookId;rating(;random value).
    """
    fields = line.strip().split(";")
    fields.append(random.randint(1463782, 37483201))

    return int(fields[3]) % 10, (int(fields[0]), int(fields[1]), float(fields[2]))



def parseBook(line):
    """
    Parses a book record format bookId;bookTitle .
    """
    fields = line.strip().split(";")

    return int(fields[0]), fields[1]


def loadRatings(ratingsFile):
    """
    Load ratings from file.
    """
    if not isfile(ratingsFile):
        print("File %s does not exist." % ratingsFile)
        sys.exit(1)
    f = open(ratingsFile, 'r')
    ratings = filter(lambda r: r[2] > 0, [parseRating(line)[1] for line in f])
    f.close()
    if not ratings:
        print("No ratings provided.")
        sys.exit(1)
    else:
        return ratings


def computeRmse(model, data, n):
    """
    Compute RMSE (Root Mean Squared Error).
    """
    predictions = model.predictAll(data.map(lambda x: (x[0], x[1])))
    predictionsAndRatings = predictions.map(lambda x: ((x[0], x[1]), x[2])) \
        .join(data.map(lambda x: ((x[0], x[1]), x[2]))) \
        .values()
    return sqrt(predictionsAndRatings.map(lambda x: (x[0] - x[1]) ** 2).reduce(add) / float(n))


if __name__ == "__main__":
    if (len(sys.argv) != 3):
        print("Usage: /path/to/spark/bin/spark-submit --driver-memory 2g " + \
        "book_recommendation.py BooksDataDir personalRatingsFile")
        sys.exit(1)

    # set up environment
    conf = SparkConf() \
        .setAppName("bookALS") \
        .set("spark.executor.memory", "8g")
    sc = SparkContext(conf=conf)

    # load personal ratings
    myRatings = loadRatings(sys.argv[2])
    myRatingsRDD = sc.parallelize(myRatings, 1)

    # load ratings and book titles

    booksHomeDir = sys.argv[1]

    # ratings is an RDD of (last digit of timestamp, (userId, bookId, rating))
    ratings = sc.textFile(join(booksHomeDir, "BX-Book-Ratings.csv")).map(parseRating)

    # books is an RDD of (bookId, bookTitle)
    books = dict(sc.textFile(join(booksHomeDir, "BX-Books.csv")).map(parseBook).collect())

    numRatings = ratings.count()
    numUsers = ratings.values().map(lambda r: r[0]).distinct().count()
    numBooks = ratings.values().map(lambda r: r[1]).distinct().count()

    print("Got %d ratings from %d users on %d books." % (numRatings, numUsers, numBooks))

    # split ratings into train (60%), validation (20%), and test (20%) based on the
    # last digit of the timestamp, add myRatings to train, and cache them

    # training, validation, test are all RDDs of (userId, bookId, rating)

    numPartitions = 4
    training = ratings.filter(lambda x: x[0] < 6) \
        .values() \
        .union(myRatingsRDD) \
        .repartition(numPartitions) \
        .cache()

    validation = ratings.filter(lambda x: x[0] >= 6 and x[0] < 8) \
        .values() \
        .repartition(numPartitions) \
        .cache()

    test = ratings.filter(lambda x: x[0] >= 8).values().cache()

    numTraining = training.count()
    numValidation = validation.count()
    numTest = test.count()

    print("Training: %d, validation: %d, test: %d" % (numTraining, numValidation, numTest))

    # train models and evaluate them on the validation set

    ranks = [8, 12]
    lambdas = [0.1, 10.0]
    numIters = [10, 20]
    bestModel = None
    bestValidationRmse = float("inf")
    bestRank = 0
    bestLambda = -1.0
    bestNumIter = -1

    for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
        model = ALS.train(training, rank, numIter, lmbda)
        validationRmse = computeRmse(model, validation, numValidation)
        print("RMSE (validation) = %f for the model trained with " % validationRmse + \
        "rank = %d, lambda = %.1f, and numIter = %d." % (rank, lmbda, numIter))
        if (validationRmse < bestValidationRmse):
            bestModel = model
            bestValidationRmse = validationRmse
            bestRank = rank
            bestLambda = lmbda
            bestNumIter = numIter

    testRmse = computeRmse(bestModel, test, numTest)

    # evaluate the best model on the test set
    print("The best model was trained with rank = %d and lambda = %.1f, " % (bestRank, bestLambda) \
    + "and numIter = %d, and its RMSE on the test set is %f." % (bestNumIter, testRmse))

    # compare the best model with a naive baseline that always returns the mean rating
    meanRating = training.union(validation).map(lambda x: x[2]).mean()
    baselineRmse = sqrt(test.map(lambda x: (meanRating - x[2]) ** 2).reduce(add) / numTest)
    improvement = (baselineRmse - testRmse) / baselineRmse * 100
    print("The best model improves the baseline by %.2f" % (improvement) + "%.")

    # make personalized recommendations

    myRatedBookIds = set([x[1] for x in myRatings])
    candidates = sc.parallelize([m for m in books if m not in myRatedBookIds])
    predictions = bestModel.predictAll(candidates.map(lambda x: (0, x))).collect()
    recommendations = sorted(predictions, key=lambda x: x[2], reverse=True)[:50]

    print("Recommendations len : ", len(recommendations))
    print("Books recommended for you:")
    for i in range(len(recommendations)):
        print("%2d: %s" % (i + 1, books[recommendations[i][1]]))


    # clean up
    sc.stop()