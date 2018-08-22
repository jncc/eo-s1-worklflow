import logging
import luigi
import os
from datetime import datetime
from os.path import basename, join
from luigi.contrib.s3 import S3Target, S3Client
from workflow_common.s3 import getBucketNameFromS3Path, getPathFromS3Path
from luigi import LocalTarget

def getS3Target(key):
    client = S3Client()
    return S3Target(path=key, client=client)

def getLocalTarget(key):
    return LocalTarget(key)

def getLocalStateTarget(targetPath, fileName):
    targetKey = join(targetPath, fileName)
    return getLocalTarget(targetKey)

def getS3StateTarget(targetPath, fileName):
    targetKey = join(targetPath, fileName)
    return getS3Target(targetKey)

def getProductIdFromSourceFile(sourceFile):
    productFilename = basename(getPathFromS3Path(sourceFile))
    return '%s_%s_%s_%s' % (productFilename[0:3], productFilename[17:25], productFilename[26:32], productFilename[42:48])

def getProductPatternFromSourceFile(sourceFile):
    productFilename = basename(sourceFile)
    return '%s_%s%s%s_%s_%s' % (productFilename[0:3], productFilename[23:25], datetime.strptime(productFilename[21:23], '%m').strftime('%b'), productFilename[17:21], productFilename[26:32], productFilename[42:48])

def getCollectionModeFromSourceFile(sourceFile):
    productFilename = basename(sourceFile)
    return '%s' % (productFilename[4:6])

def getStartDateFromSourceFile(sourceFile):
    productFilename = basename(sourceFile)
    return '%s-%s-%s' % (productFilename[17:21], productFilename[21:23], productFilename[23:25])

def getEndDateFromSourceFile(sourceFile):
    productFilename = basename(sourceFile)
    return '%s-%s-%s' % (productFilename[33:37], productFilename[37:39], productFilename[39:41])

def getProjectionFromOutputFile(outputFile):
    productFilename = basename(outputFile)
    return '%s' % (productFilename[43:51])

def createTestFile(outputfile):
    os.makedirs(os.path.dirname(outputfile), exist_ok=True)
    with open(outputfile, 'w') as f:
        f.write('TEST_FILE')