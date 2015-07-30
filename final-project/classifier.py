__author__ = 'tarciso'

import sys
import json
from hadoopUtils import HadoopUtils

MIN_NUM_ARGS = 4

#FOLDERS DEFAULT NAMES
INPUT_FOLDER_NAME = "input"
TRAIN_HISTS_FOLDER_NAME = "train-hists"
TEST_HISTS_FOLDER_NAME = "test-hists"
RESULTS_FOLDER_NAME = "results"

#TAGS
HDFS_BASE_FOLDER_TAG = "HDFSBaseFolder"
N_TAG = "N"
PREPROCESSOR_MAPPER_EXEC_PATH = "preprocessorMapperExecPath"
PREPROCESSOR_REDUCER_EXEC_PATH = "preprocessorReducerExecPath"
CLASSIFIER_MAPPER_EXEC_PATH = "classifierMapperExecPath"
CLASSIFIER_REDUCER_EXEC_PATH = "classifierReducerExecPath"
CENTROIDS_HDFS_PATH = "centroidsHDFSPath"

configs = None


def printUsage():
	print "classifier.py <testImagesJsonFilePath> <confFilePath> <outputFilePath>"

def storeInputIntoHDFS(inputLocalFilePath,inputFolder):
	HadoopUtils.put(inputLocalFilePath,inputFolder)

def preprocessInput(mapperExecPath,reducerExecPath,inputDir,outputDir,centroidsFile):
	centroidsHDFSFullPath = HadoopUtils.getHDFSFullPath(centroidsFile)
	centroidsFileName = centroidsFile.split("/")[-1]
	mapperCommandStr = HadoopUtils.buildExecCommandStr([mapperExecPath,centroidsFileName,str(2)])

	HadoopUtils.removeHDFSDirIfExists(outputDir)

	HadoopUtils.runHadoopStreamingJob(input=inputDir,
                                  output=outputDir,
                                  mapperCommand=mapperCommandStr,
				  numReducerTasks=1,
				  reducerCommand=reducerExecPath,
				  filesArray=[centroidsHDFSFullPath])

def classify(mapperExecPath,reducerExecPath,inputDir,outputDir,centroidsFile,testImgsJsonFile,N):
	centroidsHDFSFullPath = HadoopUtils.getHDFSFullPath(centroidsFile)
        centroidsFileName = centroidsFile.split("/")[-1]
	testImgsJsonHDFSFullPath = HadoopUtils.getHDFSFullPath(testImgsJsonFile)
	testImgsJsonFileName = testImgsJsonFile.split("/")[-1]
        mapperCommandStr = HadoopUtils.buildExecCommandStr([mapperExecPath,centroidsFileName,testImgsJsonFileName,str(N)])
	reducerCommandStr = HadoopUtils.buildExecCommandStr([reducerExecPath,str(N)])

        HadoopUtils.removeHDFSDirIfExists(outputDir)

        HadoopUtils.runHadoopStreamingJob(input=inputDir,
                                  output=outputDir,
                                  mapperCommand=mapperCommandStr,
                                  reducerCommand=reducerCommandStr,
                                  filesArray=[centroidsHDFSFullPath,testImgsJsonHDFSFullPath])

def readConfigs(configFilePath):
	with open(configFilePath) as config_file:    
		global configs
		configs = json.load(config_file)

if (len(sys.argv) < MIN_NUM_ARGS):
	print "Wrong Number of Arguments:",len(sys.argv)
	printUsage()
	exit(1)

testImagesJsonFilePath = sys.argv[1]
confFilePath = sys.argv[2]
outputFilePath = sys.argv[3]

readConfigs(confFilePath)

hdfsInputFolder = configs[HDFS_BASE_FOLDER_TAG] + "/" + INPUT_FOLDER_NAME
HadoopUtils.removeHDFSDirIfExists(hdfsInputFolder)
HadoopUtils.mkdir(hdfsInputFolder)

storeInputIntoHDFS(testImagesJsonFilePath,hdfsInputFolder)

#Preprocess Images and store in on HDFS
testHistsFolder = configs[HDFS_BASE_FOLDER_TAG] + "/" + TEST_HISTS_FOLDER_NAME
preprocessInput(configs[PREPROCESSOR_MAPPER_EXEC_PATH],configs[PREPROCESSOR_REDUCER_EXEC_PATH],hdfsInputFolder,testHistsFolder,configs[CENTROIDS_HDFS_PATH])

#Run Image Retrieval Processing
trainHistsFolder = configs[HDFS_BASE_FOLDER_TAG] + "/" + TRAIN_HISTS_FOLDER_NAME
resultsFolder = configs[HDFS_BASE_FOLDER_TAG] + "/" + RESULTS_FOLDER_NAME
classify(configs[CLASSIFIER_MAPPER_EXEC_PATH],configs[CLASSIFIER_REDUCER_EXEC_PATH],trainHistsFolder,resultsFolder,configs[CENTROIDS_HDFS_PATH],testHistsFolder+"/part-00000",configs[N_TAG])

#Downloading Result from HDFS
HadoopUtils.getAndMergeFilesFromHDFS(resultsFolder + "/part-*",outputFilePath)

