#!/bin/env python
import os
import logging
import ConfigParser

from common.utils import Util


class Worker(object):

    def __init__(self,db_name,hdfs_app_path,kafka_consumer,conf_type,processes):

        self._initialize_members(db_name,hdfs_app_path,kafka_consumer,conf_type,processes)

    def _initialize_members(self,db_name,hdfs_app_path,kafka_consumer,conf_type,processes):

        # get logger instance.
        self._logger = Util.get_logger('SPOT.INGEST.WRK.PROXY')

        self._db_name = db_name
        self._hdfs_app_path = hdfs_app_path
        self._kafka_consumer = kafka_consumer

        # read proxy configuration.
        self._script_path = os.path.dirname(os.path.abspath(__file__))
        conf_file = "/etc/spot.conf"
        self._conf = ConfigParser.SafeConfigParser()
        self._conf.read(conf_file)
        self._processes = processes

    def start(self):

        self._logger.info("Creating Spark Job for topic: {0}".format(self._kafka_consumer.Topic))

        # parser
        parser = self._conf.get('proxy', "parser")

        # spark conf
        diver_memory = self._conf.get('ingest', "driver_memory")
        num_exec = self._conf.get('ingest', "spark_exec")
        exec_memory = self._conf.get('ingest', "spark_executor_memory")
        exec_cores = self._conf.get('ingest', "spark_executor_cores")
        batch_size = self._conf.get('ingest', "spark_batch_size")

        jar_path = os.path.dirname(os.path.dirname(self._script_path))
        # spark job command.
        spark_job_cmd = ("spark-submit --master yarn "
                         "--driver-memory {0} "
                         "--num-executors {1} "
                         "--conf spark.executor.memory={2} "
                         "--conf spark.executor.cores={3} "
                         "--jars {4}/common/spark-streaming-kafka-0-8-assembly_2.11-2.0.0.jar "
                         "{5}/{6} "
                         "-zk {7} "
                         "-t {8} "
                         "-db {9} "
                         "-dt {10} "
                         "-w {11} "
                         "-bs {12}".format(diver_memory,num_exec,exec_memory,exec_cores,jar_path,self._script_path,parser,self._kafka_consumer.ZookeperServer,self._kafka_consumer.Topic,self._db_name,"proxy",self._processes,batch_size))

        # start spark job.
        Util.execute_cmd(spark_job_cmd,self._logger)
