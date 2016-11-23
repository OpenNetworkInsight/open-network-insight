#!/bin/env python

import time
import os
import subprocess
import logging
import ConfigParser
from multiprocessing import Process
from common.utils import Util
from common.file_collector import FileWatcher
from multiprocessing import Pool
from common.kafka_client import KafkaTopic


class Collector(object):

    def __init__(self, hdfs_app_path, kafka_topic, conf_type):

        self._initialize_members(hdfs_app_path, kafka_topic, conf_type)

    def _initialize_members(self, hdfs_app_path, kafka_topic, conf_type):

        # getting parameters.
        self._logger = logging.getLogger('SPOT.INGEST.DNS')
        self._hdfs_app_path = hdfs_app_path
        self._kafka_topic = kafka_topic

        # get script path
        self._script_path = os.path.dirname(os.path.abspath(__file__))

        # read dns configuration.
        conf_file = "/etc/spot.conf"
        self._conf = ConfigParser.SafeConfigParser()
        self._conf.read(conf_file)

        # set configuration.
        self._collector_path = self._conf.get(conf_type, 'collector_path')
        self._dsource = 'dns'
        self._hdfs_root_path = "{0}/{1}".format(hdfs_app_path, self._dsource)

        # set configuration.
        self._pkt_num = self._conf.get(conf_type, 'pkt_num')
        self._pcap_split_staging = self._conf.get(conf_type, 'pcap_split_staging')
        self._supported_files = self._conf.get(conf_type, 'supported_files')

        # create collector watcher
        self._watcher = FileWatcher(self._collector_path, self._supported_files)

        # Multiprocessing.
        self._processes = self._conf.get(conf_type, 'collector_processes')
        self._ingestion_interval = self._conf('ingest', 'ingestion_interval')
        self._pool = Pool(processes=self._processes)

    def start(self):

        self._logger.info("Starting DNS ingest")
        self._watcher.start()

        try:
            while True:
                self._ingest_files_pool()
                time.sleep(self._ingestion_interval)

        except KeyboardInterrupt:
            self._logger.info("Stopping DNS collector...")
            Util.remove_kafka_topic(self._kafka_topic.Zookeeper,self._kafka_topic.Topic,self._logger)
            self._watcher.stop()
            self._pool.terminate()
            self._pool.close()
            self._pool.join()
            SystemExit("Ingest finished...")


    def _ingest_files_pool(self):

        if self._watcher.HasFiles:

            for x in range(0,self._processes):
                file = self._watcher.GetNextFile()
                resutl = self._pool.apply_async(ingest_file,args=(file,self._pkt_num,self._pcap_split_staging,self._kafka_topic.Partition,self._hdfs_root_path ,self._kafka_topic.Topic,self._kafka_topic.BootstrapServers,))
                #resutl.get() # to debug add try and catch.
                if  not self._watcher.HasFiles: break
        return True


def ingest_file(file,pkt_num,pcap_split_staging, partition,hdfs_root_path,topic,kafka_servers):

    logger = logging.getLogger('SPOT.INGEST.FLOW.{0}'.format(os.getpid()))

    try:
        # get file name and date.
        org_file = file
        file_name_parts = file.split('/')
        file_name = file_name_parts[len(file_name_parts)-1]

        # split file.
        name = file_name.split('.')[0]
        split_cmd="editcap -c {0} {1} {2}/{3}_spot.pcap".format(pkt_num,file,pcap_split_staging,name)
        logger.info("Splitting file: {0}".format(split_cmd))
        Util.execute_cmd(split_cmd,logger)

        for currdir,subdir,files in os.walk(pcap_split_staging):
            for file in files:
                if file.endswith(".pcap") and "{0}_spot".format(name) in file:

                        # get timestamp from the file name to build hdfs path.
                        file_date = file.split('.')[0]
                        pcap_hour = file_date[-6:-4]
                        pcap_date_path = file_date[-14:-6]

                        # hdfs path with timestamp.
                        hdfs_path = "{0}/binary/{1}/{2}".format(hdfs_root_path, pcap_date_path, pcap_hour)

                        # create hdfs path.
                        Util.creat_hdfs_folder(hdfs_path, logger)

                        # load file to hdfs.
                        hadoop_pcap_file = "{0}/{1}".format(hdfs_path, file)
                        Util.load_to_hdfs(os.path.join(currdir, file), hadoop_pcap_file, logger)

                        # create event for workers to process the file.
                        logger.info( "Sending split file to worker number: {0}".format(partition))
                        KafkaTopic.SendMessage(hadoop_pcap_file, kafka_servers, topic, partition)
                        logger.info("File {0} has been successfully sent to Kafka Topic to: {1}".format(file, topic))


        logger.info("Removing file: {0}".format(org_file))
        rm_big_file = "rm {0}".format(org_file)
        Util.execute_cmd(rm_big_file, logger)

    except Exception as err:

        logger.error("There was a problem, please check the following error message:{0}".format(err.message))
        logger.error("Exception: {0}".format(err))
