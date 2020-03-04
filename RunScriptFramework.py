#-*-coding:utf-8-*-
# 兼容python2和python3
# 跑脚本的框架
# 不同时段QPS控制，多线程，多种任务分发模式，日志记录进度

# TODO:
# 支持多進程
# freq通過配置文件熱更新
# taskAssigner可以一次分配多個task
# 非法參數檢測報錯

from __future__ import division

try:
    # from urllib.parse import urlparse, urlencode
    from urllib.request import urlopen #, Request
    # from urllib.error import HTTPError
except ImportError:
    # from urlparse import urlparse
    # from urllib import urlencode
    from urllib2 import urlopen #, Request, HTTPError

import threading
import time
import logging
import logging.handlers
import os
import random

DEFAULT_STATISTIC_SLEEP_INTERVAL = 3 # second
RSF_DEFAULT_LOG_FILE_NAME = 'script.log'

def tryURLRequestManyTimes(url, retry_times, timeout_s):
    '''成功返回响应内容，否则返回-1
    timeout: 单次请求超时时间，单位：毫秒'''
    for retry_times in range(0, retry_times):
        success_flag = False
        try:
            response = urlopen(url, timeout=timeout_s)
            content = response.read()
            content = content.decode('utf-8')
            response.close()
            success_flag = True
            break
        except Exception :
            continue
    if success_flag == False:
        return -1
    return content

class HostSelector:
    '''处理URL请求任务时，用于多个主机、地域负载均衡
    线程不安全'''
    def __init__(self, hostList, hostWeight):
        '''hostList: IP或者域名列表
        hostWeight: 各个IP或者域名的权重，权重相同时传入空列表'''
        if len(hostList) == 0:
            raise ValueError('hostList is empty!')
        if len(hostWeight) != 0 \
        and len(hostList) != len(hostWeight):
            raise ValueError('length of hostList and hostWeight not match!')
        self.hostWeight = hostWeight
        self.hostList = []
        if len(hostWeight) != 0:
            for i in range(0, len(hostList)):
                host = hostList[i]
                for j in range(0, hostWeight[i]):
                    self.hostList.append(host)
            random.shuffle(self.hostList)
            random.shuffle(self.hostList)
        else:
            self.hostList = hostList
        self.hostIndex = 0
    def selectAHost(self):
        '''这个方法线程不安全'''
        self.hostIndex = (self.hostIndex + 1) % len(self.hostList)
        return self.hostList[self.hostIndex]

class RSFTaskFreqControllor:
    '''任务分配频率控制，线程不安全'''
    def __init__(self, mode, qps, averageIntervalSleepRate=0.86, slotCount=200):
        '''mode: 'free' / fixed' / 'hour', free表示不控制task的QPS，fix表示设置固定的QPS，hour表示设置一天内各个小时的QPS
        averageIntervalSleepRate: 当前请求和上一次请求的时间间隔为平均请求间隔的多少倍，取值0~1之间 ，0表示不控制'''
        if averageIntervalSleepRate >= 1 or averageIntervalSleepRate < 0:
            raise ValueError('averageIntervalSleepRate out of range!')
        self.freeFlag = False
        self.fixedFlag = False
        self.timeSection = 0 # ms 令牌槽覆盖的时间范围
        self.qpsForEachHour = []
        self.lastQps = 0 # 上一次设定的qps
        self.slot = [] # 令牌槽，循环数组
        self.slotHead = 0
        self.slotTail = 0 # 下一个未分配的slot
        self.slotCapacity = slotCount
        self.averageInterval = 0 # ms 根据qps得到的请求之间的平均间隔
        self.averageIntervalSleepRate = averageIntervalSleepRate
        self.averageIntervalTimesRate = 0 # averageInterval和averageIntervalSleepRate的乘机
        # 扩充槽
        for i in range(0, self.slotCapacity):
            self.slot.append(0)
        if mode == 'free':
            self.freeFlag = True
        elif mode == 'fixed':
            self.fixedFlag = True
            self.__setCurrentMaxQPS(qps)
        elif mode == 'hour':
            if type(qps) != list or len(qps) != 24:
                raise ValueError('type or length of value is not correct!')
            self.qpsForEachHour = qps
        else:
            raise ValueError('mode not exist!')
    def __setCurrentMaxQPS(self, qps):
        # 把QPS转换成timeSection秒内slotCount次
        self.timeSection = self.slotCapacity / qps
        self.averageInterval = 1 / qps
        self.averageIntervalTimesRate = self.averageInterval * self.averageIntervalSleepRate
    def __tryAcquireSlot(self, currentTime):
        '''成功返回True，否则返回False'''
        headTime = currentTime - self.timeSection
        slotSize = (self.slotTail + self.slotCapacity - self.slotHead) % self.slotCapacity
        # 空槽则直接分配一个令牌
        if slotSize == 0:
            self.slot[self.slotTail] = currentTime
            self.slotTail = (self.slotTail + 1) % self.slotCapacity
            return True
        # 从令牌槽的头部开始，去掉10秒以为的内容
        newIndex = self.slotHead
        foundFlag = False
        for i in range(self.slotHead, slotSize):
            if self.slot[newIndex] >= headTime:
                self.slotHead = newIndex
                foundFlag = True
                break
            newIndex = (newIndex + 1) % self.slotCapacity
        if foundFlag == False:
            # 槽内的令牌全部过期，则清空令牌槽
            self.slotHead = 0
            self.slotTail = 0
            newIndex = 0
        # 重新计算去掉过期令牌以后的槽大小
        slotSize = (self.slotTail + self.slotCapacity - self.slotHead) % self.slotCapacity
        if slotSize >= self.slotCapacity - 1:
            # 保留一个槽位不使用，避免无法分清槽位全满和全空的情况
            # 令牌槽已满，分配失败
            return False
        # 对比上一个槽的时间
        # 小于averageIntervalSleepRate的averageInterval时，需要等待
        if self.averageIntervalSleepRate != 0:
            lastSlotIndex = (self.slotTail + self.slotCapacity - 1) % self.slotCapacity
            lastSlotTime = self.slot[lastSlotIndex]
            expectedTime = lastSlotTime + self.averageIntervalTimesRate
            if currentTime < expectedTime:
                time.sleep(expectedTime - currentTime)
                currentTime = time.time()
        # 分配一个新的令牌
        self.slot[self.slotTail] = currentTime
        self.slotTail = (self.slotTail + 1) % self.slotCapacity
        return True
    def control(self):
        '''这是一个可能阻塞的方法，根据设定的QPS进行控制'''
        if self.freeFlag == True:
            return
        while True:
            currentTime = time.time()
            if self.fixedFlag == False:
                hour = time.localtime(currentTime).tm_hour
                qps = self.qpsForEachHour[hour]
                if qps != self.lastQps:
                    self.lastQps = qps
                    self.__setCurrentMaxQPS(qps)
            ret = self.__tryAcquireSlot(currentTime)
            if ret == False:
                time.sleep(self.averageIntervalTimesRate)
            else:
                return

class RSFTaskAssigner:
    '''【基类】任务分配器，用于为多个工作线程分配任务
    全局只有一个实例，需要考虑多线程安全的问题'''
    def __init__(self, **kwargs):
        self.lock = threading.Lock()
    def getTask(self):
        '''返回一个task，如果任务全部完成，则返回None'''
        raise NotImplementedError()
    def getProgress(self):
        '''返回一个字典，内容为{progress, description}
        progress为当前进度浮点型百分比,任务结束时必须返回>=100
        description的内容可选'''
        raise NotImplementedError()
    def destroy(self):
        raise NotImplementedError()
    def getAllTaskDescription(self):
        '''返回總任務的描述的字符串，例如ID的範圍'''
        raise NotImplementedError()

class FileTaskAssigner(RSFTaskAssigner):
    def __init__(self, fileName, freqControllor=None):
        RSFTaskAssigner.__init__(self)
        self.fileName = fileName
        self.file = open(fileName)
        self.file.seek(0, os.SEEK_END)
        self.fileSize = self.file.tell()
        self.file.seek(0, os.SEEK_SET)
        self.freqControllor = freqControllor
        self.curTaskMeta = None
    def getTask(self):
        self.lock.acquire()
        if self.freqControllor != None:
            self.freqControllor.control()
        line = self.file.readline()
        self.lock.release()
        if line == '':
            self.curTaskMeta = None
            return None
        line = line.strip()
        self.curTaskMeta = line
        return line
    def getProgress(self):
        self.lock.acquire()
        num = self.file.tell()
        self.lock.release()
        progress = num / self.fileSize * 100
        ret = {}
        ret['progress'] = progress
        ret['description'] = 'current line:' + str(self.curTaskMeta)
        return ret
    def destroy(self):
        self.file.close()
    def getAllTaskDescription(self):
        return 'file_name: ' + self.fileName


class ContinuesIntegerTaskAssigner(RSFTaskAssigner):
    '''整数型任务分配器，适用于单个整数型变量，遍历某一个范围，不同任务的的变量的值是等差数列的情景'''
    def __init__(self, begin, end, step, range_only, freqControllor=None):
        '''begin: 变量的起始值（包含）
        end: 变量的结束值（包含）
        step: 步长
        range_only: step不为1时，是否只返回起始值。range_only=False返回起始值内的所有值'''
        RSFTaskAssigner.__init__(self)
        self.begin = int(begin)
        self.end = int(end)
        self.step = int(step)
        self.cur = self.begin
        self.range_only = range_only
        self.freqControllor = freqControllor
        if self.begin > self.end:
            raise ValueError('begin should not beigger than end!')
    def getTask(self):
        '''返回一个列表，任务完成时返回None'''
        self.lock.acquire()
        if self.freqControllor != None:
            self.freqControllor.control()
        if self.cur > self.end:
            value = None
        else:
            value = self.cur
            self.cur += self.step
        self.lock.release()
        if value == None:
            return None
        ret = []
        endRange = value + self.step
        if endRange > self.end + 1:
            endRange = self.end + 1
        if self.range_only == True:
            return [value, endRange - 1]
        for i in range(value, endRange):
            ret.append(i)
        return ret
    def getProgress(self):
        self.lock.acquire()
        value = self.cur
        self.lock.release()
        progress =  (value - self.begin) / (self.end - self.begin) * 100
        print('value:',value, 'progress', progress, 'begin', self.begin, 'end', self.end)
        return {'progress':progress, 'description':'current:%d'%(value)}
    def destroy(self):
        pass
    def getAllTaskDescription(self):
        return 'range: %d ~ %d step: %d'%(self.begin, self.end, self.step)

class RSFTaskProcessor:
    '''【基类】处理任务的逻辑
    每个WorkerThread都有一个这个类的实例'''
    def __init__(self, logger):
        '''子类的构造函数参数必须和父类的一致'''
        self.logger = logger
    def process(self, task):
        '''task: 由TaskAssigner分配的任务
        成功返回响应内容的列表，列表的元素為字符串，后续由worker暂存，最终写入输出文件。
        如果要換行，需要手動為列表的每個元素添加'\n'。
        无数据时返回空列表，失败返回-1'''
        raise NotImplementedError()

class SingleURLRequestTaskProcessor(RSFTaskProcessor):
    '''用于处理只有1次URL请求的Task
    URL的格式必须是baseURL + str(task)'''
    def __init__(self, logger, baseURL, timeout_s, retry_times=3):
        RSFTaskProcessor.__init__(self, logger)
        self.baseURL = baseURL
        self.timeout_s = timeout_s
        self.retry_times = retry_times
    def process(self, task):
        '''成功返回响应内容，否则返回-1'''
        url = self.baseURL + str(task)
        ret = tryURLRequestManyTimes(url, self.retry_times, self.timeout_s)
        return ret

class WorkerThread(threading.Thread):
    def __init__(self, logger, failLogger, taskAssigner, typeOfTaskProcessor):
        '''outputFlag: 是否需要输出处理后的内容'''
        threading.Thread.__init__(self)
        self.taskAssigner = taskAssigner
        self.logger = logger
        self.failLogger = failLogger
        self.taskProcessor = typeOfTaskProcessor(logger)
        self.outputData = []
        self.outputDataLock = threading.Lock()
    def run(self):
        '''从TaskAssigner中获取任务并执行
        直到TaskAssigner返回None'''
        self.logger.info('worker start: ' + self.name);
        while True:
            task = self.taskAssigner.getTask()
            if task == None:
                break
            ret = self.taskProcessor.process(task)
            if ret == -1:
                self.logger.warning('Worker %s process task %s failed.'%(self.name, str(task)))
                self.failLogger.error(str(task))
            elif ret == []:
                continue
            else:
                self.__addOutputData(ret)
        self.logger.info('worker is going to exist: ' + self.name);
    def __addOutputData(self, data):
        self.outputDataLock.acquire()
        self.outputData.extend(data)
        self.outputDataLock.release()
    def getAndClearOutputData(self):
        '''返回一个列表，列表中的每个元素为每次请求对应的结果
        返回空列表表示无数据'''
        self.outputDataLock.acquire()
        ret = self.outputData
        self.outputData = []
        self.outputDataLock.release()
        return ret

class StatisticThread(threading.Thread):
    def __init__(self, logger, taskAssigner, sleepInterval=DEFAULT_STATISTIC_SLEEP_INTERVAL):
        threading.Thread.__init__(self)
        self.logger = logger
        self.taskAssigner = taskAssigner
        if sleepInterval <= 0:
            sleepInterval = DEFAULT_STATISTIC_SLEEP_INTERVAL
        self.sleepInterval = sleepInterval
    def run(self):
        self.logger.info('StatisticThread start')
        while True:
            progress = self.taskAssigner.getProgress()
            description = str(progress['description'])
            progress = progress['progress']
            self.logger.info('total progress: %.2f%% %s'%(progress, description))
            if progress >= 100.0:
                break
            time.sleep(self.sleepInterval)
        self.logger.info('StatisticThread exit')

class OutputThread(threading.Thread):
    def __init__(self, outputFileName, workerThreadList, logger, getDataInterval=3):
        threading.Thread.__init__(self)
        self.outputFileName = outputFileName
        self.getDataInterval = getDataInterval
        self.workerThreadList = workerThreadList
        self.logger = logger
    def run(self):
        # 所有workThread退出后才退出
        self.logger.info('OutputThread start.')
        while True:
            exitFlag = True # 所有WorkerThread退出后就结束
            outputData = []
            outputFile = open(self.outputFileName, 'a')
            for t in self.workerThreadList:
                if t.is_alive() == True:
                    exitFlag = False
                result = t.getAndClearOutputData()
                if len(result) == 0:
                    continue
                else:
                    outputData.extend(result)
            for data in outputData:
                outputFile.write(data)
            outputFile.close()
            if exitFlag == True:
                break
            time.sleep(self.getDataInterval)
        self.logger.info('OutputThread is going to exit.')

class RunScriptFramework:
    def __init__(self, threadCount, typeOfTaskProcessor, taskAssigner, **kwargs):
        '''threadCount: 线程数
        typeOfTaskProcessor: TaskProcessor的类型名称，不是实例
        taskAssigner: 一个TaskAssigner子类的实例
        logFileName: 日志文件名称，默认为script.log
        outputFileName: 结果输出文件的名称，默认为output.txt
        statisticPrintInterval: 进度日志打印间隔时间，单位秒'''
        # 處理參數
        self.taskAssigner = taskAssigner
        self.typeOfTaskProcessor = typeOfTaskProcessor
        self.threadList = []
        self.threadCount = int(threadCount) # 不包括statisticThread
        logFileName = None
        if 'logFileName' in kwargs:
            logFileName = kwargs['logFileName']
        if logFileName == None or logFileName == '':
            logFileName = RSF_DEFAULT_LOG_FILE_NAME
        outputFileName = 'output.txt'
        if 'outputFileName' in kwargs:
            outputFileName = kwargs['outputFileName']
        statisticPrintInterval = DEFAULT_STATISTIC_SLEEP_INTERVAL
        if 'statisticPrintInterval' in kwargs:
            statisticPrintInterval = int(kwargs['statisticPrintInterval'])

        # 創建多個logger
        logFormatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
        progressFormatter = logging.Formatter('%(asctime)s %(message)s')
        failedFormatter = logging.Formatter('%(message)s')
        logHandler = logging.FileHandler(logFileName)
        logHandler.setFormatter(logFormatter)
        progressHandler = logging.FileHandler('progress')
        progressHandler.setFormatter(progressFormatter)
        failedHandler = logging.FileHandler('failed_task')
        failedHandler.setFormatter(failedFormatter)
        self.frameLogger = logging.getLogger('FRAME')   # 跑脚本框架的日志
        self.frameLogger.addHandler(logHandler)
        self.frameLogger.setLevel(logging.INFO)
        self.workerLogger = logging.getLogger('WORKER') # worker线程的日志
        self.workerLogger.addHandler(logHandler)
        self.workerLogger.setLevel(logging.INFO)
        self.statisticLogger = logging.getLogger('STATISTIC')   # 统计线程的日志
        self.statisticLogger.addHandler(progressHandler)
        self.statisticLogger.setLevel(logging.INFO)
        self.failedLogger = logging.getLogger('FAILED') # 记录失败的任务
        self.failedLogger.addHandler(failedHandler)
        self.failedLogger.setLevel(logging.INFO)

        self.statisticThread = StatisticThread(self.statisticLogger, taskAssigner, statisticPrintInterval)
        self.outputThread = OutputThread(outputFileName, self.threadList, self.frameLogger)

    def start(self):
        '''开始执行，成功返回0，否则返回-1'''
        self.frameLogger.info('---------------- START ----------------')
        self.statisticLogger.info('---------------- START ----------------')
        self.frameLogger.info(self.taskAssigner.getAllTaskDescription())
        self.frameLogger.info('thread count: ' + str(self.threadCount))
        if self.taskAssigner == None:
            raise ValueError('RSFTaskAssigner is not set!')
        for worker_id in range(0, self.threadCount):
            t = WorkerThread(self.workerLogger, self.failedLogger, self.taskAssigner, self.typeOfTaskProcessor)
            t.name = 'worker_' + str(worker_id)
            self.threadList.append(t)
        for t in self.threadList:
            t.start()
        self.frameLogger.info('all worker_thread has start.')
        self.statisticThread.start()
        self.outputThread.start()
        self.frameLogger.info('all threads has started.')
        for t in self.threadList:
            t.join()
        self.frameLogger.info('all worker_thread has joined.')
        self.statisticThread.join()
        self.outputThread.join()
        self.frameLogger.info('all threads has joined.')
        self.frameLogger.info('exit.')
        return 0
