# RunScriptFramework python跑脚本框架

## 一、简介
这是一个python语言的跑脚本的框架，方便快速开发脚本。特点如下：
 
 - 多线程控制，线程数可以配置。
 - 日志管理，业务日志、脚本框架日志分开保存，并在日志中保存当前进度。
 - 输出管理，多线程的所有输出自动汇聚在一个文件中。
 - QPS控制，可以针对不同时间段，灵活设置QPS。
 - 多Host负载均衡，可以发起多个机房的请求，提升新能。
 - 兼容python2和python3

## 二、快速开始
RunScriptFramework的主要执行逻辑是，每个线程从`TaskAssigner`中获取任务，由`TaskProcessor`来执行任务，返回结果，由框架把结果保存到文件中。简单的场景下，可以选择一个框架中现有的`TaskAssigner`，只需继承`RSFTaskProcessor`类，在`run`方法中实现业务逻辑即可。

### 2.1 例子：通过url删除文件中key在数据库中的内容
假设有一个接口`/api/deleteAttr?key=xxx`可以删除KV数据库中指定的key。现在需要删除一批key，要删除的key保存在key.txt文件中，每个key占一行。代码如下：

``` python
from RunScriptFramework import *

# 继承RSFTaskProcessor，在run方法中写业务逻辑
class TaskProcessor(RSFTaskProcessor):
    def __init__(self, logger):
        RSFTaskProcessor.__init__(self, logger)
    def run(self, task):
        key = task[0] # task由FileTaskAssigner分配，类型为list，只包含文件中的一行
        url = 'http://www.example.com/api/deleteAttr?key=' + key
        ret = tryURLRequestManyTimes(url, 3, 3) # 超时时间为3秒，最多重试3次
        if ret == -1:
            return -1   # 失败的请求会自动记录在日志中
        return []       # 返回list类型表示成功

taskAssigner = FileTaskAssigner('key.txt')
framework = RunScriptFramework(10, TaskProcessor, taskAssigner)
framework.start()
```
程序开始执行后，会生成`script.log`，定时记录整体进度、当前执行的task和失败的task。

### 2.2 例子：通过url获取某一范围uid对应的用户名
假设有一个接口`/api/getUserInfo?uid=xxx`可以通过用户id来获取用户信息，返回一个json字符串。现在需要获取用户id在10000 ~ 20000范围内对应的用户名，并保存在文件中。并且要求按照时间段控制qps，同时请求多个机房。代码如下：

``` python
from RunScriptFramework import *
import json

# host列表与对应的权重
host_list = ['idc_1.example.com', 'idc_2.example.com', 'idc_3.expamle.com']
host_weight = [20, 25, 30]

# 每个小时的qps限制
qps_each_hour = [\
        800, 1200, 3000, 3000, 3000, 3000, 3000, 3000, 2400, 2000, \
        1800, 1800, 1800, 1800, 1800, 1800, 1800, 1800, 1500, 1200, \
        800, 800, 800, 800]

class TaskProcessor(RSFTaskProcessor):
    def __init__(self, logger):
        RSFTaskProcessor.__init__(self, logger)
        self.hostSelector = HostSelector(host_list, host_weight)
    def run(self, task):
        uid = task[0]
        # 根据权重选择一个host生成url
        url = 'http://' + self.hostSelector.selectAHost() + '/api/getUserInfo?uid=' + uid
        ret = tryURLRequestManyTimes(url, 3, 3)
        if ret == -1:
            return -1
        user_name = json.loads(ret)['user_name']
        return [str(uid) + ' ' + user_name + '\n'] # 在输出文件中保存为一行


freqControllor = RSFTaskFreqControllor('hour', qps_each_hour)
taskAssigner = ContinuesIntegerTaskAssigner(10000, 20000, 1, True, freqControllor)
framework = RunScriptFramework(10, TaskProcessor, taskAssigner, outputFileName='uid_uname.txt')
framework.start()
```

## 三、API
### class RunScriptFramework

    RunScriptFramework(threadCount, typeOfTaskProcessor, taskAssigner [, logFileName] [, outputFileName] [,statisticPrintInterval])

    threadCount: 线程数
    typeOfTaskProcessor: TaskProcessor的类型名称，不是实例
    taskAssigner: 一个TaskAssigner子类的实例
    logFileName: 日志文件名称，默认为script.log
    outputFileName: 结果输出文件的名称，默认为output.txt
    statisticPrintInterval: 进度日志打印间隔时间，单位秒，默认3秒

### RunScriptFramework.start()

    RunScriptFramework.start()
    开始执行脚本。结束时返回0。

## 四、实现
### 4.1 RunScriptFramework结构

    __main__
    |-- QPSControllor
    |-- TaskAssigner
    |-- RunScriptFramework
        |-- WorkerThread x N
        |   |-- TaskProcessor
        |   |   |-- HostSelector
        |-- StatisticThread
        |-- OutputThread
