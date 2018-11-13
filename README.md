 
obscmd是一款基于Python开发的命令行工具，可用于上传、下载和管理存储在对象存储服务中的数据，具有简单、易用等优势。您可以使用新版obscmd对OBS中的桶和对象进行操作，例如上传、下载、删除等。对于熟悉命令行程序的用户，新版obscmd是执行批量处理、自动化任务的理想选择。


使用指南下载地址：https://obs-community.obs.cn-north-1.myhwclouds.com/obscmd/obscmd%E7%94%A8%E6%88%B7%E6%8C%87%E5%8D%97(5.0.0_beta).doc

本工具完全开源，您可以自行改造，并用于商业或个人行为而不受影响。

 
跨平台：Windows、Linux都可以运行。

操作方便：完全命令行的方式进行操作。

功能完善：拥有桶基本操作，对象基本操作，桶权限管理，生命周期管理等功能。

透明化：您可以看到完整的源码，我们稍后也会开源到Github。

 

支持文件和文件夹的上传、下载、删除等操作。

通过Multipart方式，对大文件进行分片上传或下载。

支持断点续传，支持增量上传。

 

工具运行环境需要依赖python2.7.x或python3.3以上。

若使用python2.7.x在windows系统上安装后不能成功运行，则尝试运行下面命令进行安装：

       python setup.py bdist_wininst

       安装完毕后，即可使用。

 

初始化配置文件：
       工具操作系统上第一次运行命令时会生成本地目录~/.obscmd，在操作系统系统会在当前用户目录下生成.obscmd目录。其中logs存放日志文件。checkpoint存放断点续传文件。工具的配置信息放在本地文件.obscmd/obscmd.ini中。

       运行以下命令，配置或修改本地obscmd配置文件中的access_key_id和secret_access_key值。运行该命令会弹出提示用户输入对应值。

       $ obscmd configure

       OBS Access Key ID [****************KL9C]: ssdfwer234dfg

       OBS Secret Access Key [****************2GHF]: 23o4uouweorsdf

       OBS Server [*****]:

       也可根据需要手动修改配置文件obscmd.ini。

       命令在运行过程中会读取本地用户目录下.obscmd/obscmd.ini和.obscmd/default.ini文件，其中default.ini为工具默认配置文件，不建议用户修改；工具会优先选择obscmd.ini中的配置参数。这些配置包括了客户端的权限信息AK&SK，日志配置、任务配置等信息。命令运行的日志保存在~/.obscmd/logs目录汇中。用户可根据情况修改obscmd.ini文件进行配置。

除了在配置文件中指定AK&SK，用户也可以在每条命令行中通过参数指定：

       $ obscmd obs ls --ak slafjwe23 --sk lkjdlf234wefa

 

命令结构：
       新版obscmd目前只实现了OBS服务相关命令，命令设计为两级命令。命令结构如下：

       $ obscmd <command> <subcommand> [options and parameters]

       $标识命令行提示符，不同操作系统不同用户权限可能会有差异。这种结构一开始是对obscmd的基本调用。下一部分command指定一个顶级命令，通常表示obscmd中支持的华为云服务。每个云服务均拥有指定要执行的操作的附加子命令subcommand。一个操作的常规命令选项或特定参数可在命令行中以任何顺序指定。如果多次指定某个排他参数，则仅应用最后一个值。

 

命令帮助：
       要在使用obscmd时获取帮助，您只需在命令末尾添加 help。例如，以下命令会列出常规 obscmd选项和可用顶层命令的帮助。

       每个命令的帮助分为五个部分：命令名称、命令描述、命令结构、命令选项、命令使用样例。

       $ obscmd help

       以下命令会列出 obs 顶级命令的相关帮助：

       $ obscmd obs help

       以下命令会列出 obs 的ls子命令相关帮助：

       $ obscmd obs ls help
