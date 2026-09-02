开始使用
==========

开始教程前需要配置 Takler 使用环境。本节以一台普通 Linux 服务器为例，
不依赖任何特定 HPC 系统或账户。如果你需要在 HPC 环境（如通过 ``module`` 加载预安装环境、
离线安装）中使用 Takler，请参考 :doc:`/tutorial/hpc-appendix`。

安装 Takler 环境
-----------------

本教程使用 Takler 项目的两个软件包：

* `takler <https://github.com/cemc-oper/takler>`_：takler 核心项目 (Python)，用于创建工作流并运行 Takler 服务
* `takler-client <https://github.com/cemc-oper/takler-client>`_：takler 命令行客户端 (Golang)，用于与 Takler 服务进行交互

本教程的客户端命令同时给出两种写法：Go 客户端 ``takler_client`` 与 Python 客户端
``takler-client-py`` （随 takler 包一起安装，见下文），两者二选一即可。

安装 Takler 软件包
^^^^^^^^^^^^^^^^^^^^^^

安装 `Python 环境 <https://www.python.org/downloads/>`_ (要求 3.11 及以上版本)，
下载最新代码并安装，建议为 takler 创建单独的虚拟环境或 conda 环境。

.. code-block:: bash

    git clone https://github.com/cemc-oper/takler
    cd takler
    pip install .

安装完成后即可使用 Python 客户端 ``takler-client-py``。

安装 Takler 客户端（可选，Go 版本）
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

如果希望使用 Go 版本的客户端 ``takler_client``，需要安装
`Golang 环境 <https://go.dev/doc/install>`_，下载最新代码并编译：

.. code-block:: bash

    git clone https://github.com/cemc-oper/takler-client
    cd takler-client
    make

编译后会在 **bin** 目录生成可执行程序 ``takler_client``。
将其拷贝到环境变量 ``PATH`` 可以访问到的目录中（例如 ``$HOME/bin``），或将其目录加入到 ``PATH`` 中。

创建教程目录
----------------

为教程创建单独的目录：

.. code-block:: bash

    export TAKLER_HOME=$HOME/takler-tutorial
    mkdir -p ${TAKLER_HOME}
    cd ${TAKLER_HOME}

后续各节均假定当前目录为 ``${TAKLER_HOME}``，且该环境变量在当前 shell 会话中保持设置。


.. toctree::
   :hidden:
   :maxdepth: 2

   define-a-new-flow
   understanding-includes
   define-the-first-task
   checking-job-creation
   checking-the-job
   starting-a-server
   understanding-the-client
   starting-the-flow
   checking-the-results
