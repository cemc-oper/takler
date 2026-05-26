开始使用
==========

开始教程前需要配置 Takler 使用环境。

安装 Takler 环境
-----------------

本教程使用 Takler 项目的两个软件包：

* `takler <https://github.com/perillaroc/takler>`_：takler 核心项目 (Python)，用于创建工作流并运行 Takler 服务
* `takler-client <https://github.com/perillaroc/takler-client>`_：takler 命令行客户端 (Golang)，用于与 Takler 服务进行交互


CMA-HPC2023-SC3
^^^^^^^^^^^^^^^^

在 CMA-HPC2023-SC3 上可以使用 wangdp 账户预安装的 takler 环境。

.. code-block:: bash

    export MODULEPATH=/g1/u/wangdp/modules:$MODULEPATH
    module load wangdp/share/tool/takler/latest

如果在其他环境中，可以按照以下两节安装 takler 和 takler-client 两个软件包。

安装 Takler 软件包
^^^^^^^^^^^^^^^^^^^^^^

安装 `Python 环境 <https://www.python.org/downloads/>`_，下载最新代码并安装，建议为 takler 创建单独的 conda 环境。

.. code-block:: bash

    git clone https://github.com/perillaroc/takler
    cd takler
    pip install .

安装 Takler 客户端
^^^^^^^^^^^^^^^^^^^

安装 `Golang 环境 <https://go.dev/doc/install>`_，下载最新代码并编译：

.. code-block:: bash

    git clone https://github.com/perillaroc/takler-client
    cd takler-client
    make

编译后会在 **bin** 目录生成可执行程序 ``takler_client``。
将其拷贝到环境变量 ``PATH`` 可以访问到的目录中（例如 ``$HOME/bin``），或将其目录加入到 ``PATH`` 中。

.. note::

    在 CMA-HPC2023-SC3 等无法直接连接互联网的超算中，需要下载 takler 和 takler-client 的压缩包安装。

创建教程目录
----------------

为教程创建单独的目录，例如在 CMA-HPC2023-SC1 上创建目录。

.. code-block:: bash

    export TAKLER_HOME=/g6/wangdp/project/course/takler/tutorial
    mkdir -p ${TAKLER_HOME}
    cd ${TAKLER_HOME}


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