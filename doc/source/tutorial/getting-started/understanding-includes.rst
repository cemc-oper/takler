理解头文件
============

前面章节中创建一个 Shell 脚本类型 (:py:class:`~takler.tasks.shell.ShellScriptTask`) 的 task。

每个 :py:class:`~takler.tasks.shell.ShellScriptTask` 都有对应的 takler 脚本 (takler script)，定义需要执行哪些操作，脚本类似于 UNIX shell 脚本。

但 takler 脚本使用 `Jinja2 <https://jinja.palletsprojects.com/>`_ 提供的模板指令和预定义变量。

工作流定义中的变量 (:py:class:`~takler.core.Parameter`) 可以在 takler 脚本中使用，提供一种配置机制。

变量指令是 ``{{ some_var }}``：

.. code-block::

    {{ TAKLER_HOST }}

头文件引用指令是 ``{% include "some file" %}``

.. code-block::

    {% include "head.takler" %}

head.takler
------------------

head.takler 头文件放在 takler 脚本的开头，用于：

* 提供与 Takler 服务通讯的环境
* 定义脚本错误处理，当脚本出错捕获 trap 信号时，通知服务该任务 abort
* 使用 child 命令 (child command) ``init`` 通知服务该任务已经开始

应用在 CMA-PI 上的示例 head.takler

.. code-block:: bash
    :linenos:

    #!/bin/bash

    set -e          # stop the shell on first error
    set -u          # fail when using an undefined variable
    set -x          # echo script lines as they are executed
    # set -o pipefail # fail if last(rightmost) command exits with a non-zero status

    date

    # Defines the variables that are needed for any
    # communication with the takler server

    export TAKLER_HOST={{ TAKLER_HOST }}
    export TAKLER_PORT={{ TAKLER_PORT }}
    export TAKLER_NAME={{ TAKLER_NAME }}
    export TAKLER_PASS={{ TAKLER_PASS }}

    # for ksh
    . /etc/profile.d/modules.sh

    RID=$( echo ${SLURM_JOB_ID:-0} )
    if [ $RID -eq 0 ] ; then
      RID=$$
    fi
    export TAKLER_RID=$RID

    export PATH=~/bin:$PATH

    takler_client init --host ${TAKLER_HOST} --port ${TAKLER_PORT} \
      --task-id ${TAKLER_RID} --node-path ${TAKLER_NAME}

    # Define a error handler
    ERROR() {
       set +e                      # Clear -e flag, so we don't fail
       wait                        # wait for background process to stop
       takler_client abort --host ${TAKLER_HOST} --port ${TAKLER_PORT} \
          --node-path ${TAKLER_NAME}
       trap 0                      # Remove the trap
       exit 0                      # End the script
    }
    # Trap any calls to exit and errors caught by the -e flag

    trap ERROR 0

    # Trap any signal that may cause the script to fail

    trap '{ echo "Killed by a signal";trap 0;ERROR; }' 1 2 3 4 5 6 7 8 10 12 13 15
    echo "exec on hostname:" "$(hostname)"

上面示例中的 ``TAKLER_PASS`` 是 takler 为每个 task 生成的作业一次性口令，
在服务启用鉴权模式 (auth mode) 后，child 命令需要携带该口令才能被服务接受。
``takler_client`` 从环境变量 ``TAKLER_PASS`` 中读取它，因此头文件中需要把该变量导出。

.. note::

    takler 不提供 ``head.takler``，该文件由使用方自行编写和维护。
    在启用鉴权模式之前，请先在自己的 ``head.takler`` 中加入
    ``export TAKLER_PASS={{TAKLER_PASS}}`` 一行，
    否则该 task 的 child 命令会被服务拒绝。


tail.takler
-------------

tail.takler 放在 takler 脚本文件的结尾，通知服务该任务已经完成。
使用 child command 中的 ``complete`` 命令。

.. code-block:: bash

    date
    wait                      # wait for background process to stop
    takler_client complete --host ${TAKLER_HOST} --port ${TAKLER_PORT} \
          --node-path ${TAKLER_NAME}
    trap 0                    # Remove all traps
    exit 0                    # End the shell


这两个头文件主要用于与 takler 服务通讯：

* 建立通讯环境
* 在任务开始时通知服务任务已经开始
* 在任务结束时通知服务任务已经完成
* 在任务出错时通知服务任务发生错误

练习
-----

1. 在 ``${TAKLER_HOME}/test`` 目录中创建头文件 `head.takler` 和 `tail.takler`