任务脚本
========

每个 :py:class:`~takler.tasks.shell.ShellScriptTask` 对应一个
**takler 脚本**（约定扩展名 ``.takler`` ），它是作业的真正内容。
任务每次运行时，脚本被渲染成一个可直接执行的 **作业文件** 再提交
运行；渲染与提交流程见 :doc:`/guide/job-management` ，本页关注
脚本本身的写法： Jinja2 渲染模型、 include 搜索路径、 head / tail
约定，以及脚本与 takler 服务之间的通讯约定。入门介绍见
:doc:`/tutorial/getting-started/define-the-first-task` 与
:doc:`/tutorial/getting-started/understanding-includes` 。

渲染模型
--------

takler 脚本使用 `Jinja2 <https://jinja.palletsprojects.com/>`_ 模板
语法（不是 ecFlow 的 ``%VAR%`` ）。渲染由 ``ShellRender`` 完成：
以脚本文件为模板，用任务的变量合并视图作为上下文渲染一次，结果
写入作业文件。

渲染上下文是
:py:meth:`Node.parameters <takler.core.node.Node.parameters>`
返回的合并视图：节点自己、各级祖先直到 bunch 的全部变量，同名时
近处优先、 user 参数优先于 generated 参数（详见
:doc:`/guide/variables` ）。脚本中用 ``{{ 变量名 }}`` 引用：

.. code-block:: bash

    echo "processing date {{ TAKLER_DATE }}"
    echo "task {{ TAKLER_NAME }} attempt {{ TAKLER_TRY_NO }}"

除变量替换外， Jinja2 的全套模板指令都可用——条件
``{% if %}`` 、循环 ``{% for %}`` 、注释 ``{# ... #}`` 等，
例如按变量裁剪脚本内容：

.. code-block:: bash

    {% if DEBUG %}set -x{% endif %}

.. warning::

    引用**未定义**的变量不会报错： Jinja2 默认把它渲染为空字符串，
    ``check_job_creation`` 也检查不出来（见 :doc:`/guide/variables`
    的同名警告）。反之，脚本文件缺失、 include 无法解析、模板语法
    错误都会使渲染失败（见下文「渲染失败」）。

include 与搜索路径
------------------

把各任务脚本共用的片段（通讯环境、错误处理等）抽成头文件，在脚本
顶部用 include 指令引入：

.. code-block:: bash

    {% include "head.takler" %}

include 的查找目录依次为（首个命中生效）：

1. **脚本自身所在目录** —— 与 ``.takler`` 脚本同目录的头文件总是
   最先被找到
2. ``TAKLER_INCLUDE`` 变量列出的目录，按列出顺序查找

``TAKLER_INCLUDE`` 是一个普通的 user 变量，取值是用
``os.pathsep`` （ Linux 上为 ``:`` ）分隔的目录列表，通常定义在
flow 上供整棵树共享：

.. code-block:: python

    flow.add_parameter("TAKLER_INCLUDE", "/path/to/includes:/path/to/more")

查找通过
:py:meth:`Node.find_parent_parameter <takler.core.node.Node.find_parent_parameter>`
沿父链进行，因此也可以定义在容器或任务上覆盖搜索路径；未定义该
变量时只在脚本所在目录查找。

head.takler / tail.takler 约定
------------------------------

takler 不提供 ``head.takler`` / ``tail.takler`` ，它们由使用方自行
编写和维护，惯例是：

* ``head.takler`` 放在脚本开头：打开 ``set -e`` 等防御性开关、
  导出与服务通讯所需的环境变量、用 ``init`` child 命令上报任务
  开始、安装把脚本错误转成 ``abort`` 上报的 trap
* ``tail.takler`` 放在脚本末尾：等待后台命令结束、用 ``complete``
  child 命令上报任务完成、清除 trap 并退出

教程使用的 ``head.takler`` 完整内容如下（教程另有一份
``takler-client-py`` 写法，见
:doc:`/tutorial/getting-started/understanding-includes` ）：

.. literalinclude:: /../examples/getting_started/test/head.takler
    :language: bash
    :linenos:

要点：

* 第 3~5 行： ``set -e`` 使任一命令失败即退出，配合第 42 行的
  ``trap ERROR 0`` 让退出时统一走 ``ERROR`` 处理函数上报
  ``abort``
* 第 13~16 行：导出与服务通讯的四个变量。 ``TAKLER_HOST`` /
  ``TAKLER_PORT`` 定位服务， ``TAKLER_NAME`` 是节点路径，
  ``TAKLER_PASS`` 是本次运行的一次性作业口令（启用鉴权后 child
  命令必须携带，见 :doc:`/operation/security` ）
* 第 20~24 行： ``TAKLER_RID`` 是作业标识，本地运行时取当前进程
  号；在作业调度系统中可换成调度系统分配的作业号
* 第 28~29 行： ``init`` 上报任务开始并登记作业标识，任务状态
  从 ``submitted`` 变为 ``active``
* 第 32~46 行： ``ERROR`` 函数先 ``wait`` 后台命令，再上报
  ``abort`` ，最后清除 trap 并以 ``exit 0`` 结束——退出码已无法
  改变结果，状态以 child 命令上报为准

对应的 ``tail.takler`` ：

.. literalinclude:: /../examples/getting_started/test/tail.takler
    :language: bash
    :linenos:

正常路径上 ``complete`` 上报后 ``trap 0`` 清除 trap 、 ``exit 0``
退出；脚本中途出错则由 ``head.takler`` 安装的 trap 接管。

child 命令约定
--------------

脚本通过五个 **child 命令** 向服务上报自身状态： ``init`` 、
``complete`` 、 ``abort`` 、 ``event`` 、 ``meter`` （没有
``label`` ，见 :doc:`/tutorial/events-and-meters` ）。命令参数
与环境变量的约定：

* 节点路径： ``takler-client-py`` 的 child 命令在未给
  ``--node-path`` 时从环境变量 ``TAKLER_NAME`` 读取；为保证两个
  客户端行为一致，示例头文件统一显式传 ``--node-path
  ${TAKLER_NAME}``
* 作业口令：两个客户端都从环境变量 ``TAKLER_PASS`` 读取
* 作业标识： ``init`` 用 ``--task-id`` 上报 ``TAKLER_RID`` ，
  服务用它识别僵尸上报（见
  :doc:`/tutorial/zombies-and-restart` ）

脱离服务单独运行
----------------

调试脚本时经常需要不启动服务直接跑一遍。设置环境变量
``NO_TAKLER`` 后，两个客户端的 child 命令都立即返回成功、不再
连接服务（``takler-client-py`` 会打印 ``ignore because NO_TAKLER
is set.`` ）：

.. code-block:: bash

    export NO_TAKLER=1
    $TAKLER_HOME/test/t1.job1

配合 :doc:`/guide/defining-flows` 的 ``check_job_creation`` 干跑，
完整的离线检查流程是：干跑生成 ``.job`` 文件 → 检查渲染结果 →
``NO_TAKLER=1`` 手动执行验证脚本逻辑。详见
:doc:`/tutorial/getting-started/checking-the-job` 。

渲染失败
--------

下列情况都会使渲染失败， ``create_job_script`` 统一包装为
``JobSubmissionError`` 抛出：

* ``TAKLER_SCRIPT`` 指向的脚本文件不存在
* ``{% include %}`` 的头文件在全部搜索路径中都找不到
* 模板语法错误（如不配对的花括号）

提交阶段失败的后续处理（任务直接转为 ``aborted`` ）见
:doc:`/guide/job-management` 。
