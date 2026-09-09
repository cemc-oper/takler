作业管理
========

任务从「依赖满足」到「作业在后台运行」之间发生的事情，统称为作业
管理：生成作业文件、拼装提交命令、启动进程、以及失败时把任务置为
``aborted`` 。本页按一次运行尝试的顺序逐项说明，脚本本身的写法见
:doc:`/guide/task-script` 。

作业创建全过程
--------------

调度器判定任务可以运行后调用
:py:meth:`Task.run <takler.core.Task.run>` ，对
:py:class:`~takler.tasks.shell.ShellScriptTask` 来说一次运行尝试的
完整路径如下：

.. mermaid::

    flowchart TD
        run["Task.run()"] --> before["before_run：try_no 加一，轮换作业口令"]
        before --> create["create_job_script：渲染脚本，写出作业文件"]
        create --> cmd["render_job_command：渲染提交命令"]
        cmd --> spawn["ShellRunner.spwan：/bin/sh -c 启动进程"]
        spawn --> submitted["after_run：状态置为 submitted"]
        create -.->|渲染失败| abort1["aborted（JobSubmissionError）"]
        spawn -.->|进程创建失败| abort1
        spawn -.->|进程非零退出| fail["on_job_failure：仍 submitted/active 才 aborted"]

1. ``before_run`` ： ``try_no`` 加一、生成新的作业口令
   （ ``TAKLER_PASS`` ）、清掉上一次的 ``task_id`` 与
   ``aborted_reason``
2. ``create_job_script`` ：先刷新全部 generated 参数，再取
   ``TAKLER_SCRIPT`` 渲染脚本并写出作业文件，最后给作业文件加
   属主执行位（见下文「权限位与 umask 」）
3. ``render_job_command`` ：按 ``TAKLER_SHELL_JOB_CMD`` 模板渲染
   出提交命令
4. ``ShellRunner.spwan`` ：用 ``/bin/sh -c <提交命令>`` 在服务进程
   的事件循环里派生一个子进程任务，作业在后台运行
5. ``after_run`` ：任务状态置为 ``submitted`` 。此后状态推进由
   脚本中的 child 命令上报驱动（ ``init`` → ``active`` ，
   ``complete`` / ``abort`` 收尾）

提交失败（第 2 、 3 步渲染失败，或第 4 步进程创建失败）统一抛出
``JobSubmissionError`` ，记 ERROR 日志后任务直接转为 ``aborted`` ，
**不会** 经过 ``submitted`` ；进程创建失败常见于没有可用事件循环
或系统拒绝派生进程。

生成变量与文件
--------------

一次尝试涉及三个 generated 变量（``ShellScriptTask`` 额外生成，
完整变量表见 :doc:`/guide/variables` ）：

.. list-table::
    :header-rows: 1
    :widths: 22 40 38

    * - 变量
      - 取值
      - 说明
    * - ``TAKLER_SCRIPT``
      - ``script_path`` ，可用同名 user 参数覆盖
      - 任务脚本路径
    * - ``TAKLER_JOB``
      - ``{TAKLER_HOME}{节点路径}.job{try_no}``
      - 渲染产出的作业文件，每次尝试一个新文件
    * - ``TAKLER_JOBOUT``
      - ``{TAKLER_HOME}{节点路径}.{try_no}``
      - 默认提交命令下 stdout 与 stderr 的共同落点

两个路径都会解析为绝对路径。以 ``TAKLER_HOME=$HOME/takler-tutorial``
、任务 ``/test/t1`` 、第二次尝试为例，目录里会出现：

.. code-block::

    $TAKLER_HOME/test/t1.takler   # 用户编写的脚本
    $TAKLER_HOME/test/t1.job1     # 第 1 次尝试的作业文件
    $TAKLER_HOME/test/t1.1        # 第 1 次尝试的输出
    $TAKLER_HOME/test/t1.job2     # 第 2 次尝试的作业文件
    $TAKLER_HOME/test/t1.2        # 第 2 次尝试的输出

旧尝试的作业文件与输出**不会**被清理，按 ``try_no`` 并存，便于
回溯上一次失败。

try_no 与作业口令
-----------------

``try_no`` 标记这是第几次尝试：每次运行前加一， ``requeue`` 时清零
（``requeue`` 意味着下一轮从第 1 次重新计起，作业文件从
``.job1`` 重新写起）。每次加一的同时轮换一次性作业口令
``TAKLER_PASS`` ——43 个字符的 URL-safe 随机串，不含 shell 元字符，
可直接写进作业脚本。

口令是 child 命令的凭据：启用鉴权后，服务只接受携带当前口令的
上报，过期口令（如 requeue 之前启动的旧作业）会被识别为僵尸。
口令不入 ``show`` 输出，也不入 checkpoint 正文（checkpoint 另有
独立的 ``job_passwords`` 映射）。完整叙述见
:doc:`/tutorial/zombies-and-restart` 。

定制作业提交命令
----------------

提交命令取自变量 ``TAKLER_SHELL_JOB_CMD`` ，未定义时用默认值：

.. code-block::

    {{TAKLER_JOB}} 1> {{TAKLER_JOBOUT}} 2>&1

即直接执行作业文件， stdout 与 stderr 一并重定向到
``TAKLER_JOBOUT`` 。模板本身也用 Jinja2 渲染，上下文与脚本渲染
相同（全部可见变量），因此可以在 flow 或容器上定义一次供整棵
子树共享：

.. code-block:: python

    # 单独留存 stderr，并记录每次提交时间
    flow.add_parameter(
        "TAKLER_SHELL_JOB_CMD",
        "date >> {{TAKLER_JOBOUT}}.log; "
        "{{TAKLER_JOB}} 1>> {{TAKLER_JOBOUT}} 2> {{TAKLER_JOBOUT}}.err",
    )

查找沿父链进行（
:py:meth:`Node.find_parent_parameter <takler.core.node.Node.find_parent_parameter>`
），离任务近的定义优先。

.. note::

    定制模板时保留「作业失败能以非零码退出」这一性质——
    ``on_job_failure`` 依赖进程退出码识别失败（见下一节）。把
    命令包进总是返回 0 的包装器会让失败静默。

作业失败如何变成 aborted
------------------------

运行中的作业有三条路径走向 ``aborted`` ：

1. **脚本 trap 上报** （常规路径）： ``head.takler`` 安装的 trap 在
   脚本出错时执行 ``abort`` child 命令，状态立即转为
   ``aborted`` ，脚本随后以 ``exit 0`` 收尾
2. **进程非零退出** （兜底路径）： trap 无法接管的终止方式
   （如 SIGKILL ）使 ``/bin/sh -c`` 非零退出，服务端的作业任务以
   ``CalledProcessError`` 结束，记 ERROR 日志后触发
   ``on_job_failure``
3. **提交失败**：上文的 ``JobSubmissionError`` 路径

``on_job_failure`` 只在任务仍处于 ``submitted`` 或 ``active`` 时
置 ``aborted`` ：若 child 命令已经上报了 ``complete`` 或
``aborted`` ，包装进程的退出码不再覆盖结果，仅记一条 INFO 日志。
这保证了「状态以 child 命令上报为准，进程退出码只做兜底」。

权限位与 umask
--------------

作业文件由 takler 以默认方式创建，读写权限位完全由服务进程的
umask 决定； takler 只额外补一个**属主执行位**（``chmod
mode | 0o100`` ），从不显式设定整体权限。原因是作业脚本内嵌
``TAKLER_PASS`` 口令，是否允许同组 / 其他用户读取属于部署决策，
应由启动服务时的 umask 表达，而不是由 takler 放宽。例如以
``umask 077`` 启动服务，作业文件即仅属主可读写执行。

现状限制
--------

* **只有本地 shell 后台运行** 一种提交方式：作业是与服务同机、
  同用户的子进程，没有 PBS / Slurm 等调度系统提交；需要调度系统
  资源时只能在脚本内部自行调用 ``qsub`` / ``sbatch``
* **没有 kill 实现**： ``TAKLER_SHELL_KILL_CMD`` 常量已定义但全库
  无任何引用，从界面或 CLI 都无法终止一个运行中的作业；需要终止
  时用系统工具按 ``TAKLER_RID`` （进程号）手动处理，任务状态随后
  由作业脚本的 trap 或 ``on_job_failure`` 兜底
* **作业输出不经过 RPC** ：输出文件留在 ``TAKLER_HOME`` 下，
  客户端命令取不到内容； TUI 的 output 页是直接读本地文件，
  因此要求 TUI 与服务能访问同一文件系统
