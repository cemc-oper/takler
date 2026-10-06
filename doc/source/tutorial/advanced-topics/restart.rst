重启恢复 (checkpoint)
======================

上一节介绍了僵尸检测。本节介绍 takler 的另一个可靠性机制：
**检查点 (checkpoint)** ——服务周期性保存快照，进程被杀掉后重启可以
从快照恢复。恢复不会自动重提交作业；外部作业是否仍在运行需要人工核实。
只有 checkpoint 用于恢复，运行和审计日志不参与重放。

本节可以亲手复现，继续使用 :doc:`controlling-the-flow` 一节的示例
工作流。

快照
--------------------

运行中的服务把整棵节点树周期性写入检查点文件（默认
``takler.check`` ，位于服务进程的工作目录，默认每 120 秒一次）；正常
退出时会再写最后一次。写入是原子的（临时文件 + 重命名），并同时维护
一个备份文件 ``takler.check.b`` 。快照保存节点状态、 ``suspended``
标记、事件与标尺取值、限额占用、repeat 当前值、时间依赖的 ``free``
闩锁、任务的 ``attempt_id`` / ``task_id`` / ``try_no`` / 中止原因、
flow 的 begun 标记与逻辑日历，以及**在途任务** （ ``submitted`` /
``active`` ）的作业口令
——因此文件权限被固定为仅属主可读写 (``0600``)。

恢复
--------------------

主、备文件都不存在时作为新服务以 running 启动。只要其中任一文件存在，
服务就按 ``takler.check`` → ``takler.check.b`` 依次完整校验；两份都
无效时拒绝启动，不会改用空工作流。恢复出的 ``submitted`` /
``active`` 任务**保持原状，不会重新提交作业**。匹配快照身份和口令的
旧作业仍可上报；快照之后才发生的提交可能没有记录，不能从日志补回。
恢复后的服务默认 halted，人工核实并执行 ``server-resume`` 前不会
启动下游新执行。快照格式、回退链与排查方法的完整说明见
:doc:`/operation/checkpoint` 。

动手观察一次重启恢复
--------------------

1. 沿用 :doc:`zombies` 一节带 ``sleep 300`` 的工作流，启动服务、
   ``begin`` ，等 ``t1`` 进入 ``active``
2. 等待至少一个快照周期（默认 120 秒；确认工作目录下出现
   ``takler.check`` ），然后用 ``kill -9`` 杀掉服务进程——模拟宕机，
   正常退出的收尾快照不会执行
3. 重新运行 **test.py** 启动服务，日志中会出现：

   .. code-block::

       INFO ... restored 1 flow(s) and 4 node(s) from checkpoint file takler.check.
       INFO ... recovered the job password of 1 task(s) from checkpoint file takler.check.

4. 用 ``server-status`` 和 ``show --show-all`` 核实服务为 halted、
   ``t1`` 仍是 ``active``、本轮 ``attempt_id`` 和外部作业是否匹配。
   若旧作业的 ``sleep`` 结束后上报 ``complete``，匹配上报可被接受，
   但 halted 期间下游不会开始
5. 核实快照以外的提交和作业结果，必要时择一使用 ``force`` 或
   ``requeue`` 修正节点；最后执行 ``server-resume``，让下游继续调度。
   写命令成功只表示内存操作成功，下一次快照前仍可能丢失

.. note::

    重启后服务监听的地址或端口如果与快照中记录的不同，且快照里还有
    在途任务，那些任务作业脚本里写死的是旧地址，它们的上报会连不上
    新服务——服务启动时会对这种情况记录 ERROR 并列出受影响的任务。
    恢复在途任务时请保持地址端口不变。

练习
-----

1. 把快照周期调短（``connect.yaml`` 的 ``checkpoint.interval`` ，
   最短 10 秒），复现一次 ``kill -9`` 后的恢复
2. 删除 ``takler.check`` 但保留 ``takler.check.b`` ，重启服务，
   确认从备份快照恢复
3. 恢复后用 ``show`` 检查事件、标尺与 repeat 当前值，确认它们都从
   快照还原
