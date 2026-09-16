高级话题
==========

本章在 :doc:`/tutorial/going-further/index` 的基础上，介绍控制工作流
**循环与运行**\ 的功能，以及运行期的可靠性机制：

* 循环与定时：:doc:`repeat` 让一组任务按日期循环运行，:doc:`time`
  让节点等到一天中的某个时刻才运行
* 并发控制：:doc:`limits` 限制同时运行的任务数量
* 运行控制：:doc:`controlling-the-flow` 介绍 begin / requeue / suspend
  / force 等人工干预命令
* 可靠性：:doc:`zombies` 介绍僵尸上报的识别与处置，:doc:`restart`
  介绍服务重启后如何从检查点恢复

每节介绍一个新概念，建议按顺序完成各节练习。

.. note::

    ecFlow 教程高级话题中的部分内容在 takler 中**没有对应功能**，例如
    ``cron`` / ``date`` / ``day`` 时间属性、整数与枚举 repeat 变体、
    ``late`` 迟报检测、 ``alias`` 别名等。完整的差异清单与替代做法见
    :doc:`/guide/ecflow-differences` 。

.. toctree::
   :hidden:
   :maxdepth: 2

   repeat
   time
   limits
   controlling-the-flow
   zombies
   restart
