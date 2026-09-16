属性
====

属性 (attribute) 是挂在节点上的结构化条目，控制节点何时运行、运行几次、
并发占用多少资源，以及运行期间向外界暴露什么进度。本章按属性分节，
每节给出构造参数、代码示例、在触发器中的用法，以及与 requeue 和
序列化的交互。入门介绍见教程：事件 :doc:`/tutorial/going-further/events` 、
标尺 :doc:`/tutorial/going-further/meters` 、限额 :doc:`/tutorial/advanced-topics/limits` 、
repeat :doc:`/tutorial/advanced-topics/repeat` 与时间 :doc:`/tutorial/advanced-topics/time` 。

.. list-table::
    :header-rows: 1
    :widths: 15 45 40

    * - 属性
      - 作用
      - 添加方法
    * - :doc:`event`
      - 任务运行期间上报的布尔开关，用于放行下游
      - :py:meth:`Node.add_event <takler.core.node.Node.add_event>`
    * - :doc:`meter`
      - 任务运行期间上报的带范围整数，用于表示进度
      - :py:meth:`Node.add_meter <takler.core.node.Node.add_meter>`
    * - :doc:`limit`
      - 限制同时运行的任务数量
      - :py:meth:`Node.add_limit <takler.core.node.Node.add_limit>` 与
        :py:meth:`Node.add_in_limit <takler.core.node.Node.add_in_limit>`
    * - :doc:`repeat`
      - 节点（及其子树）按日期循环运行
      - :py:meth:`Node.add_repeat <takler.core.node.Node.add_repeat>`
    * - :doc:`time`
      - 让节点等到 flow 逻辑时钟到达某个时刻才可运行
      - :py:meth:`Node.add_time <takler.core.node.Node.add_time>`

五类属性都会随节点序列化：保存状态（ checkpoint 、 ``show`` ）时带运行
期取值，只保存定义时（如 load 一棵树）只保留定义、运行期取值回到初始
状态。各节末尾分别说明。

.. toctree::
   :hidden:
   :maxdepth: 1

   event
   meter
   limit
   repeat
   time
