僵尸检测与处置
==============

一个 child 命令（ ``init`` 、 ``complete`` 、 ``abort`` 、 ``event`` 、
``meter`` ）如果不属于服务当前记录的运行实例，就是 **zombie** 。典型情形
是 task 被 requeue 之后，仍在运行的旧作业才上报 ``complete`` ——放任这类
上报会污染新运行实例的状态。本页说明三个判定条件、三种处置策略与配置
方式； ``auth_mode`` 本身的配置见 :doc:`/operation/security` 。

判定的时机与范围
----------------

每个 child 命令在调度器中定位并类型检查目标节点之后、写入任何状态之前
先过一次 zombie 判定。两种目标不算 zombie ，保持原有的报错：

* 节点不存在： ``NodeNotFoundError``
* 节点不是 task ： ``NodeTypeError``

运维命令（ ``requeue`` 、 ``suspend`` 等）不做 zombie 判定——zombie 是
作业实例与服务记录不一致的概念，只存在于 child 命令上。

三个判定条件
------------

三个条件按固定顺序 ``Z1`` → ``Z2`` → ``Z3`` 检查，命中第一个即停止。
顺序从最具体到最宽泛： ``Z1`` 能说出调用方属于哪次运行（持的是旧口令）
， ``Z2`` 只能说出任务不在接受上报的状态——一个被 requeue 的任务同时
命中两者，报 ``Z1`` 比报 ``Z2`` 多一层诊断信息。

.. list-table::
    :header-rows: 1
    :widths: 10 60 30

    * - 条件
      - 含义
      - 判定的鉴权模式
    * - ``Z1``
      - 命令携带的口令与目标 task 当前口令不一致，或目标 task 没有口令
      - 仅 ``enabled``
    * - ``Z2``
      - 目标 task 既不是 submitted 也不是 active 状态，即服务没有期待
        它的任何上报
      - 两种模式都判定
    * - ``Z3``
      - ``init`` 命令携带的 ``task_id`` 与 active 状态目标 task 已记录
        的取值不一致，即第二个作业声称拥有同一次运行
      - 两种模式都判定

细节约定：

* ``Z1`` 的口令比较是常数时间的，防止按响应延迟逐字符推测口令。不带
  ``takler-pass`` 的调用视同不匹配（「没有口令」不是凭据）； ``enabled``
  下拦截器会先拒绝缺少凭据的 child 命令，因此这种情况很少走到判定层
* ``Z3`` 只适用于 ``init`` ——它是唯一携带作业 id 的 child 命令；空白
  的 id 与未携带视为同一个「无 id 」取值
* ``Z1`` 在 ``disabled`` 下整体跳过：不鉴权时没有客户端被期待携带
  ``takler-pass`` ，每个 child 命令都会命中 ``Z1`` ，等于拒绝整个 child
  协议。 ``Z2`` 与 ``Z3`` 不需要凭据，在两种模式下都在场——requeue
  场景不靠鉴权也能拦住

三种处置策略
------------

``zombie_policy`` 是服务端全局设置，对全部 zombie 生效：

.. list-table::
    :header-rows: 1
    :widths: 12 88

    * - 取值
      - 处置方式
    * - ``fail``
      - 默认值。不改变目标 task 的任何状态（状态、 ``task_id`` 、
        ``try_no`` 、 ``aborted_reason`` 与口令都保持原样），返回
        ``flag=31`` ，客户端以退出码 ``3`` 结束并输出分类名 ``zombie``
    * - ``fob``
      - 不改变目标 task 的任何状态，但返回成功（ ``flag=0`` ），旧作业
        静默继续——客户端无从得知自己的上报被丢弃，日志中的 WARNING 与
        审计记录是这次处置唯一的痕迹
    * - ``adopt``
      - 执行该命令，并把命令携带的口令收养为目标 task 的口令（空白的
        口令视为未携带，不收养）； ``Z3`` 情形下的 ``task_id`` 由
        ``init`` 命令自身执行时写入

每次处置恰好记录一条 WARNING ，含节点路径、命令名、命中的条件、生效的
策略与目标 task 当前状态，并写出一条审计记录（ ``event`` 与 ``outcome``
均为 ``zombie`` ，见 :doc:`/operation/security` 的「审计日志」一节）。
日志与审计记录都不含口令取值。

典型场景： requeue 之后旧作业上报
---------------------------------

时间线： ``t1`` 提交作业并上报 ``init`` （任务进入 active ）→ 运维
requeue （任务回到 queued ，口令被清掉）→ 旧作业继续运行并上报
``complete`` ：

* ``auth_mode`` 为 ``enabled`` ：旧作业持的是已清空的旧口令，命中
  ``Z1``
* ``auth_mode`` 为 ``disabled`` ：目标任务不在 submitted / active ，
  命中 ``Z2``

两种模式下这条 ``complete`` 都不会写进新运行实例。该场景的端到端演示见
:doc:`/tutorial/zombies-and-restart` 。

.. note::

    ``Z2`` 与 ``Z3`` 在 ``auth_mode`` 为 ``disabled`` 时同样生效，这是
    本版本对既有部署可见的行为改变： requeue 之后旧作业上报的 child 命令
    会被拒绝，而不再静默污染新实例的状态。升级后如需临时保留旧行为，可以
    把 ``zombie_policy`` 设为 ``fob`` 。

与检查点的关系
--------------

快照只持久化 submitted / active 任务的口令（其他状态的口令即使匹配也会
命中 ``Z2`` ，没有保存价值），重启后还原到对应 task 上，在途作业因此
**不会** 因为重启变成 zombie ，见 :doc:`/operation/checkpoint` 。反过来
， ``auth_mode`` 为 ``enabled`` 时恢复自检发现的「在途但无口令」任务，
其 child 命令会命中 ``Z1`` ，由本页的策略处置。

配置方式
--------

``zombie_policy`` 在 ``connect.yaml`` 的 ``security`` 段配置，或用环境
变量 ``TAKLER_ZOMBIE_POLICY`` 覆盖；没有命令行选项。无法识别的取值记一条
WARNING 并回退到默认的 ``fail`` 。完整的优先级链见
:doc:`/operation/connect-config` 。
