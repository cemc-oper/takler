僵尸检测与处置
==============

一个 child 命令（ ``init`` 、 ``complete`` 、 ``abort`` 、 ``event`` 、
``meter`` ）如果不属于服务当前记录的运行实例，就是 **zombie** 。典型情形
是 task 被 requeue 之后，仍在运行的旧作业才上报 ``complete`` ——放任这类
上报会污染新运行实例的状态。本页说明执行身份门禁，以及门禁通过后
既有的三个 zombie 条件和三种策略； ``auth_mode`` 的配置见
:doc:`/operation/security` 。

判定的时机与范围
----------------

每个 child 命令在调度器中定位并类型检查目标节点之后、写入任何状态之前，
先核对当前 ``attempt_id``、提交状态和在线树归属。旧身份、终态、
缺失身份直接返回 ``attempt_mismatch``；非法 UUID 是输入错误。
``event`` / ``meter`` 还须核对上报者 Task 与目标同属一个 Flow。
这层门禁不受 ``zombie_policy`` 的 ``fob`` / ``adopt`` 影响。
之后才执行既有 zombie 判定。两种目标保持原有的报错：

* 节点不存在： ``NodeNotFoundError``
* 节点不是 task ： ``NodeTypeError``

运维命令（ ``requeue`` 、 ``suspend`` 等）不做 zombie 判定——zombie 是
作业实例与服务记录不一致的概念，只存在于 child 命令上。

三个判定条件
------------

三个条件按固定顺序 ``Z1`` → ``Z2`` → ``Z3`` 检查，命中第一个即停止。
顺序从口令到状态和 RID；这三项只适用于已经通过当前 attempt 门禁的
上报。requeue 后旧作业的上报在此之前就被拒绝，不会按 Z1/Z2 处置。

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
* ``Z1`` 在 ``disabled`` 下整体跳过。``Z2`` 与 ``Z3`` 不需要凭据；
  不论鉴权模式如何，旧 attempt 都先被身份门禁挡住

三种处置策略
------------

``zombie_policy`` 是服务端全局设置，只对通过当前 attempt 门禁后
命中的 zombie 条件生效：

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
requeue （任务回到 queued，当前 attempt 清空）→ 旧作业继续运行并上报
``complete``。无论 ``auth_mode`` 和 ``zombie_policy`` 如何设置，
旧 ``attempt_id`` 与当前任务不匹配，服务返回 ``attempt_mismatch``；
若新尝试已提交，也不会误收旧上报。该场景的端到端演示见
:doc:`/tutorial/advanced-topics/zombies` 。

.. note::

    ``Z2`` 与 ``Z3`` 在 ``auth_mode`` 为 ``disabled`` 时仍可判定，
    但 ``fob`` 不能让旧 attempt 上报成功。UUID 隔离不是鉴权；
    生产部署仍须启用鉴权保护 child 和 operator 命令。

与检查点的关系
--------------

checkpoint v3 在 ``runtime`` 保存当前 ``attempt_id``，在 ``secrets``
保存 submitted / active 任务的配对口令。恢复前强制校验两者完整性，
缺失或冲突时拒绝整份快照；不会把「在途但无口令」任务交给 zombie 策略。
有效快照恢复后，匹配身份和口令的在途上报仍可被接受，见
:doc:`/operation/checkpoint` 。

配置方式
--------

``zombie_policy`` 在 ``connect.yaml`` 的 ``security`` 段配置，或用环境
变量 ``TAKLER_ZOMBIE_POLICY`` 覆盖；没有命令行选项。无法识别的取值记一条
WARNING 并回退到默认的 ``fail`` 。完整的优先级链见
:doc:`/operation/connect-config` 。
