僵尸作业 (zombie)
==================

本节介绍 takler 的**僵尸检测**机制：识别并处置「不属于任务当前运行
实例」的上报，防止一个已经被重排队的任务的旧作业污染新一轮运行。
下一节 :doc:`restart` 介绍服务重启后的恢复。

本节可以亲手复现，继续使用 :doc:`controlling-the-flow` 一节的示例
工作流。

什么是僵尸
--------------------

任务脚本通过 child 命令（``init`` 、 ``complete`` 、 ``abort`` 、
``event`` 、 ``meter`` ）向服务上报状态。典型的僵尸场景：任务
``t1`` 正在运行，你对它执行了 ``requeue`` ——服务端 ``t1`` 回到
``queued`` 并很快被重新提交（新一轮运行开始），但**旧作业还在跑**。
当旧作业最后调用 ``complete`` 上报时，这条上报就属于已经不存在的运行
实例：如果放行它，新一轮运行的状态就会被旧作业篡改。

执行身份、try_no 与作业口令
-------------------------------

服务为每次实际执行分配一个不可复用的 ``attempt_id`` UUID，并通过作业脚本
中的 ``TAKLER_ATTEMPT_ID`` 传给五个 child 命令。服务先核对该身份、任务
仍在当前树中的归属和 submitted/active 状态，再允许任何状态变更。
``zombie_policy`` 的 ``fob`` 或 ``adopt`` 也不能放行旧身份。

``try_no`` 每次运行加一，但 ``requeue`` 会把它清零，因此不能用它识别
某轮执行。实际作业文件默认名为 ``t1.job<attempt_id>``，重排队后即使
``try_no`` 再次为 1，文件也不复用。``TAKLER_RID`` 是 ``init`` 上报的
外部作业标识，不是本轮身份。每轮还生成新的**作业口令**，通过
``TAKLER_PASS`` 传给 child 命令；启用鉴权时它是凭据，但身份 UUID 本身
不是凭据。更多区别见 :doc:`/guide/job-management`。

判定条件
--------------------

身份检查通过后，既有 zombie 检测还会检查：

* ``Z1`` ：命令携带的口令与任务当前口令不符（或任务根本没有口令）。
  只在服务端开启鉴权 (``auth_mode: enabled``) 时检查
* ``Z2`` ：任务当前状态不是 ``submitted`` 也不是 ``active`` ——
  服务端没有正在等它上报的运行实例；当前服务入口的身份检查会先拒绝
  此类上报
* ``Z3`` ：``init`` 命令携带的作业号与已记录的当前作业号不同——
  两个作业都声称自己拥有这次运行

处置策略
--------------------

**旧 attempt、缺失/非法 attempt 和终态上报** 由身份检查直接拒绝，
不进入下列策略。通过身份检查但命中 zombie 条件的命令按
``zombie_policy`` 配置处置：

* ``fail`` （默认）：拒绝，任务状态保持不变，客户端收到 ``ZombieError``
  报错并以非零码退出
* ``fob`` ：丢弃但回答成功，客户端无从察觉（服务端日志仍有记录）
* ``adopt`` ：接受上报，并把口令接管为命令携带的口令

无论哪种策略，每次处置都会在服务日志留下一条 WARNING，写明节点路径、
命令名、命中的条件与当前状态。处置策略的配置方式见
:doc:`/operation/zombie` ，鉴权的配置方式见 :doc:`/operation/security`
。

动手观察一次僵尸拒绝
--------------------

为了让作业运行足够久，先临时在 **task1.takler** 中加一行
``sleep 300`` ，然后：

1. 启动服务并 ``begin`` ，等 ``t1`` 进入 ``active``；从 ``show``
   记录这轮的 ``attempt_id``
2. 执行 ``takler-client-py requeue /test/t1`` ， ``t1`` 回到
   ``queued`` （旧作业仍在运行）
3. 在旧作业的 ``sleep`` 结束、调用 ``complete`` 时，服务拒绝旧身份，
   客户端报 ``attempt_mismatch``；旧上报不会改变新一轮状态。
   即使服务尚未再次提交新作业，旧身份仍已失效

也可以用记录的旧 UUID 模拟过时上报：在 shell 中设置
``OLD_ATTEMPT`` 为上一步记录值，然后执行
``takler-client-py complete --node-path /test/t1 --attempt-id "$OLD_ATTEMPT"``。
结果同样是 ``attempt_mismatch``。未给 ``--attempt-id`` 且没有
``TAKLER_ATTEMPT_ID`` 时，客户端会在发送前报输入错误。

.. note::

    默认部署不开启鉴权，此时不检查口令对应的 ``Z1``，但始终校验
    ``attempt_id``。生产环境仍应参考 :doc:`/operation/security`
    启用鉴权：UUID 用于隔离执行，不替代作业口令认证。

练习
-----

1. 按本节步骤复现一次旧身份拒绝，确认状态不变
2. 查看某次运行生成的 ``t1.job<attempt_id>`` 作业脚本，找到
   ``TAKLER_ATTEMPT_ID`` 和 ``TAKLER_PASS``；requeue 后再次运行，
   对比两次值均不同
3. 在 ``t1`` 处于 ``queued`` 时携带旧 ``--attempt-id`` 手动执行
   ``complete``，确认返回 ``attempt_mismatch``
