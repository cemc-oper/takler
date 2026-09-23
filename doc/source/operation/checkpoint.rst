检查点与恢复
============

takler 服务把整棵节点树的状态定期快照到**检查点文件**，重启后从它恢复，
在途作业因此可以跨重启继续上报。本页说明快照里有什么、写入如何保证
不留下半个文件、启动时的恢复流程与恢复失败时的排查方法。

检查点文件的路径与快照周期的取值优先级（ ``--checkpoint-file`` /
``--checkpoint-interval`` > ``connect.yaml`` 的 ``checkpoint`` 段 >
内置默认）见 :doc:`/operation/connect-config` ；停机时写最终快照的
流程见 :doc:`/operation/deployment` 。

快照内容
--------

检查点文件是一个 JSON 文档，顶层五个键：

.. list-table::
    :header-rows: 1
    :widths: 22 78

    * - 键
      - 内容
    * - ``format_version``
      - 快照格式版本，当前为 ``2``
    * - ``takler_version``
      - 写出快照的 takler 版本，仅作诊断，不参与恢复
    * - ``written_at``
      - 写入时间，仅作诊断，不参与恢复
    * - ``bunch``
      - 整棵节点树的状态序列化：节点状态、 ``suspended`` 、event 与
        meter 的当前值、limit 的计数、repeat 计数、时间属性的 free
        闩锁、task 的 ``task_id`` / ``try_no`` / ``aborted_reason``
        、flow 的 ``begun`` 标志与 calendar
    * - ``job_passwords``
      - 「节点路径 → 作业一次性口令」映射，只收录状态为 submitted 或
        active 且口令非空的 task

``job_passwords`` 刻意放在 ``bunch`` **之外** ： ``show`` 命令与快照共用
同一份节点树序列化，口令若放在树内会随 ``show`` 返回给每个调用方。

**重启后在途任务为何仍能上报** ：恢复时 submitted / active 任务的状态原样
保留（不会重新排队，也不会重新提交作业）， ``job_passwords`` 中的口令再
写回对应 task。旧作业脚本里注入的 ``TAKLER_PASS`` 因此仍然匹配，目标
task 也仍处在接受上报的状态，child 命令两个 zombie 条件都不命中
（见 :doc:`/operation/zombie` ）。complete 与 aborted 任务的口令不持久化
——它们的运行已结束，对口令的上报无论如何都会命中 ``Z2`` 。

快照中的服务地址（ ``server_state`` ）在恢复时被丢弃， ``TAKLER_HOST`` /
``TAKLER_PORT`` 始终宣告当前进程，而不是写快照的那个进程。

原子写与备份文件
----------------

每次快照按固定的五个步骤写入：

#. 创建检查点文件的父目录（若不存在）
#. 把新快照写入同目录的临时文件（ ``takler.check.tmp.<pid>`` ），
   写完 ``fsync`` 落盘
#. 把当前检查点文件复制为备份用的临时文件（首次快照时跳过）
#. 用备份临时文件原子替换**备份文件**（检查点路径加 ``.b`` 后缀，如
   ``takler.check.b`` ）
#. 用新快照的临时文件原子替换**检查点文件**

临时文件与目标同目录，替换因此是同一文件系统内的原子操作——任何时刻
检查点文件要么不存在、要么是一份完整快照，不存在写了一半的状态；
``fsync`` 保证机器掉电（而不只是进程被杀）后内容仍可读。备份文件路径由
检查点路径派生，不可单独配置，两份文件因此不会失配；首次快照后才有
备份文件。

任一步骤失败：记一条含目标路径、步骤名与原因的 ERROR，清理临时文件，
**检查点文件与备份文件保持原样**，服务继续运行——写不出快照不是停服务
的理由。单次写入耗时超过配置周期时只记一条 WARNING，不补写。

检查点文件、备份文件与临时文件都以仅所有者可读写（ ``0600`` ）创建：
快照含有全部在途作业的一次性口令，在共享登录节点上不能被其他用户读到。

周期快照任务在服务启动的最后一步才创建，且先睡眠一个周期再写第一次，
不会一启动就覆盖刚恢复出来的快照。优雅停机时先取消周期任务、等它已
派出的写入完成，再同步写出**最终快照**，因此正常停机不丢失任何状态
变化（ ``SIGKILL`` 没有这一步，见 :doc:`/operation/deployment` ）。

周期与文件位置
--------------

内置默认值为当前工作目录下的 ``takler.check`` 、每 ``120`` 秒一次。
周期小于 ``10`` 秒（或不合法）被拒绝，记一条 WARNING 并回退到
``120`` 秒——更密的快照会把时间花在序列化节点树上而不是调度上。
相对路径按服务进程的工作目录解析，生产部署应给服务专设工作目录，
见 :doc:`/operation/deployment` 的启动前检查清单。

启动恢复流程
------------

恢复在 ``takler-server`` 启动流程中位于安全校验之后、调度器启动之前，
因此第一次依赖求解看到的就是恢复后的节点树。恢复**永不抛异常**：
快照不可用只会逐级降级，不会阻止启动。

回退链为 **检查点文件 → 备份文件 → 空 bunch** ：

* 文件不存在：记一条 INFO，进入下一级
* 文件不可读、不是 JSON、缺 ``bunch`` 键，或 ``format_version`` 不是当前
  整数版本 ``2``：记一条含路径与原因的 ERROR，进入下一级。缺少版本、
  null、布尔值、浮点数以及旧/未知版本均拒绝；不提供历史格式兼容
* 两级都不可用：记一条 ERROR，以空 bunch 启动

成功恢复时记 INFO，报告 flow、节点和口令数量。先在临时树校验全部
节点类型、运行字段、引用、limit 占用和在途口令；任何一项失败都拒绝
整份快照，再尝试备份。不会跳过坏 flow 后报告成功。
``job_passwords`` 必须存在；未知路径、非 task 路径、终态口令条目或
缺失 submitted/active 口令使快照失败，不回显口令值。

根级属性恢复
~~~~~~~~~~~~

恢复保留原 Bunch 对象，恢复根名称、用户参数（含 null）、状态、默认状态、
触发器、事件、标尺、limit/in-limit、repeat 和 time 的已存储值。
根参数仍由 flow 和子节点继承；这些存储字段的往返不代表根调度属性能控制全部 flow。
``trigger_free`` / ``complete_trigger_free`` 保存 free-dep 的运行效果，
v2 必须显式保存这两个字段及完成触发锁存；缺失时拒绝恢复。

checkpoint 恢复保留当前 ``server_state`` 对象及部署配置，不使用快照旧值替换
host/port、服务器参数或当前 TLS/auth 等配置。快照中根或子节点的用户参数
``TAKLER_HOST`` / ``TAKLER_PORT`` 覆盖项被移除并记录不含值的冲突警告，
使继承得到当前服务地址；显式用户 ``TAKLER_HOME`` 和其他业务参数保持原值。
独立 ``Bunch.from_dict`` 没有在线部署上下文，仍恢复其字典中的 server_state。

在途口令完整性在提交临时树之前强制校验，不因关闭认证而跳过。
恢复之后还会进行地址诊断：

* **地址一致性校验** ：比较快照记录的服务地址与本次启动地址。一致记
  INFO ；不一致但没有在途任务记 WARNING （把空闲服务迁到另一台机器是
  常规操作）；不一致且仍有 submitted / active 任务记 ERROR ，列出两组
  地址与全部受影响任务的路径——这些在途作业的作业脚本里写死了旧地址，
  上报会一直重试到失败。该前提的完整说明见 :doc:`/operation/deployment`
  的选项表注记，恢复后的表现见 :doc:`/tutorial/advanced-topics/restart`

自定义 Task 的恢复要求
----------------------

恢复只使用启动代码注册的稳定 ``type_id``。扩展必须提供定义 schema、
构造器、定义导出器及运行状态 schema/读写 codec，见 :doc:`/develop/extending`。
输入不能触发动态模块导入；未知类型使整份快照失败。版本 1 不再读取，
没有别名或转换器。

发给他人分析前的脱敏
--------------------

``job_passwords`` 键保存着全部在途作业的一次性口令，文件权限也因此收紧
为 ``0600`` 。需要把快照发给他人分析时，先删掉这个键：

.. code-block:: bash

    jq 'del(.job_passwords)' takler.check > takler.check.shared

恢复失败时怎么查
----------------

恢复过程的所有结论都在日志里（控制台或 ``TAKLER_LOG_FILE`` 指定的文件，
见 :doc:`/operation/deployment` ），按以下记录定位：

* ``failed to parse checkpoint file ...`` ：某级文件不可用，原因在
  同一行（不是 JSON 、缺 ``bunch`` 键、版本过新等），服务已进入回退链
  的下一级
* ``could not restore from the checkpoint file ... nor from the backup
  file`` ：两级都失败，服务以空 bunch 启动
* ``failed to restore the bunch ...``：整份快照未通过类型、字段或引用校验，
  原在线树保持不变，继续尝试备份
* ``restored N flow(s) and M node(s)`` 与 ``recovered the job password
  of K task(s)`` ：恢复成功，把数字与预期对比即可确认完整性
* ``checkpoint server address differs ...`` ：地址不一致的分级记录，
  出现 ERROR 说明有在途任务的上报会失败，需决定是否以旧地址重启
