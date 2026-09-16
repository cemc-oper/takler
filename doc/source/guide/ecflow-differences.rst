与 ecFlow 的差异与限制
======================

takler 的节点模型与调度语义沿袭 ecFlow ，但只实现了其中的一个
子集。本页**集中**列出从 ecFlow 迁移时需要知道的全部缺失项及
对应的替代做法——它是文档中唯一罗列缺失项的地方，其余页面只在
相关处交叉引用本页，不重复列举。

节点与属性
----------

``family`` 节点类型
    没有 ``Family`` 类，容器统一为
    :py:class:`~takler.core.NodeContainer` （ ``Flow`` 本身也是
    容器）。ecFlow 中 suite / family / task 三级在 takler 中是
    bunch / flow / 容器嵌套，见 :doc:`/guide/concepts` 。

``label``
    没有该属性，也没有对应的 child 命令。想从运行中的作业暴露一个
    可读值，可用 :doc:`meter </guide/attributes/meter>` 上报数值，
    或写进作业输出后用 TUI 的 output 页查看（见 :doc:`/guide/tui` ）。

``cron`` / ``date`` / ``day`` / ``today``
    均无。时间触发只有 :doc:`time </guide/attributes/time>` （每日
    固定时刻），按日期循环只有 ``RepeatDate`` （见
    :doc:`/guide/attributes/repeat` ）。「在特定日期区间运行」用
    ``RepeatDate`` 的起止日期表达。

``time`` 的区间形式
    不支持（如 ecFlow 的 ``time 10:00 12:00 00:30`` ）。 takler 的
    每个 time 属性只是单一时刻；需要窗口语义时在脚本内自行等待，
    或改用事件 / meter 由外部条件驱动。

``repeat`` 的其他变体
    仅 ``RepeatDate`` ，没有 integer / string / enumerated / day
    变体。枚举式循环的替代写法是在 Python 里用 ``for`` 循环直接
    生成一组任务或容器——工作流本来就是 Python 代码，生成式定义
    比声明式 repeat 更灵活，见 :doc:`/guide/defining-flows` 。

``late``
    没有迟报检测。需要超时告警时在脚本内自行检查，或由外部监控
    轮询 ``show`` 输出实现。

``autocancel`` / ``autorestore``
    均无。完成后的清理、失败后子树的恢复都要显式操作
    （ :doc:`/guide/cli` 的 ``requeue`` / ``force`` ）。

``alias``
    没有节点别名，节点只有路径一个身份。

per-node ``zombie`` 属性
    僵尸判定与处置是服务端的全局设置（ ``zombie_policy`` ），不能
    按节点配置，见 :doc:`/tutorial/advanced-topics/zombies` 与
    :doc:`/operation/security` 。

``defstatus`` 的全部状态
    只支持 ``queued`` 与 ``complete`` 两种默认状态，对应节点的
    ``default_node_status`` （ ``requeue`` 后的归宿）；其余状态
    是运行时的瞬时状态，不能作为默认值。

定义语言与命令
--------------

``.def`` 文本定义语言与解析器
    没有。工作流只能用 Python API 定义（
    :doc:`/guide/defining-flows` ）；与文本定义最接近的是
    ``takler-client-py load`` ，可把
    :py:meth:`Flow.to_dict <takler.core.Flow.to_dict>` 序列化出的
    JSON 文件加载进服务（见 :doc:`/guide/cli` ）。

``%VAR%`` 变量替换与 ``%include``
    用 Jinja2 模板取代：变量写作 ``{{NAME}}`` ，包含写作
    ``{% include %}`` ，搜索路径规则见 :doc:`/guide/task-script` 。

``alter`` / ``delete`` / ``replace`` / ``migrate`` / ``check`` / ``order`` / ``kill`` / ``status`` 命令
    均无对应命令。逐项说明与替代：

    * ``check`` ：定义期校验在 Python 侧完成（属性重复、取值非法等在
      建树时即抛异常）；脚本可渲染性用 ``check_job_creation`` 在提交
      前验证（见 :doc:`/guide/job-management` ）。
    * ``status`` ：查询状态用 ``show`` （ :doc:`/guide/cli` ）或
      :doc:`TUI </guide/tui>` 。
    * ``kill`` ：没有实现。从界面或 CLI 都无法终止运行中的作业，需
      用系统工具按 ``TAKLER_RID`` 手动处理，见
      :doc:`/guide/job-management` 的「现状限制」。
    * ``alter`` / ``delete`` / ``replace`` / ``migrate`` /
      ``order`` ：运行时不能修改、删除或重排节点；结构性变更需修改
      Python 定义并重建 bunch （重启服务或另起 bunch 加载）。

作业提交
--------

ecFlow 支持多种作业提交方式（本地、 PBS 、 Slurm 、 LSF 等），
takler **只有本地 shell 后台运行** 一种：作业是与服务同机、同用户
的子进程，需要调度系统资源时只能在脚本内部自行调用 ``qsub`` /
``sbatch`` 。详见 :doc:`/guide/job-management` 的「现状限制」。
