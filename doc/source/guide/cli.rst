命令行参考
==========

takler 提供两个命令行客户端，与服务通讯的协议、地址解析规则和退出码
完全一致，同一份作业脚本可以混用两者：

``takler_client``
    Go 实现的独立二进制，来自单独的 `takler-client
    <https://github.com/cemc-oper/takler-client>`_ 仓库，按该仓库的
    说明构建安装。

``takler-client-py``
    Python 实现，随 ``pip install takler`` 一并安装，即
    ``takler.client.cli`` 模块的入口（ ``python -m takler.client``
    等价）。

命令分三组： **child 命令** 在作业脚本内上报任务状态（用法见
:doc:`/guide/task-script` ）， **控制命令** 改变节点或 flow 的状态，
**查询命令** 读取服务状态。本页逐条列出全部命令与选项，并说明地址
解析与退出码——作业脚本只能用退出码判断成败（配合 ``set -e`` ），
stderr 固定为一行诊断信息，绝不出现 Python traceback 。

地址解析
--------

两个客户端按完全相同的优先级确定服务地址（高优先级覆盖低优先级，
host 与 port 各自独立解析）：

1. 命令行选项 ``--host`` / ``--port``
2. 环境变量 ``TAKLER_CONNECT_FILE`` 指向的 ``connect.yaml``
   （取其中 ``server.address`` 段）
3. 环境变量 ``TAKLER_HOST`` / ``TAKLER_PORT``
4. 内置默认 ``localhost:33083``

``connect.yaml`` 同时还提供 TLS 与鉴权设置（客户端读取其
``security`` 段），完整字段说明见 :doc:`/operation/security` 。

退出码
------

.. list-table::
    :header-rows: 1
    :widths: 12 88

    * - 退出码
      - 含义
    * - ``0``
      - 成功。child 命令在 ``NO_TAKLER`` 已设置时短路并打印一行提示，
        也以 ``0`` 退出
    * - ``1``
      - 请求本身不被接受：节点不存在、节点路径格式错误、取值不支持、
        触发器表达式语法错误、鉴权拒绝等
    * - ``3``
      - 服务端执行失败（如作业提交失败、僵尸检测拒绝上报），或服务
        返回了客户端无法解析的响应
    * - ``4``
      - 重试窗口（Retry Window）耗尽仍无法连上服务

失败时 stderr 恰好一行：服务端拒绝是 ``错误分类名: 服务端消息`` ，
客户端本地失败是 ``异常类型名: 描述`` 。未预期异常的 traceback 只写入
日志文件（配置 ``TAKLER_LOG_FILE`` 时），不会出现在 stderr 。

TLS 与鉴权
----------

启用 TLS 或鉴权（ ``auth_mode: enabled`` ）后，客户端按「命令行选项
> 环境变量 > ``connect.yaml`` 的 ``security`` 段」的优先级解析三个
设置，详见 :doc:`/operation/security` ：

.. list-table::
    :header-rows: 1
    :widths: 34 30 36

    * - 环境变量
      - ``connect.yaml`` 字段
      - ``takler_client`` 选项
    * - ``TAKLER_TLS_CA_FILE``
      - ``security.ca_file``
      - ``--tls-ca``
    * - ``TAKLER_TLS_SERVER_NAME``
      - ``security.server_name``
      - ``--tls-server-name``
    * - ``TAKLER_SECRET_FILE``
      - ``security.operator_secret_file``
      - ``--secret-file``

注意 ``takler-client-py`` **没有** 对应的命令行选项，只能使用前两级
来源。

.. note::

    ``takler-client-py`` 基于 gRPC Python 实现，解析非 IP 主机名失败
    时（报错形如 ``DNS resolution failed`` ）需设置
    ``GRPC_DNS_RESOLVER=native`` ；Go 实现的 ``takler_client`` 不受
    此影响。

child 命令
----------

仅供作业脚本内部使用，把任务状态或属性变化上报给服务。所有 child
命令遵循同一约定：

* ``--node-path`` 省略时取环境变量 ``TAKLER_NAME`` （作业文件中已
  导出，通常无需显式给出）
* 上报凭据从环境变量 ``TAKLER_PASS`` 读取（启用鉴权时服务端校验，
  见 :doc:`/guide/job-management` 与 :doc:`/operation/security` ）
* 环境变量 ``NO_TAKLER`` 已设置时立即成功返回，不发起任何网络
  请求——这让同一份脚本可以脱离服务单独调试（见
  :doc:`/guide/task-script` ）

``init``
~~~~~~~~

把任务标记为 ``active`` ，通常由 ``head.takler`` 调用。

.. list-table::
    :widths: 24 76

    * - ``--task-id``
      - 本次运行的作业标识，作业脚本中取 ``$TAKLER_RID`` （必填）
    * - ``--node-path``
      - 节点路径，缺省取 ``TAKLER_NAME``
    * - ``--host`` / ``--port``
      - 服务地址，按「地址解析」一节回退

``complete``
~~~~~~~~~~~~

把任务标记为 ``complete`` ，通常由 ``tail.takler`` 调用。选项为
``--node-path`` 、 ``--host`` 、 ``--port`` ，含义同上。

``abort``
~~~~~~~~~

把任务标记为 ``aborted`` 并记录中止原因，通常由 ``head.takler``
安装的错误 trap 调用。在 ``complete`` 的选项之外另有
``--reason`` （中止原因文本，缺省为空串）。

``event``
~~~~~~~~~

把任务上的指定 event 置为 set （触发依赖该 event 的下游节点）。
在 ``complete`` 的选项之外另有 ``--event-name`` （必填）。事件与
meter 的入门示例见 :doc:`/tutorial/events-and-meters` 。

``meter``
~~~~~~~~~

把任务上的指定 meter 更新为给定取值，取值越界会被服务端拒绝
（退出码 ``1`` ）。在 ``complete`` 的选项之外另有
``--meter-name`` 与 ``--meter-value`` （均必填）。

控制命令
--------

控制命令接受一个或多个节点路径作为位置参数，多数要求节点所属的
flow 已经 ``begin`` 。

``requeue``
~~~~~~~~~~~

::

    takler-client-py requeue [OPTIONS] NODE_PATH...

把节点重置回排队状态（ ``default_node_status`` ，缺省为
``queued`` ），容器与 flow 还会级联重置整棵子树：子孙任务重新
排队、事件重置回 clear 、 meter 归零、 ``try_no`` 清零、 repeat
回到起始值。仅选项 ``--host`` / ``--port`` 。对尚未 ``begin`` 的
flow 执行会被拒绝（退出码 ``1`` ）。

``suspend`` / ``resume``
~~~~~~~~~~~~~~~~~~~~~~~~

::

    takler-client-py suspend [OPTIONS] NODE_PATH...
    takler-client-py resume  [OPTIONS] NODE_PATH...

挂起节点及其整棵子树（不再创建新作业，已在运行的作业不受影响），
或解除挂起。仅选项 ``--host`` / ``--port`` 。

``run``
~~~~~~~

::

    takler-client-py run [--force / --no-force] NODE_PATH...

立即运行任务节点，无视其依赖是否满足；任务已处于 ``submitted``
或 ``active`` 时不重复启动，除非加 ``--force`` 强制再运行一次。

``force``
~~~~~~~~~

**仅 ``takler-client-py`` 提供。**

::

    takler-client-py force [--recursive / --no-recursive] STATE VARIABLE_PATH...

无视节点当前状态，直接把它置为 ``STATE`` （ ``queued`` 、
``submitted`` 、 ``active`` 、 ``complete`` 、 ``aborted`` 、
``unknown`` 之一）。 ``--recursive`` （缺省开）把同一状态下沉到全部
子孙节点。这是修复卡死工作流的兜底手段，请谨慎使用。

``free-dep``
~~~~~~~~~~~~

**仅 ``takler-client-py`` 提供。**

::

    takler-client-py free-dep --dep-type {all,time,trigger} NODE_PATH...

解除节点的依赖： ``time`` 解除时间依赖， ``trigger`` 解除触发器依
赖， ``all`` 两者皆解除。 ``--dep-type`` 请**显式给出**——省略时
客户端发出的默认值不合法，服务端会以 unsupported value 拒绝
（退出码 ``1`` ）。

``load``
~~~~~~~~

**仅 ``takler-client-py`` 提供。**

::

    takler-client-py load [--flow-type json] FLOW_FILE_PATH

把文件中的 flow 定义加载进服务的 bunch 。目前 ``--flow-type``
仅支持 ``json`` （ :py:meth:`Flow.to_dict <takler.core.Flow.to_dict>`
序列化格式）。

``begin``
~~~~~~~~~

**仅 ``takler-client-py`` 提供。**

::

    takler-client-py begin [--force] [FLOW_NAME]

启动 flow 的日历时钟并重置节点树，工作流由此开始运转。省略
``FLOW_NAME`` 表示 begin 全部 flow ；对已 begin 的 flow 再次执行
需要 ``--force`` 。

查询命令
--------

``show``
~~~~~~~~

::

    takler-client-py show [--show-trigger / --no-show-trigger]
                          [--show-parameter / --no-show-parameter]
                          [--show-limit / --no-show-limit]
                          [--show-event / --no-show-event]
                          [--show-meter / --no-show-meter]
                          [--show-all / --no-show-all]

以文本树形式打印 bunch 的全部节点。 limit 、 event 、 meter 缺省
显示， trigger 与 parameter 缺省不显示； ``--show-all`` 等价于全部
打开（覆盖其余开关）。树中各属性的含义见
:doc:`/guide/attributes/index` 。

``ping``
~~~~~~~~

检查服务可达性并打印往返耗时，退出码遵循上表——服务不可达时退出
码为 ``4`` ，适合作为健康检查接入监控。

``coroutine``
~~~~~~~~~~~~~

**仅 ``takler-client-py`` 提供。** 打印服务进程当前的协程列表，供
调试使用。

两个客户端的差异
----------------

==================  ===================================  ===================================
                    ``takler_client`` (Go)               ``takler-client-py`` (Python)
==================  ===================================  ===================================
child 命令          init / complete / abort /            相同
                    event / meter
控制命令            requeue / suspend / resume / run     另有 force / free-dep / load /
                                                         begin
查询命令            show / ping                          另有 coroutine
TLS / 鉴权选项      全部子命令接受 ``--tls-ca`` /        无命令行选项，只用环境变量与
                    ``--tls-server-name`` /              ``connect.yaml``
                    ``--secret-file``
安装方式            单独构建的 Go 二进制                 随 ``pip install takler`` 安装
==================  ===================================  ===================================

协议语义、地址解析、退出码、 ``TAKLER_NAME`` / ``TAKLER_PASS`` /
``NO_TAKLER`` 约定在两者间完全一致，作业脚本写成哪种都能运行。
