架构总览
========

本页给新贡献者一张全局地图： takler 由哪几个进程组成、代码按什么方向
分层、服务端进程内部如何组装，以及一次作业从提交到 ``complete`` 走过
的完整路径。 ``core`` 包内部的设计决策单独在 :doc:`core-design` 展开。

进程视图
--------

一次典型的部署里有三类进程：

.. mermaid::

    flowchart LR
        subgraph clients["客户端进程（任意多台机器）"]
            CLI["takler-client-py<br/>（Python CLI ）"]
            TUI["takler-tui"]
            GO["takler_client<br/>（Go 客户端）"]
        end
        subgraph server["服务端进程（takler-server ）"]
            GRPC["GrpcTransport<br/>（gRPC 服务）"]
            HTTP["HttpTransport<br/>（HTTP 服务， M3 可选）"]
            SCHED["Scheduler<br/>（调度主循环）"]
            CKPT["CheckpointManager<br/>（周期快照）"]
        end
        JOB["作业进程<br/>/bin/sh -c 派生"]

        CLI -- "gRPC / HTTP" --> GRPC
        CLI -- "gRPC / HTTP" --> HTTP
        TUI -- "gRPC / HTTP" --> GRPC
        TUI -- "gRPC / HTTP" --> HTTP
        GO -- "gRPC / HTTP" --> GRPC
        GO -- "gRPC / HTTP" --> HTTP
        GRPC --> SCHED
        HTTP --> SCHED
        SCHED -- "派生子进程" --> JOB
        JOB -- "child 命令<br/>（gRPC / HTTP 上报）" --> GRPC
        JOB -- "child 命令<br/>（gRPC / HTTP 上报）" --> HTTP
        CKPT -.->|"读写快照文件"| DISK[("takler.check")]

* **服务端进程** 是唯一持有节点树真源的进程。同一个 ``asyncio`` 事件
  循环里跑着几个服务： gRPC 服务（ ``GrpcTransport`` ）响应请求——配
  置 ``connect.yaml`` 的 ``server.http`` 小节后（ M3 ，需
  ``takler[http]`` extra ）， HTTP 服务（ ``HttpTransport`` ）在独立
  端口响应同一批命令，两个 transport 共用调度器、鉴权判定层与审计日
  志； ``Scheduler`` 主循环默认每 ``10`` 秒推进一次依赖解析、
  ``CheckpointManager`` 周期性把节点树写成快照文件。
* **客户端进程** 是无状态的命令行与界面：把运维命令（ ``requeue`` /
  ``suspend`` / ``show`` 等）翻译成 RPC 发出去，打印响应后退出
  （ TUI 则持续轮询 ``show`` ）。三个客户端共享同一份 proto 契约，
  见 :doc:`/guide/cli` 。Python 客户端的命令面之下是 transport 抽象
  （ ``ClientTransport`` ）：默认 gRPC ，选择 ``http`` （
  ``server.transport`` / ``TAKLER_TRANSPORT`` ）后同一套命令改走
  HTTP 服务端口，TUI 自身不感知协议（ M3 任务 8 ）。
* **作业进程** 由服务端用 ``/bin/sh -c`` 派生，是服务端机器上的子进程
  。它与服务端的唯一联系是脚本里 ``head.takler`` / ``tail.takler``
  调用的 child 命令（ ``init`` / ``complete`` / ``abort`` /
  ``event`` / ``meter`` ）—— 这些命令本身就是一次 takler-client-py
  调用，带着渲染时注入的地址（ ``TAKLER_HOST`` / ``TAKLER_PORT`` ）与
  一次性作业口令（ ``TAKLER_PASS`` ）回连服务端（这些变量的生成规则
  见 :doc:`/guide/variables` ）。服务地址与口令在
  **渲染时写死** 进作业文件，这是「作业在跑但状态不动」一类运维问题的
  根源（见 :doc:`/operation/troubleshooting` ）。

包分层
------

代码按职责分层；网络和调度逻辑不进入 core。节点序列化入口局部委托给
``takler.serialization`` 注册表/codec，后者负责组装 core 与内建任务类型；
``takler.schema`` 不导入执行对象：

.. mermaid::

    flowchart TD
        TUI["takler.tui<br/>终端界面"]
        CLIENT["takler.client<br/>CLI 与服务调用封装"]
        SERVER["takler.server<br/>服务组装 / 调度 / RPC / 快照 / 安全"]
        TASKS["takler.tasks<br/>任务类型实现（shell ）"]
        CORE["takler.core<br/>节点树 / 状态 / 表达式 / 序列化"]
        BASE["takler.exceptions · takler.constant<br/>takler.visitor · takler.logging"]

        TUI --> CLIENT
        TUI --> CORE
        CLIENT --> CORE
        CLIENT -.->|"复用 stub 与配置模型"| PROTO["takler.server.protocol<br/>takler.server.connect_config"]
        SERVER --> CORE
        SERVER --> PROTO
        TASKS --> CORE
        CORE --> BASE
        TASKS --> BASE
        SERVER --> BASE
        CLIENT --> BASE

各层的职责边界：

.. list-table::
    :header-rows: 1
    :widths: 22 78

    * - 包
      - 职责与边界
    * - ``takler.core``
      - 领域模型： ``Bunch`` / ``Flow`` / ``NodeContainer`` / ``Task``
        的节点树，状态与传播，触发器表达式，限额、事件、标尺、 repeat 、
        时间依赖， ``to_dict`` / ``from_dict`` 序列化。**不知道** 网络、
        进程与文件提交的存在 —— 它能脱离服务端单独实例化与驱动（单元测试
        与 TUI 的 ``show`` 解析就是这么用的）。设计细节见
        :doc:`core-design` 。
    * - ``takler.tasks``
      - ``Task`` 的具体实现，目前只有
        :py:class:`~takler.tasks.shell.ShellScriptTask` ：把
        「运行任务」落实为渲染脚本、写作业文件、派生 ``/bin/sh -c``
        子进程。受信任的内建注册表显式导入 ``ShellScriptTask``，
        文档中的类型 ID 不触发模块导入（见 :doc:`core-design`）。
    * - ``takler.server``
      - 服务端的一切： ``TaklerServer`` 组装与生命周期、 ``Scheduler``
        主循环与全部 ``run_command_*`` 操作、 ``GrpcTransport`` 的
        gRPC 适配与监听生命周期、 ``HttpTransport`` 的 HTTP 适配与
        uvicorn 生命周期（ M3 ， ``takler[http]`` ）、
        ``CommandHandlers`` 的传输中立命令处理
        （异常边界、 error_code 映射、控制命令审计）、 ``ServerTransport``
        挂载点抽象、快照、鉴权（ ``AuthGate`` 判定层 + gRPC
        ``AuthInterceptor`` / HTTP 依赖注入两处适配）、 zombie 判定、审计、
        ``connect.yaml`` 模型。 ``server.protocol`` 子包放 proto 生成的
        stub 与 pb2 ↔ DTO 编解码。
    * - ``takler.client``
      - Python 客户端： Typer 命令行、 ``TaklerServiceClient`` 的
        channel / 重试 / 凭据装配、退出码与 stderr 契约。它 **复用**
        ``takler.server.protocol`` 的 stub 与
        ``takler.server.connect_config`` 的配置模型 —— 协议与配置文件
        的 schema 在仓库里只有一份，所有客户端共享同一契约。
        ``show`` 的输出在客户端本地反序列化成 ``Bunch`` 再排版打印，
        所以客户端也依赖 ``takler.core`` 与 ``takler.visitor`` 。
    * - ``takler.tui``
      - 终端界面，建立在 ``takler.client.service_client`` 之上；轮询
        ``show`` 并把响应解析回 ``core`` 的节点树来渲染。
    * - 基础层
      - ``takler.exceptions`` 的异常体系（ ``error_code`` 映射的输入
        ，见 :doc:`/operation/reference` ）、 ``takler.constant`` 的
        默认地址、 ``takler.visitor`` 的树遍历工具、
        ``takler.logging`` 的日志子系统。它们不 import 任何 takler
        上层包（ ``visitor`` 只依赖 ``core.node`` ）。

服务端进程的组装
----------------

``takler-server`` 的入口（ ``takler.server.cli`` ）刻意做得很薄：解析
命令行与 ``connect.yaml`` ，构建 ``TaklerServer`` 后交给
``asyncio.run`` 。所有装配都在 ``TaklerServer.__init__`` 里完成 ——
``AuditLogger`` 、 ``AuthInterceptor`` 、 ``ZombieDetector`` 、
``Scheduler`` 、 ``GrpcTransport`` 、 ``CheckpointManager`` 与唯一的
``Bunch`` ：

.. mermaid::

    flowchart TD
        TS["TaklerServer"]
        TS --> AL["AuditLogger<br/>审计记录"]
        TS --> AI["AuthInterceptor<br/>gRPC 鉴权拦截器"]
        TS --> ZD["ZombieDetector<br/>Z1 / Z2 / Z3 判定"]
        TS --> SCHED["Scheduler<br/>主循环 + run_command_*"]
        TS --> SVC["GrpcTransport<br/>RPC 处理器"]
        TS --> CM["CheckpointManager<br/>恢复 + 周期快照"]
        ZD --> SCHED
        SCHED --> BUNCH[("Bunch<br/>（core 节点树）")]
        SVC --> SCHED

启动顺序是契约（ ``TaklerServer.start`` ）：配置日志 → 检查 umask
（必须在任何线程 / 任务出现之前） → 校验安全配置（不可用即启动失败，
见 :doc:`/operation/security` ） → 从快照恢复节点树（必须在主循环
存在之前，见 :doc:`/operation/checkpoint` ） → 依次启动调度器、
gRPC 服务、周期快照任务。

两条贯穿运行期的主线：

* **RPC 主线** ： gRPC transport ``GrpcTransport`` 只做 pb2 ↔ DTO 转换
  （ ``takler.server.protocol.adapter`` ），命令本身交给传输中立的
  ``CommandHandlers`` （ ``takler.server.handlers`` ）：每个命令都把
  调用 ``Scheduler`` 的那段代码包在 ``CommandHandlers._handle_command``
  里 —— 这是命令的异常边界：正常路径原样返回；异常路径按异常策略
  记日志、转成 ``flag != 0`` 的 ``ServiceResponse`` （ ``flag`` 取值
  见 :doc:`/operation/reference` 的 error_code 表），控制命令另写一
  条审计记录（见 :doc:`/operation/audit` ）。 ``Scheduler`` 的
  ``run_command_*`` 公开接口收发 ``takler.protocol`` 的 DTO。
* **调度主线** ： ``Scheduler.main_loop`` 每轮对每个已开始
  （ ``begun`` ）的 flow 调 ``_process_flow`` —— 先
  ``flow.update_calendar(now)`` 推进日历，再
  ``flow.resolve_dependencies()`` 自顶向下检查依赖并提交满足条件的
  任务。每个 flow 有独立的异常边界：一个 flow 的异常在
  ``resilient`` 策略下只跳过这个 flow （见
  :doc:`/operation/resilience` ）。

一次作业的完整路径
------------------

把两条主线与一个作业进程串起来，就是新贡献者最常问的「一次 job 从
提交到 ``complete`` 都经过了哪」：

.. mermaid::

    sequenceDiagram
        participant OP as 运维客户端
        participant SVC as GrpcTransport
        participant SCH as Scheduler
        participant TASK as Task（core ）
        participant JOB as 作业进程

        OP ->> SVC : load / begin
        SVC ->> SCH : run_command_load / run_command_begin
        SCH ->> TASK : Flow.from_dict 建树 / flow.begin 启动日历
        loop 主循环（默认 10 秒一轮）
            SCH ->> TASK : update_calendar + resolve_dependencies
            Note over TASK : 依赖全部满足 → Task.run()
            TASK ->> TASK : before_run：try_no +1 ，轮换作业口令
            TASK ->> TASK : create_job_script：渲染脚本写作业文件
            TASK ->> JOB : ShellRunner.spawn：/bin/sh -c
            TASK -->> SCH : after_run：状态置为 submitted
        end
        JOB ->> SVC : init（child 命令，带 TAKLER_PASS ）
        SVC ->> SCH : run_command_init（先过 zombie 判定）
        SCH ->> TASK : node.init → active，状态沿树 swim 上溯
        Note over JOB : 作业体运行，event / meter 同理上报
        JOB ->> SVC : complete
        SVC ->> SCH : run_command_complete
        SCH ->> TASK : node.complete → complete，swim 上溯、释放限额令牌

要点：

* **提交发生在主循环里** ，不在任何 RPC 处理器里 —— ``run --force``
  之类的控制命令只是把任务推进 ``run()`` ，依赖驱动的自动提交永远是
  主循环那一轮 ``resolve_dependencies`` 的结果。 ``ShellRunner``
  用 ``/bin/sh -c`` 把作业派生为服务进程的子进程；渲染或进程创建
  失败在这一步直接把任务打成 ``aborted`` （ ``JobSubmissionError``
  ），见 :doc:`/guide/job-management` 。
* **状态推进发生在 child 命令的 RPC 里** ： ``submitted`` →
  ``active`` → ``complete`` / ``aborted`` 全部由作业进程的上报驱动，
  服务端不轮询作业。上报先过 ``ZombieDetector`` 的判定（ Z1 / Z2 /
  Z3 ，见 :doc:`/operation/zombie` ），通过才写节点；写完沿父链
  swim 上溯重算容器状态（传播规则见 :doc:`core-design` ）。
* **快照与此正交** ： ``CheckpointManager`` 按间隔把 ``Bunch`` 序列化
  落盘，不参与上面任何一步；恢复时地址不一致的处理见
  :doc:`/operation/checkpoint` 。

相关页面： 节点树、状态与表达式的包内设计 :doc:`core-design` ；面向
使用者的作业生命周期细节 :doc:`/guide/job-management` ；状态机语义
:doc:`/guide/node-status` 。
