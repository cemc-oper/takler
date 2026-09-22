协议与跨语言契约
================

本页面向客户端实现者： Go 客户端 ``takler_client`` 是参照实现，任何新
语言的客户端都应能与它对齐。

M3 起协议分两层描述：

* **协议模型** （ ``takler/protocol`` 包）是传输中立的：十六个命令各
  有一对请求 / 应答 DTO （ ``commands.py`` ）、一个信封类型
  （ ``envelope.py`` ）与一张 error_code 分类表（ ``error_code.py``
  ）。这一层不 import ``grpc`` 、 ``takler_pb2`` 或
  ``takler.server`` / ``takler.client`` 下的任何模块，因此服务端与
  两个客户端可以共用同一份模型而不拖入任何 transport 。
* **transport** 是模型的一种线上编码，现有两种： gRPC （默认，随
  ``takler`` 主包安装）与 HTTP （ ``takler[http]`` extra ）。两个
  transport 承载**同一套命令面与同一套语义**——超时、重试、退出码
  与凭据完全一致，差别只在线上字节长什么样。

gRPC 编码的唯一权威来源仍是
``src/takler/server/protocol/takler.proto`` ；本页解释字段背后的语义
与两边的常量契约，不逐字段抄 proto 定义。服务端如何把命令接进节点树
见 :doc:`architecture` 与 :doc:`core-design` 。

命令面
------

全部命令共十六个：五个 child 命令、八个控制命令、三个查询命令。每个
命令在协议模型里有一个名字（ ``Command`` 枚举，取值就是两个 CLI 使
用的命令词），在 gRPC 上对应一个 RPC 方法，在 HTTP 上对应
``POST /v1/commands/{command}`` 路径的一段。三种命名的一一对应关系
由 ``server/handlers.py`` 的 ``METHOD_NAME_BY_COMMAND`` 钉住：

.. list-table::
    :header-rows: 1
    :widths: 14 24 26 36

    * - 命令名
      - gRPC 方法
      - 请求 → 应答
      - 用途
    * - ``init``
      - ``RunCommandInit``
      - ``InitCommand`` → ``ServiceResponse``
      - 作业启动上报； ``task_id`` 写入 ``TAKLER_RID``
    * - ``complete``
      - ``RunCommandComplete``
      - ``CompleteCommand`` → ``ServiceResponse``
      - 作业正常结束
    * - ``abort``
      - ``RunCommandAbort``
      - ``AbortCommand`` → ``ServiceResponse``
      - 作业失败； ``reason`` 记入 ``aborted_reason``
    * - ``event``
      - ``RunCommandEvent``
      - ``EventCommand`` → ``ServiceResponse``
      - 置位事件（只能 set ，清除见下方 force ）
    * - ``meter``
      - ``RunCommandMeter``
      - ``MeterCommand`` → ``ServiceResponse``
      - 更新标尺； ``meter_value`` 线上是 **字符串**
    * - ``requeue``
      - ``RunCommandRequeue``
      - ``RequeueCommand`` → ``ServiceResponse``
      - 重排队
    * - ``suspend`` / ``resume``
      - ``RunCommandSuspend`` / ``RunCommandResume``
      - ``SuspendCommand`` / ``ResumeCommand`` → ``ServiceResponse``
      - 挂起 / 恢复（ M3 起各有独立的消息类型，不再共用）
    * - ``run``
      - ``RunCommandRun``
      - ``RunCommand`` → ``ServiceResponse``
      - 手动提交任务； ``force`` 跳过状态检查
    * - ``force``
      - ``RunCommandForce``
      - ``ForceCommand`` → ``ServiceResponse``
      - 强制置节点状态或置 / 清事件
    * - ``free-dep``
      - ``RunCommandFreeDep``
      - ``FreeDepCommand`` → ``ServiceResponse``
      - 手动释放依赖
    * - ``load``
      - ``RunCommandLoad``
      - ``LoadCommand`` → ``ServiceResponse``
      - 载入 flow 定义
    * - ``begin``
      - ``RunCommandBegin``
      - ``BeginCommand`` → ``ServiceResponse``
      - 启动日历
    * - ``show``
      - ``RunRequestShow``
      - ``ShowRequest`` → ``ShowResponse``
      - 查询节点树
    * - ``ping``
      - ``RunRequestPing``
      - ``PingRequest`` → ``PingResponse``
      - 健康检查（唯一免凭据的方法）
    * - ``coroutine``
      - ``QueryCoroutine``
      - ``CoroutineRequest`` → ``CoroutineResponse``
      - 列出服务端事件循环上的协程

gRPC 上全部是 unary-unary 调用（一次请求一次应答，没有流式）。

请求 DTO 里需要实现者注意的语义：

* 五个 child 命令共用基类 ``ChildCommand`` 的 ``node_path`` 定位目标
  节点（ gRPC 编码里这层是 ``ChildCommandOptions`` 包装消息， DTO 层
  把它摊平进各命令）。 ``event`` 只能置位；清除事件走 ``force`` ，把
  ``path`` 写成 ``节点路径:事件名`` 、 ``state`` 用 ``clear``
  （ ``set`` 亦可）。
* ``MeterCommand.meter_value`` 线上是字符串、离开 DTO 时是 ``int``
  —— ``int()`` 转换从调度器移进了 DTO 校验。一个非整数的字符串以
  ``internal_error`` 收场（ ``int()`` 抛出的 ``ValueError`` 不是
  takler 异常）。客户端**不在本地校验**请求：请求以普通 dict 按 DTO
  字段名过线，校验是服务端的职责，这样两种客户端发同一个坏值会得到
  同一个 ``flag`` 。
* ``ForceCommand.path`` 接受节点路径与 ``节点:事件`` 两种形式；
  ``recursive`` 只对节点有意义（语义见 :doc:`/guide/node-status` 的
  sink 一节）。
* ``LoadCommand.flow`` 在 gRPC 编码里是 ``bytes`` ，内容为
  :py:class:`~takler.core.Flow` 的 ``to_dict`` JSON ； HTTP 信封里
  同一字段按 JSON 模式的 pydantic 序列化规则走 **base64 字符串**。
  ``flow_type`` 目前只接受 ``"json"`` ，其他取值与坏 JSON 分别以
  ``unsupported_value`` / ``invalid_request`` 拒绝。载入按 Tree 模
  式恢复：状态归零、未 begun （见 :doc:`core-design` 的序列化一节）
  。
* ``BeginCommand.flow_name`` 为空串表示对 Bunch 里全部 flow 执行
  begin —— 空串就是线上形式，不是省略字段。
* ``ShowResponse.output`` 是服务端排版好的文本，客户端直接打印，不再
  解析。
* DTO 的字段默认值对齐 CLI 的表面而不是 proto3 零值：
  ``ForceCommand.recursive`` 默认 ``True`` （两个 CLI 的
  ``--recursive`` 默认开）， ``show`` 的三个展示开关默认 ``True``
  。省略字段的请求得到的是运维在 CLI 上不加该选项时的取值。

DTO 基类 ``ProtocolModel`` 一律 ``extra="forbid"`` ：拼错或多出的字
段是校验错误，不是静默丢弃。两端同批发布时这是正确的默认；哪天要支
持混合版本部署，需要重新评估这一条。

信封 Envelope
-------------

HTTP 编码的线上形式是信封 JSON
（ :py:class:`~takler.protocol.envelope.Envelope` ），请求与响应同
一个类型：

.. list-table::
    :header-rows: 1
    :widths: 18 82

    * - 字段
      - 语义
    * - ``version``
      - 信封格式版本，当前固定 ``"1"`` 。存在是为了让将来的格式变更
        能被识别，而不是以难解的方式失败；本阶段不做版本协商
    * - ``trace_id``
      - 每条请求生成的关联标识， 32 位小写十六进制、无连字符。贯穿
        客户端、transport 与服务端日志；响应信封**回显**请求的
        ``trace_id``
    * - ``target``
      - 为路线图中的代理路由预留，本阶段从不设置
    * - ``auth``
      - 凭据进入消息体时的载体（三个字段名与 gRPC metadata 键对应：
        ``job_password`` / ``secret`` / ``user`` ）。 gRPC 与 HTTP
        下凭据都走 metadata / 请求头而不走信封，该字段为将来的代理
        形态预留
    * - ``command``
      - ``Command`` 枚举，必须是十六个已知命令之一——未知命令名在
        信封校验时就失败，不会漏到下游
    * - ``payload``
      - 请求或应答 DTO 以 ``model_dump(mode="json")`` 序列化后的
        dict

gRPC transport **从不序列化信封** ：那里由 RPC 方法名扮演
``command`` 、 pb2 消息扮演 ``payload`` 。信封是 HTTP 的线上形式，
同时也是两种 transport 下 ``trace_id`` 统一生成与读取的地方。

HTTP transport
--------------

HTTP 服务（ FastAPI + uvicorn ， ``takler[http]`` extra ）只有一个端
点 ``POST /v1/commands/{command}`` ，请求与响应体都是信封 JSON ；
自带的 OpenAPI 文档（ ``/docs`` ）兼作 transport 的自我描述。线上
规则：

* URL 里的 ``{command}`` 与信封内的 ``command`` 必须一致，不一致以
  ``400`` 拒绝。
* **HTTP 200 不代表业务成功** 。状态码只表达 transport 层与鉴权结
  果： ``401`` / ``403`` 是鉴权拒绝（对应 gRPC 的
  ``UNAUTHENTICATED`` / ``PERMISSION_DENIED`` ）， ``422`` 是命令名
  或信封不合法（ FastAPI 的参数校验，含 ``extra="forbid"`` 拒绝未知
  字段）， ``400`` 是 URL 与信封命令不一致。命令本身的成败永远在
  ``200`` 响应信封的 ``payload.flag`` 里——一个请求 DTO 校验失败
  （如非整数的 ``meter_value`` ）也是 ``200`` 加非零 ``flag`` ，与
  gRPC 上同一个坏请求的分类完全一致。
* 鉴权判定发生在看信封**之前**（ FastAPI 的依赖注入先于请求体校验
  ）：未通过鉴权的调用方甚至无法让服务端解析信封，被拒绝的请求不可
  能改变任何节点状态。
* 响应信封的 ``trace_id`` 回显请求的 ``trace_id`` ；服务端日志里同
  一个 ``trace_id`` 把一次调用的各环节串起来。

客户端对 HTTP 失败的分类（ ``client/http_transport.py`` 的
``classify_http_error`` ）与 gRPC 的状态码映射一一对应：

* 可重试：状态码 ``408`` / ``429`` / ``500`` / ``502`` / ``503`` /
  ``504`` ，以及 httpx 的连接类错误；
* 不重试、直接映射为异常： ``400`` / ``422`` →
  ``InvalidRequestError`` （退出码 ``1`` ）， ``401`` / ``403`` →
  ``PermissionDeniedError`` （退出码 ``1`` ）；
* 其余状态码与其余 httpx transport 错误是 FATAL （不重试，退出码
  ``4`` ）。

``ServiceResponse`` 与 error_code
---------------------------------

除三个查询命令外，所有命令的应答都是
``ServiceResponse{flag, message}`` ：

* ``flag`` 是错误分类码（ Error_Code ）： ``0`` 表示成功，非 ``0``
  表示失败并标识分类。 ``message`` 形如 ``{异常类名}: {异常消息}``
  。
* **gRPC 消息的字段形状是冻结的** ： ``flag`` 保持 ``int32`` 、字
  段号 ``1`` ， ``message`` 保持 ``string`` 、字段号 ``2`` ，不允
  许增删字段。分类码是复用既有 ``flag`` 字段而不是新增字段引入的，
  只判断 ``flag != 0`` 的老客户端因此永远兼容。
  ``tests/server/test_proto_contract.py`` 同时钉住 descriptor 的形
  状与 proto 注释里指向分类码表的说明。
* 分类码表在 ``takler/protocol/error_code.py`` ，不是 proto 枚举
  —— 这个模块只依赖 ``takler.exceptions`` ，客户端不用拉入
  protobuf 生成代码或服务端包就能完成映射。完整的十六码表（码值、
  分类名、异常类、客户端退出码）见 :doc:`/operation/reference` ，
  这里只说查找规则：按 **确切异常类型** 查表，不沿继承链；没有专
  属码的 takler 异常归 ``1`` （ ``takler_error`` ），非 takler 异
  常归 ``99`` （ ``internal_error`` ）；客户端遇到表中不存在的码，
  分类名读作 ``unknown`` ，退出码取最保守的 ``3`` 。
* 分类名按码值分组： ``0`` 是 ``success`` ； ``10`` ~ ``15`` 是请
  求与节点树（ ``node_not_found`` 、 ``invalid_node_path`` 、
  ``node_type`` 、 ``unsupported_value`` 、 ``flow_state`` 、
  ``invalid_request`` ）； ``20`` 是 ``expression_syntax`` ；
  ``30`` / ``31`` 是 ``job_submission`` 与 ``zombie`` ； ``40`` ~
  ``43`` 是传输与鉴权（ ``transport`` 、 ``client_connection`` 、
  ``server_response`` 、 ``permission_denied`` ）—— 其中 ``40`` /
  ``41`` / ``42`` 只由客户端本地抛出，永远不会出现在服务端返回的
  ``flag`` 里； ``1`` 与 ``99`` 分别是 ``takler_error`` 与
  ``internal_error`` 两个兜底。
* 三个查询命令没有 ``flag`` 字段，错误复用各自的应答类型： ``show``
  把错误写进 ``output`` （ ``error: {类型}: {消息}`` ）， ``ping``
  与 ``coroutine`` 返回空应答。鉴权拒绝不走应答体，见下节。

凭据与权限分级
--------------

凭据三个键，键名在两种 transport 上相同——gRPC 的 metadata 键即
HTTP 的请求头名（ HTTP 头不区分大小写），都是全小写、不带
``-bin`` 后缀（值是 ASCII 文本， ``-bin`` 会把值声明为二进制）：

.. list-table::
    :header-rows: 1
    :widths: 22 78

    * - 键
      - 语义
    * - ``takler-pass``
      - child 命令携带的一次性作业口令（ ``TAKLER_PASS`` ）。鉴权层只
        检查它在场，值与目标任务的口令是否一致由 ``ZombieDetector``
        的 Z1 判定（见 :doc:`/operation/zombie` ）—— 缺失是「没有凭
        据」，不匹配是「另一个作业实例的凭据」，后者可能按僵尸策略放
        行。
    * - ``takler-secret``
      - 运维命令携带的共享口令，与 ``Operator_Secret_Set`` 做常数时间
        比对（见 :doc:`/operation/security` ）。
    * - ``takler-user``
      - 运维命令携带的 OS 用户名，须命中白名单；同时进入审计记录。

服务端解析规则： 空白值视为未携带；同一键重复出现时 **第一个出现生效**
（ HTTP/2 允许同名头重复，取首个是确定性的、且不受追加方影响）。

两个 transport 共用同一个鉴权判定层（ ``server/auth.py`` 的
``AuthGate`` ）：它只认命令的规范操作名（ gRPC 方法名），因此权限
表、拒绝分类与审计记录不感知请求来自哪个端口。每个命令按
``PRIVILEGE_BY_METHOD`` 归入三级之一，表以全限定方法名
（ ``/takler_protocol.TaklerServer/<方法名>`` ）为键：

* ``CHILD`` —— 五个 child 命令；
* ``OPERATOR`` —— 八个控制命令，外加 ``show`` 与 ``coroutine`` 两
  个只读命令（它们返回整份流程定义，侦察价值与写操作相当）；
* ``PUBLIC`` —— 仅 ``ping`` 。

**表里没有的方法一律按 ``OPERATOR`` 处理** —— 故意 fail-closed ，
新增命令忘了分类的结果是要凭据，而不是一个匿名的写入口。
``tests/server/test_privilege_table_property.py`` 遍历 descriptor 断
言每个已声明方法都有显式条目。 ``auth_mode`` 为 ``disabled`` 时不做
任何检查，但凭据仍被解析并发布给僵尸判定与审计。

鉴权拒绝的应答也是契约的一部分，分类字符串三个：
``missing_credential`` 是「你没带凭据」（ gRPC 状态码
``UNAUTHENTICATED`` / HTTP ``401`` ）， ``invalid_credential`` 与
``not_in_whitelist`` 是「带了但不成立」（ ``PERMISSION_DENIED`` /
``403`` ）。拒绝细节里只有方法名与分类，不含任何凭据值，也不区分
「密钥错了」与「服务端文件读不出」。

枚举： ``ForceState`` 与 ``DepType``
------------------------------------

``ForceCommand.ForceState`` 的八个取值：
``unknown=0`` 、 ``complete=1`` 、 ``queued=2`` 、 ``submitted=3`` 、
``active=4`` 、 ``aborted=5`` 、 ``clear=6`` 、 ``set=7`` 。注意两点：
它的编号 **与** core 的 :py:class:`~takler.core.NodeStatus` 枚举值
（ ``unknown=1`` 到 ``aborted=6`` ） **不一致** ，两套编号不能互转；
末尾的 ``clear`` / ``set`` 不是节点状态，是事件的清除与置位（配合
``节点:事件`` 路径使用）。

``FreeDepCommand.DepType`` 的三个取值： ``all=0`` 、 ``trigger=1`` 、
``time=2`` 。 proto3 下缺省即零值 ``all`` 。

编号只是 gRPC 编码细节：线上与 DTO 层都用枚举 **名字符串** （
``takler.protocol`` 的 ``ForceState`` / ``DepType`` 与 proto 枚举同
名），两个 CLI 接受的词—— ``complete`` 、 ``set`` 、 ``free-dep``
的 ``--type`` 取值——就是这些名字。

超时与重试契约
--------------

以下常量同属跨语言契约，两个客户端在两种 transport 下都必须一致：

* 单次尝试超时 ``10`` 秒；
* 重试窗口： child 命令 ``86400`` 秒（一天，作业可以比服务活得久），
  控制与查询命令 ``60`` 秒；环境变量 ``TAKLER_TIMEOUT`` 覆盖窗口（见
  :doc:`/operation/reference` ）；
* 退避公式 ``min(2**(n-1), 60)`` 秒， ``n`` 为第几次尝试；
* 可重试的 gRPC 状态码： ``UNAVAILABLE`` 、 ``DEADLINE_EXCEEDED`` 、
  ``RESOURCE_EXHAUSTED`` 、 ``UNKNOWN`` ；不可重试：
  ``INVALID_ARGUMENT`` 、 ``NOT_FOUND`` 、 ``PERMISSION_DENIED`` 、
  ``UNAUTHENTICATED`` （请求本身错了，重试不会改变结果）。 HTTP 侧
  的对应分类见上文「 HTTP transport 」一节；
* **业务失败不重试** ： ``flag != 0`` 的应答是正常返回，不是传输故
  障，客户端直接把它交给调用方，既不重试也不抛异常。两种 transport
  跑同一个重试循环（ Python 侧
  ``takler.client.retry.run_with_retry`` ， Go 侧
  ``common/retry.go`` 的 ``RunWithRetry`` ），只有「线上失败 → 重
  试判定」的映射按 transport 各有一份。

重新生成 stub
-------------

Python stub （ ``takler_pb2.py`` 与 ``takler_pb2_grpc.py`` ）在仓库根
目录用 dev 依赖组里的 ``grpcio-tools`` 重新生成：

.. code-block:: console

    $ python -m grpc_tools.protoc -Isrc --python_out=src --grpc_python_out=src \
        takler/server/protocol/takler.proto

``grpcio-tools`` 固定在 ``>=1.83`` 不是随手写的：更低的版本把
protobuf 上限钉在 ``<7`` ，与本项目自己的 ``protobuf>=7.35`` 冲突，生
成物会不可复现。生成的 ``*_pb2*.py`` 被 ruff 与覆盖率同时排除，不要手
工编辑。

Go 侧的生成命令写在 ``takler.proto`` 头部注释里（ ``protoc --go_out``
系列），产物落在 ``takler-client`` 仓库的 ``takler_protocol/`` 目录；
proto 的 ``go_package`` 选项已指向该仓库。

跨语言契约测试
--------------

常量契约靠一对「漂移哨兵」测试维持： Python 半是
``tests/client/test_cross_language_contract.py`` ， Go 半是
``takler-client`` 仓库的 ``common/errorcode_test.go`` 。两边各自 **手
抄** 同一份期望值 —— error_code 十六行、退出码映射、重试常量、状态码
集合（ gRPC 状态码与 HTTP 状态码各一份） —— 刻意不从生产代码的表里
推导：读着自己要监督的表的测试永远发现不了那张表被改错。任何一侧改了
常量而另一侧没跟上，失败的是测试而不是线上协议。

另有两个相关哨兵： ``tests/server/test_proto_contract.py`` 钉住
``ServiceResponse`` 的字段形状与注释；
``tests/server/test_privilege_table_property.py`` 断言权限表覆盖
descriptor 里的全部方法。

线上行为的对齐靠 ``takler-client`` 仓库的契约脚本：
``scripts/http_contract.sh`` 对真实 takler 服务端（同挂 gRPC 与
HTTP ）让 Go 客户端跑全部十六个命令，断言退出码与输出行在两种
transport 下一致（含 ``meter "abc"`` 过线后得 ``internal_error`` 这
类边界）； CI 的 ``http-contract`` job 检出 takler 仓启动服务端后
跑同一脚本。

新增一个命令要同步改的地方
--------------------------

以新增一个控制命令为例，清单按依赖顺序：

#. ``server/protocol/takler.proto`` ：定义请求消息与 rpc 。
   ``ServiceResponse`` 的两个字段保持不动。
#. 重新生成 Python stub （上节命令）与 Go stub （ ``takler-client`` 仓
   库）。
#. ``protocol/commands.py`` ： ``Command`` 枚举加命令名（ CLI 词，即
   HTTP 路径段），定义请求与应答 DTO ，并在
   ``REQUEST_TYPE_BY_COMMAND`` / ``RESPONSE_TYPE_BY_COMMAND`` 两张注
   册表登记。 HTTP transport 的端点、信封校验与 OpenAPI 文档由这两张
   表驱动，**不需要** 再动 ``server/http_transport.py`` 。
#. ``server/auth.py`` 的 ``PRIVILEGE_BY_METHOD`` 加一条显式分类。不加
   也会安全地回落到 ``OPERATOR`` ，但完备性测试会失败。
#. ``server/handlers.py`` ： ``METHOD_NAME_BY_COMMAND`` 加命令名到
   gRPC 方法名的映射， ``CommandHandlers`` 加命令实现，
   ``_REQUEST_INFO_BY_COMMAND`` 加日志摘要（异常边界与 error_code
   映射由 ``_handle_command`` 统一承担，见 :doc:`architecture` 的
   RPC 主线）。 ``OPERATOR`` 级的写操作从被分类起就自动进审计；只读
   方法要列进 ``_READ_ONLY_OPERATOR_METHODS`` 才不会被记审计。
#. ``server/protocol/adapter.py`` 加 pb2 ↔ DTO 转换，
   ``server/grpc_transport.py`` 的 ``GrpcTransport`` 加一行分派方法。
#. ``server/scheduler.py`` 加对应的 ``run_command_*`` 。
#. ``client/service_client.py`` 加客户端方法， ``client/cli.py`` 加命
   令； Go 侧对应改 ``common/client_*.go`` 与 ``cmd/cmd_*.go`` 。两
   个客户端的 gRPC 与 HTTP transport 都从各自的命令面统一编址，新命
   令不需要动 transport 实现本身。
#. 只有引入了新的异常类型才动 ``protocol/error_code.py`` ，并
   同步 Go 侧 ``common/errorcode.go`` 与两边的契约测试。
#. 文档： :doc:`/guide/cli` 的命令表，动了 error_code 或环境变量时连
   带 :doc:`/operation/reference` 。

协议本身的错误形态（退出码、stderr 契约、重试表现）以
:doc:`/guide/cli` 与 :doc:`/operation/reference` 为准。
