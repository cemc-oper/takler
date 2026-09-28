纯定义文档
==========

``takler.schema`` 提供不依赖 core、server 或 client 执行对象的
``DefinitionDocument``。文档必需包含 ``kind="takler.definition"``、
整数 ``schema_version=1`` 和单个 Flow 或 Bunch ``root``。
每个节点必需包含稳定的 ``type_id`` 和 ``name``。

导出已有内存树：

.. code-block:: python

   from takler.core import Flow
   from takler.serialization import export_definition
   from takler.schema import parse_definition

   flow = Flow("forecast")
   flow.add_task("prepare")
   document = export_definition(flow)
   text = document.model_dump_json(indent=2)
   validated = parse_definition(text)

``export_definition`` 返回模型；``model_dump(mode="json")`` 返回字典。
文件读写由调用方负责，导出不读取脚本、不执行任务、不 begin、不修改
日历、依赖缓存或 limit 占用。不要用此文档恢复运行进度。

支持的内建节点标识为 ``takler.bunch``、``takler.flow``、
``takler.container``、``takler.task``、``takler.shell``。
导出按精确 Python 类型匹配；未知子类和未经注册的 ``@task`` 局部类型
明确失败，不会被降成普通 Task。内建 ``type_data`` 只能为空对象。
受信任扩展注册通过 ``NodeRegistration`` 完成，见 :doc:`extending`。

定义保留用户参数（包括 null）、默认 queued/complete 状态、触发器文本、
事件 initial_value、meter 上下界、limit 容量、in-limit 引用与 tokens、
repeat 日期范围及步长、HH:MM 时间、shell script_path。
事件当前值、meter 当前进度、占用计数、repeat 当前日期、free 标记、
任务身份/尝试次数/口令、当前状态和 Flow 日历均不进入文档。
Bunch 只导出 name、user_parameters、flows；根上配置未支持的调度属性
会报 ``unsupported_root_attribute``。

服务器生成的地址和默认参数不导出；显式用户 TAKLER_HOME 保留。
用户参数中的 TAKLER_HOST、TAKLER_PORT、TAKLER_PASS、TAKLER_SECRET
（不区分大小写）被拒绝。其他业务参数按原值导出，文档应按配置数据保护。

``parse_definition`` 接受 JSON 文本、字节或字典，拒绝未知字段/版本、
类型转换、重复 JSON 键、NaN/Infinity、尾随 JSON、重复名称与非法树形。
错误为 ``DefinitionError``，其 ``code`` 和 ``document_path`` 可供调用方
记录；错误文本不包含原始参数值。直接使用 Pydantic 校验接口的调用方
不要记录完整 ValidationError 或输入对象。

字段模型只校验纯数据。``build_definition(document, registry=..., existing_bunch=...)``
构造隔离树，校验触发器语法与引用、limit 引用及容量；返回未 begin 的 Flow/Bunch。
``existing_bunch`` 只作为跨 flow 引用的只读上下文，不把候选 Flow 加入在线树，
也不更改在线缓存或占用。构造失败不会替换在线节点。

现有 ``to_dict`` / ``from_dict`` 改用受信任 ``type_id``；checkpoint 格式为 2，
完整校验后才恢复。网络 load 只接受版本 1 的单个 Flow 纯定义；Bunch 根、
旧混合格式、运行字段和未知类型均拒绝（invalid_request，flag=15）。
同名 flow 已存在时拒绝（flow_state，flag=14），保留原树及运行进度。
加载成功不自动 begin；显式执行 begin 后才参与调度。查询只读化留给 R0-15。
不提供旧 module/class 别名或历史格式转换器。

服务端 flow 替换
-----------------------

``Scheduler.run_command_replace(ReplaceCommand(target_path, flow_bytes))``
提供单个已有 Flow 的内存替换。``target_path`` 必须为规范绝对路径，
新定义必须是同名 Flow，不能隐式新增、重命名或传入 Bunch。
此服务端接口已实现；网络协议、CLI 命令、operator 鉴权及审计接入由后续
集成任务提供，当前不能通过客户端调用 replace。

服务端在隔离树上 begin，然后准备跨 flow trigger、complete-trigger 和
in-limit 的引用更新。最终提交前重新检查目标身份，以及旧 flow 自身和
每个后代的真实状态：存在 active/submitted 即拒绝，暂停不豁免。
旧树 limit 仍有占用，或外部 limit 仍记录旧树路径的占用时，也拒绝替换。
新定义删除仍被引用的节点、变量或 limit，或使 in-limit tokens 超过容量时，
返回定义错误，在线树和缓存均不修改。

成功后新 flow 已 begun，保留提交时旧 flow 自身的 suspended；后代暂停、
运行身份、repeat/event/meter 进度均不迁移，按新定义和 begin 规则初始化。
根 Bunch 与部署参数保留；外部消费者切换到新对象，旧提交回调失去在线归属。
重复执行会再次初始化，不提供幂等去重。

成功响应为 ``flow replaced in memory; checkpoint pending``，仅表示内存生效。
replace 不同步写 checkpoint；后续周期保存新树，保存前崩溃仍可能丢失替换。

准备与提交都运行在 scheduler 的单写者上下文中。最终状态检查到内存换入、
缓存更新之间不 await，不调用扩展回调或 I/O。扩展构造和初始化须遵守
受信任注册器的无副作用约定；builder 的 ``initialize`` 回调在连接在线引用
之前执行，不得提交作业、访问文件或修改在线对象。

服务端错误分类：目标不存在 10，路径非法 11，目标非 Flow 12，不支持的
类型/版本 13，运行状态/身份变化/占用冲突 14，定义/名称/引用非法 15，
表达式语法错误 20，非预期构造/初始化失败 99。错误消息不含定义参数值。
