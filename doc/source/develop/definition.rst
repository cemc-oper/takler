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
受信任扩展注册及统一 builder 属于后续 R0-11。

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

字段模型只校验纯数据；表达式语法、limit 引用解析及隔离构建留给 builder。
现有 ``to_dict``、checkpoint、show 和网络 load 尚未切换到此新入口；
R0-11/12 将分别接入构建与 load。新定义格式不提供旧混合格式转换。
