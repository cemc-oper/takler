初始查询与分页
==============

``show`` 和 TUI 使用 QueryDocument v1 读取工作流状态。Python
``takler-client-py``、Go ``takler_client`` 在 HTTP 和 gRPC 上使用同一
字段与错误语义。两者默认读取整个 bunch；大树建议先选定范围。

.. code-block:: console

   takler-client-py show --flow forecast --depth 2
   takler_client show --scope /forecast/prepare
   takler-client-py show --scope /forecast/prepare/task1 --show-parameter --show-trigger
   takler-client-py sync --flow forecast --depth 2 --polls 5 --interval 1
   takler-tui --scope /forecast

``--scope`` 是规范绝对路径，``--flow`` 是 Flow 名称，两者不能同时使用。
``--depth 0`` 只包含所选范围的根节点；未指定时读取全部后代。普通
``show`` 只读取摘要；``--show-parameter``、``--show-trigger``、
``--show-limit``、``--show-event``、``--show-meter`` 和 ``--show-all``
会对所选节点再取实时详情。详情选项最多用于 4096 个所选节点；超出时
先缩小范围。TUI 只给已展开的部分建行，选中节点后才读取详情。

快照与详情的时点
------------------------------

一个完整摘要由多页组成。每页最多 2048 个节点、解码后最多 256 KiB；
同一次读取的各页共享 ``session_id``、``query_id``、``snapshot_id``、
``as_of`` 和 ``expires_at``。客户端校验身份、页序和父子关系，在所有页
成功后才替换当前视图；读取中断不会输出半棵树。快照保留约 120 秒，
过期后必须从第一页重新读取。客户端缓存包含已提交视图和新视图 staging，
总预算为 128 MiB；超限会明确失败。服务端同时保留的查询数据也有预算，
不会无限积累。分页可在默认 4 MiB gRPC 接收限制下读取本机测试的 100k
完成态合成树；真实业务容量仍须按自己的树形与活跃负载复测。

摘要节点包含 ``path``、``node_kind``、``type_id``、``status``、
``suspended``，Task 可能包含 ``attempt_id``。摘要不包含每个节点的全部
属性。详情按需选择 ``definition``、``parameters``、``runtime``、
``artifacts``、``service`` 字段组；其中 service 用于根节点。过大的详情
字段由至多 64 KiB 的分块读取并校验长度与摘要，单字段预算为 16 MiB。
参数继承和脱敏规则见 :doc:`/guide/variables` 与 :doc:`/guide/cli`。

详情是在摘要**之后独立实时采样**的结果，并返回自己的 ``as_of``；
``show`` 会标明这一点。服务会校验 session、查询身份、Flow 定义代次和
Task attempt，身份失效时拒绝旧详情。不要把摘要和详情解释为同一事务
时点。查询只是只读展示，不能当作 DefinitionDocument、checkpoint 或
恢复输入；checkpoint 才是服务重启后的恢复来源。

失败与能力边界
------------------------------

``resource_exhausted`` 可能表示查询预算用尽，也可能表示捕获大树时状态
发生变化。后者可等待状态稳定后重试完整读取，或使用 ``--scope`` 缩小
范围；CLI 不自动重试这类业务响应。TUI 只对捕获冲突作至多两次额外
尝试，仍失败时保留上次完整视图并标记 ``stale``。``snapshot_expired``
意味着续页不可用；``stale_identity`` 意味着所选详情身份已改变；
``field_too_large`` 意味着请求字段超过预算。遇到这些错误，应重新取得
快照、缩小范围或选择更少字段。``unsupported_version`` 与
``unsupported_capability`` 提示客户端与服务端能力不匹配；应使用配套
版本，不能把版本错误当成空结果。操作查询需要相应 operator 凭据。

升级时应将服务端与 Python/Go 客户端换成同一 QueryDocument v1 配对；
旧服务端没有初始查询路由，现行 ``show`` 不会自动退回旧全量 ``show``。
旧 ``ShowResponse`` JSON 也不是 QueryDocument v1 页，不能直接当作
新客户端缓存或恢复输入。升级查询接口不改变既有 checkpoint 文件的
恢复职责。

QueryDocument v1 的 capabilities 声明 ``snapshot=true``、
``detail=true``、``since=true``。服务端提供摘要的 revision 增量、
有界分页和 reset；初始页和增量目前只接受 ``summary`` 字段组。
Python 客户端可用 ``run_query_since(snapshot)`` 在同一连接上读取并合并增量；
只有完整批次通过校验后才推进水位。局部结构失效在结果的 ``stale_scopes``
中标记，调用方可重取相应范围。服务端要求重置时抛出带
``reset_scope`` 的 ``QueryResponseError``，旧视图标为陈旧。高层
``query_sync`` 和 CLI ``sync`` 会在失效或 reset 后重取所选范围，
CLI 只输出最终完整视图。``--polls`` 决定初始快照后的增量读取次数，
``--interval`` 决定后续读取间隔。Go 客户端与 TUI 的增量缓存仍待后续任务接入，
TUI 定时刷新仍重新取得所选范围的完整摘要；不提供跨重启查询缓存。查询不提供取消运行中
任务、迟报、远程 artifact 或新的只读角色；其它与 ecFlow 的差异见
:doc:`/guide/ecflow-differences`。本机 100k 完成态树测试不代表
100k 运行中任务的生产容量；大树捕获冲突和周期 checkpoint 均可能
增加用户可见等待时间。

Python 编程接口
---------------

下面的示例一次读取选定 Flow 的摘要，再按需读取一个 Task 的参数详情。
``query_snapshot`` 与 ``query_detail`` 返回结构化对象；长期连接可使用
``run_query_snapshot`` / ``run_query_since`` / ``run_query_detail``。

.. literalinclude:: /../examples/query/initial_query.py
   :language: python
