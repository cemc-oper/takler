core 设计
=========

``takler.core`` 是领域模型层：节点树、状态传播、依赖解析、触发器
表达式与序列化。它不 import 网络、进程与文件提交相关的任何代码，
可以脱离服务端单独实例化与驱动 —— 单元测试、 TUI 解析 ``show``
响应、教程里的内存内流程都直接用它。读完本页应能把「某个行为」
定位到具体的源文件与函数。

Node 继承体系
-------------

.. mermaid::

    classDiagram
        Node <|-- NodeContainer
        Node <|-- Task
        NodeContainer <|-- Flow
        NodeContainer <|-- Bunch
        Task <|-- ShellScriptTask

        class Node {
            +state: State
            +trigger_expression
            +events / meters / limits
            +repeat / times
            +computed_status(immediate)
            +resolve_dependencies()
        }
        class NodeContainer {
            +children
            +sink_status_change()
        }
        class Task {
            +task_id / try_no / job_password
            +run() / init() / complete() / abort()
        }
        class Flow {
            +calendar / begun
            +begin()
        }
        class Bunch {
            +flows: dict
            +server_state: ServerState
        }
        class ShellScriptTask {
            +script_path
            +submit() / create_job_script()
        }

* :py:class:`~takler.core.node.Node` 是抽象基类（ ``ABC`` ），持有
  所有节点共有的东西： ``State`` 、用户参数、触发器、事件、标尺、
  限额、 repeat 、时间属性，以及树结构（ ``parent`` / ``children``
  ）。它与 :py:class:`~takler.core.NodeContainer` 、
  :py:class:`~takler.core.Task` 构成「容器 / 叶子」的二分。
* :py:class:`~takler.core.NodeContainer` 是可以有子节点的容器；
  :py:class:`~takler.core.Flow` 是它的子类，多加 **日历**
  （ ``Calendar`` ）与 ``begun`` 标志 —— 只有 begun 的 flow 才被
  调度主循环处理（见 :doc:`architecture` 的调度主线）。
* :py:class:`~takler.core.Bunch` 也是 ``NodeContainer`` ，但它是
  服务端的根：用 ``flows`` 字典（而非 ``children`` ）持有全部
  flow ，另带一个 ``ServerState`` 提供服务级 generated 参数
  （ ``TAKLER_HOST`` / ``TAKLER_PORT`` / ``TAKLER_HOME`` ），参数
  继承链最终在这里收尾（见 :doc:`/guide/variables` ）。
* :py:class:`~takler.core.Task` 是叶子： ``run()`` / ``init()`` /
  ``complete()`` / ``abort()`` 的状态推进入口，持有 ``task_id`` 、
  ``try_no`` 、 ``aborted_reason`` 与一次性 ``job_password`` 。
  :py:class:`~takler.tasks.shell.ShellScriptTask` 是它目前唯一的实
  用子类（在 ``takler.tasks`` 包，不在 core ）； ``task`` /
  ``async_task`` 装饰器则在调用处动态生成内联 ``Task`` 子类。

状态计算与 sink / swim 传播
---------------------------

状态机语义（ ``unknown`` / ``complete`` / ``queued`` / ``submitted``
/ ``active`` / ``aborted`` ）见 :doc:`/guide/node-status` ；这里讲
**传播机制** ，它分布在 ``node.py`` 与 ``node_container.py`` 。

叶子与容器的 ``computed_status`` 语义不同：

* ``Task.computed_status`` 直接返回自己的 ``state.node_status`` ；
* ``NodeContainer.computed_status`` 调
  ``compute_most_significant_status(children, immediate)`` 取
  **最显著** 的子状态：按 ``aborted`` > ``active`` > ``submitted``
  > ``queued`` > ``complete`` 的固定优先级（注意这与
  ``NodeStatus`` 枚举值的大小序一致， ``Bunch.get_node_status``
  因此可以直接对 flow 状态取 ``max`` ），都没有则
  ``unknown`` 。 ``immediate=True`` 表示直接用子节点当前状态，不再
  递归下钻。

两个方向的传播：

* **sink （向下）** ： ``sink_status_change`` 把一个状态**强制** 刷
  到整棵子树。 ``Node.sink_status_change_only`` 只刷自己；
  ``NodeContainer`` 覆写后递归到全部后代。 ``force --recursive``
  走的就是这条路（ ``Scheduler.run_command_force`` →
  ``sink_status_change`` ）。
* **swim （向上）** ：叶子状态变化后沿父链上溯重算。入口是
  ``set_node_status`` → ``handle_status_change`` ：
  ``NodeContainer.handle_status_change`` 与
  ``Task.handle_status_change`` 都会调 ``swim_status_change`` ，
  后者用 ``computed_status(immediate=True)`` 重算自身，变了就写入
  并继续向 ``parent`` 冒泡直到根。 ``Task.handle_status_change``
  在上溯之前先 ``update_limits()`` —— 按新状态占用或释放限额令牌
  （ ``submitted`` 占用、 ``complete`` / ``aborted`` 释放），所以
  **限额占用是状态传播的副作用** ，不是调度器的独立簿记。

``swim_status_change`` 里还藏着 repeat 的推进逻辑：容器重算出
``complete`` 时，若挂着 repeat 就 ``increment()`` 一次；未到终点值
则就地 ``requeue(reset_repeat=False)`` 让子树为下一个 repeat 值重跑
，到终点才真正停在 ``complete`` 。语义细节见
:doc:`/guide/attributes/repeat` 。

依赖解析： resolve_dependencies 与 travel_bunch
-----------------------------------------------

调度主循环每轮对每个 begun 的 flow 调
``Flow.resolve_dependencies()`` （继承自 ``NodeContainer`` ），三个
层级的实现各管一段：

* ``NodeContainer.resolve_dependencies`` ：先 ``check_dependencies``
  检查自己，过了就**递归** 每个子节点的
  ``resolve_dependencies`` —— 依赖检查自顶向下贯穿整棵树。
* ``Node.check_dependencies`` （容器走它，任务在它的基础上扩展）：
  依次检查 未 suspended → 时间依赖
  （ ``resolve_time_dependencies`` ：没有任何时间属性即放行，有则
  至少一个 ``TimeAttribute.is_free`` ） → complete 触发器
  （ ``evaluate_complete_trigger`` 为真则闩上
  ``is_complete_triggered`` 、置 ``complete`` 并返回 False ） →
  普通触发器 ``evaluate_trigger`` 。
* ``Task.check_dependencies`` 在最外面再加任务特有的两道闸：状态闸
  （ ``complete`` / ``active`` / ``submitted`` / ``unknown`` 直接拒
  绝； ``aborted`` 也拒绝 —— aborted 的任务不会自动重跑）与限额闸
  （ ``check_in_limit_up`` 沿父链检查每个 ``InLimit`` 令牌充足）。

``Task.resolve_dependencies`` 在全部检查通过后调 ``self.run()``
—— **依赖解析与任务提交是同一个调用** ，主循环之外没有第二个自动
提交入口。

``Scheduler.travel_bunch`` 是主循环使用的遍历方法的历史形态：对
``bunch.flows`` 里每个 flow 调 ``resolve_dependencies`` 。当前主循
环用的是带异常边界的 ``_process_flow`` （先 ``update_calendar``
再 ``resolve_dependencies`` ，见 :doc:`architecture` ）；
``travel_bunch`` 仍保留在 ``Scheduler`` 上供直接驱动调度器的场景
使用。

触发器： Expression 到 AST
--------------------------

语法本身（ ``==`` / ``and`` / ``or`` / 事件 / 标尺 / 状态比较）见
:doc:`/guide/trigger-expression` ；这里讲实现管线，分布在
``expression.py`` 、 ``expression_parser.py`` 、
``expression_ast.py`` 三个文件。

#. **懒解析** 。 ``add_trigger(str)`` 默认只把字符串存进
   :py:class:`~takler.core.expression.Expression` ，不解析；
   首次 ``evaluate_trigger`` 时才 ``create_ast`` 。 ``parse=True``
   可以在添加时立即解析校验（失败抛 ``ExpressionSyntaxError`` 且
   节点不被改坏）。反序列化也以 ``parse=False`` 恢复 —— 定义期
   只做字符串搬运。
#. **解析** 。 ``parse_trigger`` 用 `Lark <https://lark-parser.
   readthedocs.io/>`_ 文法把字符串解析成语法树，再由
   ``ExpressionTransformer`` 折叠成 takler 自己的 AST （ ``AstRoot``
   为根的二叉树：内部节点是 ``AstOpAnd`` / ``AstOpOr`` / 比较与
   加法运算符，叶子是 ``AstNodePath`` / ``AstVariablePath`` /
   ``AstNodeStatus`` / ``AstInteger`` ）。文法错误统一包装为
   ``ExpressionSyntaxError`` 并带上 Lark 报告的行 / 列。
#. **绑定** 。 ``create_ast`` 之后调 ``ast.set_parent_node(node)``
   把 AST 绑到宿主节点： ``AstNodePath.set_parent_node`` 立即沿树
   解析节点路径并 **缓存** 目标节点（ ``_reference_node`` ），找不
   到抛 ``NodeNotFoundError`` —— 所以「触发器引用了不存在的节点」
   在首次求值时报错，而不是在定义时报错。 ``AstVariablePath`` 只
   校验一次存在性， **不缓存** 变量本身：事件 / 标尺的值在变，每次
   ``value()`` 都重新查找。
#. **求值** 。 ``Expression.evaluate`` 先看 ``free`` 闩（
   ``free_dep`` 会把它置位，置位即恒真），否则交给 AST 。值语义：
   ``AstNodePath.value`` 返回目标节点的 ``NodeStatus`` ；
   ``AstVariablePath.value`` 把 ``Event`` 映射为 ``1`` / ``0`` 、
   ``Meter`` 取整数值、 ``Parameter`` 取其值；比较节点对左右
   ``value()`` 比较，逻辑节点对左右 ``evaluate()`` 短路求值。

序列化： Tree / Status 双模式与 class_type 反射
-----------------------------------------------

``to_dict`` / ``from_dict`` 是节点树与外界（ ``show`` 响应、
快照文件、 ``load`` 命令）之间的唯一通道。每个可序列化的类自己实
现这对方法， ``Node`` 的基类实现负责公共字段，子类用
``fill_from_dict`` 链式填充各自多出来的字段（ ``Task`` 填
``task_id`` / ``try_no`` ， ``Flow`` 填 ``begun`` / ``calendar``
， ``ShellScriptTask`` 填 ``script_path`` ）。

**双模式** 由 :py:class:`~takler.core.util.SerializationType`
区分：

* ``Tree`` —— 只恢复 **定义** ：节点结构、参数、触发器字符串、
  事件 / 标尺 / 限额 / repeat / 时间属性，运行状态全部取初始值
  （ ``State`` 为 ``unknown`` ， ``begun`` 为 ``False`` ，日历字
  段为空）。 ``load`` 命令用它：载入的是一份新定义，不是一段历史。
* ``Status`` —— 定义之外再恢复 **运行状态** ： ``State`` 、
  ``begun`` 、日历、 complete 触发器的闩、限额占用等。快照文件与
  ``show`` 响应用它，见 :doc:`/operation/checkpoint` 。

**class_type 反射** ：每个节点的 dict 里带
``class_type: {module, name}`` ， ``Node.from_dict`` 用
``importlib.import_module`` 找到类、调 ``class_object(name=...)``
构造，再走 ``fill_from_dict`` 。这就是服务端 **不 import**
``takler.tasks`` 却能恢复 ``ShellScriptTask`` 的机制 —— 代价是自
定义 ``Task`` 子类必须能被 ``importlib`` 按模块路径导入（快照恢
复失败的常见原因，见 :doc:`/operation/troubleshooting` ）。

**什么不进序列化** 与进了一样重要： ``Task.job_password`` 被刻意
排除 —— ``to_dict`` 同时喂给 ``show`` 响应与快照文件，序列化它
等于把全部在途作业口令发给任何能调 ``show`` 的人。它由
``increment_try_no`` 与 ``requeue`` 两个写入点维护，不变式是
「 ``job_password`` 为空当且仅当 ``try_no == 0`` 」。同理，
``user_parameters`` 之外的 generated 参数（ ``TAKLER_NAME`` 等）
不序列化，它们在任务运行时由 ``update_generated_parameters``
现场重算（见 :doc:`/guide/variables` ）。
