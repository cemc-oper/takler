TUI 使用
========

``takler-tui`` 是基于 `Textual <https://textual.textualize.io/>`_ 的
终端图形界面：左侧是节点树，右侧按 tab 查看选中节点的属性、参数、
脚本与作业输出，快捷键和右键菜单执行常用控制操作。

安装与启动
----------

TUI 依赖 ``tui`` extra （ ``textual`` 与 ``rich`` ）：

.. code-block:: bash

    pip install takler[tui]

启动：

.. code-block:: bash

    takler-tui                          # 等价于 python -m takler.tui
    takler-tui --host 10.0.0.8 --port 33083
    takler-tui --connect-file /path/to/connect.yaml

连接地址按以下优先级解析（注意与 :doc:`/guide/cli` 的顺序**不同**：
这里 connect 文件优先于 ``--host`` / ``--port`` ）：

1. ``--connect-file`` 或环境变量 ``TAKLER_CONNECT_FILE`` 指向的
   ``connect.yaml``
2. ``--host`` / ``--port``
3. 环境变量 ``TAKLER_HOST`` / ``TAKLER_PORT``
4. 内置默认 ``localhost:33083``

.. warning::

    ``--connect-file`` 在 TUI 中**只用于解析地址**。 TLS 与鉴权设置
    不从 ``connect.yaml`` 的 ``security`` 段读取，只认环境变量
    ``TAKLER_TLS_CA_FILE`` 、 ``TAKLER_TLS_SERVER_NAME`` 与
    ``TAKLER_SECRET_FILE`` 。因此服务端启用鉴权后，运行 TUI 的账户
    必须能读到 Operator Secret 文件，否则控制操作会被服务端拒绝；
    背景见 :doc:`/operation/security` 。

界面布局
--------

.. code-block::

    ┌──────────────────────────────────────────────────────┐
    │ 标题栏（时钟）                                        │
    │ 工具栏：刷新按钮 / 自动刷新倒计时 / bunch 名 / 刷新时间 │
    ├───────────────────┬──────────────────────────────────┤
    │ 节点树            │ tab 页：info / parameters /      │
    │ （bunch 层级 +    │         script / job / output    │
    │  行内属性行）     │                                  │
    ├───────────────────┴──────────────────────────────────┤
    │ 状态栏：选中节点 + 操作结果                            │
    │ 快捷键提示（Footer）                                   │
    └──────────────────────────────────────────────────────┘

节点树节点按状态着色（与 :doc:`/guide/node-status` 一致）， suspend
的节点另有标记；容器节点下方带行内属性行（ trigger 、 time 、
event 、 meter 等），双击非叶节点或按 ``空格`` 折叠 / 展开。

启动后自动拉取一次全量状态，之后**手动刷新**（ ``r`` ），或在工具栏
打开自动刷新（ 60 秒一轮，手动刷新会重置倒计时）。控制操作成功后
也会自动刷新一次。

五个 tab
--------

info
    选中节点的全量属性，内容相当于 ``show --show-all`` ：路径、类
    型、状态（含 suspend 标记）、子节点数、 trigger 、 complete
    trigger 、 repeat 、 time 、 limit 、 in-limit 、 event 、
    meter 、 user 参数，以及从祖先节点**继承**来的参数。

parameters
    节点的参数表（ kind / name / value 三列）： ``local`` 行是节点
    自己定义的 user 参数， ``inherited`` 行是从祖先继承的参数——
    与渲染脚本时实际生效的查找结果一致（ local 遮蔽同名
    inherited ，见 :doc:`/guide/variables` ）。

script
    任务的 ``TAKLER_SCRIPT`` 模板文件原文。仅选中任务时显示。

job
    最新一次运行尝试生成的作业文件（ ``.jobN`` 中修改时间最新
    者）。仅选中任务时显示。

output
    最新作业输出文件的最后 1000 行，外加同目录下所有以
    ``<任务名>.`` 开头的文件列表（输出、历次作业文件等）：点击
    某行改为 tail 该文件，点击列头排序。仅选中任务时显示。

.. note::

    script / job / output 三个 tab **直接读本地文件系统**，不经过
    RPC ，因此要求 TUI 与服务能访问同一个 ``TAKLER_HOME`` （共享
    文件系统或同机运行）。原因见 :doc:`/guide/job-management` 的
    「现状限制」。

快捷键与右键菜单
----------------

查询与导航：

.. list-table::
    :header-rows: 1
    :widths: 16 84

    * - 按键
      - 作用
    * - ``r``
      - 刷新全量状态
    * - ``p``
      - ping 服务，状态栏显示往返耗时
    * - ``m`` 或右键节点
      - 打开选中节点的操作菜单
    * - ``空格``
      - 折叠 / 展开节点
    * - ``q``
      - 退出

控制操作（需先选中节点，成功后自动刷新）：

.. list-table::
    :header-rows: 1
    :widths: 16 84

    * - 按键
      - 作用
    * - ``Ctrl+R``
      - Run ：立即运行任务（仅任务节点出现在菜单中）
    * - ``Ctrl+Q``
      - Requeue ：重置节点及子树回排队状态
    * - ``Ctrl+S``
      - Suspend ：挂起节点及子树
    * - ``Ctrl+U``
      - Resume ：解除挂起
    * - ``Ctrl+F``
      - Force complete ：直接把节点置为 ``complete`` （弹确认框）
    * - ``Ctrl+D``
      - Free dependencies ：解除全部依赖（等价
        ``free-dep --dep-type all`` ）

右键菜单（或 ``m`` ）与快捷键一一对应，另多一项 **Force…** 子菜单：
可把节点强制置为 ``complete`` / ``queued`` / ``submitted`` /
``active`` / ``aborted`` / ``unknown`` 之一，当前状态对应的项会被
禁用。所有 Force 操作都会先弹确认框，且只作用于选中节点本身
（不下沉到子孙）。这些操作的命令行等价形式见 :doc:`/guide/cli` 。

.. tip::

    在开启 XON/XOFF 流控的终端里， ``Ctrl+S`` / ``Ctrl+Q`` 可能被
    TTY 拦截而表现失灵；执行 ``stty -ixon`` 或换用默认关闭流控的
    终端即可。
