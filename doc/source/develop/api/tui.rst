TUI
=====

本页收录 ``takler.tui`` 包的模块。**TUI 是内部实现** ，接口稳定性弱
于 ``takler.core`` ；它的用法见 :doc:`/guide/cli` 的 TUI 一节。这些
模块依赖可选依赖 ``textual`` / ``rich`` （ ``takler[tui]`` ），未安
装时本页各节在文档构建中以 mock 导入顶替（见 ``conf.py`` 的
``autodoc_mock_imports`` ）。

应用与服务
----------

``TaklerTuiApp`` 是 Textual 应用入口； ``TaklerTuiService`` 是
``TaklerServiceClient`` 的薄封装，负责轮询 show 输出。

.. automodule:: takler.tui.app
   :members:

.. automodule:: takler.tui.service
   :members:

show 解析与状态样式
-------------------

``show_parser`` 把 show 命令的 JSON 载荷重建为只读视图
（ ``ShowSnapshot`` / ``NodeInfo`` ）； ``state_style`` 把节点状态映
射为终端样式。

.. automodule:: takler.tui.show_parser
   :members:

.. automodule:: takler.tui.state_style
   :members:

菜单
----

``menu`` 定义节点动作菜单（ requeue / suspend / force 等）与确认对
话框。

.. automodule:: takler.tui.menu
   :members:

标签页与组件
------------

``tabs`` 是右侧标签页（信息 / 参数 / 脚本 / 作业 / 输出），
``widgets`` 是主屏组件（节点树 / 状态栏 / 工具栏）。

.. automodule:: takler.tui.tabs
   :members:

.. automodule:: takler.tui.widgets
   :members:
