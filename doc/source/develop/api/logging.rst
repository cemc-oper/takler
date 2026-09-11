日志
=====

本页收录 ``takler.logging`` 包的模块。**日志子系统是内部实现** ，接
口稳定性弱于 ``takler.core`` ；自定义 backend 的扩展契约见
:doc:`/develop/extending` ，运行期配置项见 :doc:`/operation/logging`
。

入口
----

包级别的两个入口： ``get_logger`` 取组件 logger ， ``configure`` 是
集中的配置入口（ Logging_Configurator ）。 ``:ignore-module-all:``
让本节只收录包内定义的对象，从子模块 re-export 的异常类在各自模块收
录。

.. automodule:: takler.logging
   :members:
   :ignore-module-all:

配置与级别
----------

``config`` 把显式参数、环境变量与内置默认值解析成唯一的
``ResolvedConfig`` ； ``levels`` 定义 canonical 级别枚举
``LogLevel`` 。

.. automodule:: takler.logging.config
   :members:

.. automodule:: takler.logging.levels
   :members:

错误与结果对象
--------------

``errors`` 定义配置错误 ``InvalidLogLevelError`` 与幂等应用的回执
``SettingFailure`` / ``ApplyResult`` 。

.. automodule:: takler.logging.errors
   :members:

格式化与后端
------------

``formatter`` 渲染 canonical 日志行； ``backends`` 定义后端抽象
（ ``LoggingBackend`` / ``NamedLogger`` ）与基于探测的选择器。两个具
体实现： ``stdlib_backend`` 总是可用； ``loguru_backend`` 只在安装了
可选依赖 ``loguru`` （ ``takler[log]`` ）时可导入。

.. automodule:: takler.logging.formatter
   :members:

.. automodule:: takler.logging.backends
   :members:

.. automodule:: takler.logging.backends.stdlib_backend
   :members:

.. automodule:: takler.logging.backends.loguru_backend
   :members:
