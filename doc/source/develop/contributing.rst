贡献指南
========

本页覆盖从检出代码到提交一个合规改动的全过程：环境、风格、测试、覆盖
率、版本号与文档构建。

环境搭建
--------

仓库用 `uv <https://docs.astral.sh/uv/>`_ 管理环境与锁文件。一次同步即
完成可编辑安装：

.. code-block:: console

    $ uv sync --locked --extra tui --extra log --extra test

三个 ``--extra`` 是用户可见的可选依赖（ TUI 、 loguru 后端、测试依
赖）； ``dev`` 依赖组（ ruff 、 grpcio-tools 等）由 ``uv sync`` 默认
带上，无需显式声明。 ``--locked`` 让锁文件与 ``pyproject.toml`` 不一
致时直接失败，而不是就地重新解析 —— 改了依赖后记得 ``uv lock`` 并把
``uv.lock`` 一起提交。

不用 uv 时的等价物是 ``pip install -e ".[tui,log,test]"`` ，但请注意本
仓库的「本地通过 == CI 通过」依赖锁文件（见下节），裸 pip 现场解析的
环境可能与 CI 不一致。

代码风格
--------

两条命令，都必须通过：

.. code-block:: console

    $ uv run ruff check .
    $ uv run ruff format --check .

ruff 的 lint 规则集是 **显式声明** 的 ``select = ["E4", "E7", "E9",
"F"]`` ，不是 ruff 的版本默认值。原因是 ruff 0.16 把默认规则集从 59
条扩到 413 条，曾造成本地（锁文件里的 0.15.x ）全绿、 CI （现场解析
的最新版）报上千条错误。要采纳新规则，请作为一次独立的、有意的改
动：追加规则前缀并修完对应告警，不要顺手混进功能改动里。

protobuf 生成的 ``*_pb2.py`` / ``*_pb2_grpc.py`` 在 ruff 与覆盖率中同
时被排除，永远不要手工编辑它们（重新生成见 :doc:`protocol` ）。

运行测试
--------

.. code-block:: console

    $ uv run pytest

* pytest 必须以 ``--import-mode=importlib`` 运行，已在
  ``pyproject.toml`` 的 ``addopts`` 里固定。原因： ``tests/logging/``
  包在默认的 ``prepend`` 模式下会遮蔽标准库 ``logging`` 模块，导致收
  集失败。共享的 hypothesis 生成器也因此用包路径导入（
  ``from tests.strategies import ...`` ）。
* ``slow`` 标记（ ``pyproject.toml`` 已注册）标记派生真实进程或耗时
  较长的测试； ``uv run pytest -m "not slow"`` 可以跳过。
* 属性测试用 hypothesis ：共享生成器集中在 ``tests/strategies.py``
  （ ``bunches`` / ``flow_operation_sequences`` 等），约定
  ``@settings(max_examples=100, deadline=None)`` 。生成器里刻意固化
  了若干不变式（如 ``begun`` 当日历已初始化才为真），扩展时保持。
* 需要「时间流逝」但不想真睡的测试用 ``tests/conftest.py`` 的
  ``FakeClock`` ：把实例同时注入 ``clock=`` 与 ``sleep=`` 两个注入
  点，逻辑时间随 sleep 推进，一整天的 child 重试窗口在毫秒级跑完。
* 文档示例由 ``tests/doc/test_doc_examples.py`` 钉住：文档里的行为声
  明应有对应测试，改动核心行为破坏了文档示例会让这里失败。

覆盖率门槛
----------

本地 ``pyproject.toml`` **故意不设** ``fail_under`` —— 门槛只针对特
定模块，且 ``pytest tests/some_file.py`` 这类局部运行不应因门槛失
败。门槛在 CI （ ``.github/workflows/test.yml`` ）的两个独立步骤里，
复用 pytest 步骤写出的同一份覆盖率数据：

* ``takler/client/*`` 、 ``server/network_service.py`` 与
  ``server/protocol/*`` 合计不低于 ``85%`` ；
* ``server/auth.py`` / ``zombie.py`` / ``audit.py`` / ``tls.py`` **逐
  模块** 不低于 ``85%`` —— 刻意不合并成一次检查：合计值会让高覆盖率
  的小模块把退步的大模块托过线。

本地复现：

.. code-block:: console

    $ uv run pytest --cov=takler --cov-report=term
    $ uv run coverage report \
        --include="src/takler/client/*,src/takler/server/network_service.py,src/takler/server/protocol/*" \
        --fail-under=85

版本号
------

版本号由 setuptools_scm 从 git 标签与历史推导，构建时写入
``src/takler/_version.py`` （该文件是生成物，不要手工编辑）。两点推
论：

* 版本号形如 ``0.1.1.dev35+g57ecd3f1c`` 是正常状态，表示距最近标签
  35 个提交；
* CI 检出必须 ``fetch-depth: 0`` —— 浅克隆拿不到标签，推导出的版本
  号与仓库状态无关。本地 `uv sync` 同理需要完整历史。

文档
----

文档源码在 ``doc/source`` ，全文中文，主题
``pydata_sphinx_theme`` 。本地构建：

.. code-block:: console

    $ uv sync --group docs
    $ uv run sphinx-build -b html -W -E doc/source doc/build

``-W`` 把警告当错误，每批文档改动都必须零警告通过。写作约定：

* **示例单一来源** ：教程与指南的 Python 示例放在 ``doc/examples/``
  下， rst 里用 ``literalinclude`` 引用，不在 rst 里手抄代码；示例的
  行为由 ``tests/doc/test_doc_examples.py`` 钉住。
* **交叉引用受 nitpicky 约束** ： ``:py:class:`` / ``:py:meth:`` 只能
  指向 API 页（ ``develop/api/`` ）已收录的对象，其余类名一律用
  ``双反引号字面量`` 。
* 图用 mermaid （架构图、时序图、状态图），并列的客户端命令用
  sphinx-design 的 tab 。
* 一个已知的 docutils 怪癖： ``**加粗**`` 的结束标记紧跟中文字符会被
  误判为未闭合（ ``Inline strong start-string without end-string``
  ），在加粗片段与中文之间留一个空格即可。

提交消息
--------

提交消息全部使用英文，结构为：文本形式的 gitmoji + 一行标题 + 空行
+ 短横线列表逐条列出具体改动 + 可选的一段背景说明。示例::

    :memo: Add events and meters tutorial page with step7 example

    - new tutorial page doc/source/tutorial/events-and-meters.rst
    - runnable example step7_events_and_meters.py and task1_with_events.takler
    - tests assert event/meter gating, requeue reset, and out-of-range rejection

    Event and meter reports let downstream tasks start before the upstream
    task completes, e.g. t1 releasing t2 halfway through its run.

CI
--

``.github/workflows/test.yml`` 在 Python 3.11 与 3.12 矩阵上执行：
``uv sync --locked`` 还原环境 → ``ruff check`` → ``ruff format
--check`` → ``pytest --cov`` → 两道覆盖率门。所有命令都走 ``uv
run`` ，与本地完全一致 —— 本地按本页命令跑过， CI 就不会给出不同的
结论。
