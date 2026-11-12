贡献指南
========

本页覆盖从检出代码到提交一个合规改动的全过程：环境、风格、测试、覆盖
率、版本号与文档构建。

环境搭建
--------

仓库用 `uv <https://docs.astral.sh/uv/>`_ 管理环境与锁文件。一次同步即
完成可编辑安装：

.. code-block:: console

    $ uv sync --locked

``uv sync`` 默认安装 ``dev`` 组，其中包括 ``test`` 组、
``tui`` / ``log`` / ``http`` extra 以及 Ruff、grpcio-tools 等开发工具。
``--locked`` 会在锁文件与 ``pyproject.toml`` 不一致时失败。
修改依赖后运行 ``uv lock``，并一并提交 ``uv.lock``。

不用 uv 时的等价物是 ``pip install -e ".[tui,log]"`` ，但请注意本仓
库的「本地通过 == CI 通过」依赖锁文件（见下节），裸 pip 现场解析的
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

* ``tests/unit/`` 存放小数据、替身依赖和单组件测试。日常 CI 只收集此目录。
* ``tests/integration/`` 存放真实 gRPC / HTTP、进程与检查点恢复、
  完整 TUI、跨语言契约和 Sphinx 构建测试。本地按变更涉及的模块选择。
* ``tests/extended/`` 存放规模、长时属性、性能和专用工具测试，按需运行。

.. code-block:: console

    $ uv run pytest tests/unit
    $ uv sync --locked --all-groups
    $ uv run --all-groups pytest tests/integration
    $ uv run pytest tests/extended --durations=30

直接运行 ``uv run --all-groups pytest`` 会收集三个目录。G0 验证使用
``scripts/run_snapshot_g0.py`` 准备场景表和运行上下文。真实 Flow
验收在独立的 ``takler-test-suite`` 仓库中通过其 ``run.py`` 运行。

* pytest 必须以 ``--import-mode=importlib`` 运行，已在
  ``pyproject.toml`` 的 ``addopts`` 里固定。``tests/unit/logging/``
  包在默认的 ``prepend`` 模式下会遮蔽标准库 ``logging`` 模块，导致收
  集失败。共享的 hypothesis 生成器也因此用包路径导入（
  ``from tests.strategies import ...`` ）。
* ``integration``、``extended`` 和 ``slow`` 标记可用于进一步筛选。
  日常 CI 的范围由目录确定。
* 属性测试用 hypothesis ：共享生成器集中在 ``tests/strategies.py``
  （ ``bunches`` / ``flow_operation_sequences`` 等），约定
  ``@settings(max_examples=100, deadline=None)`` 。生成器里刻意固化
  了若干不变式（如 ``begun`` 当日历已初始化才为真），扩展时保持。
* 需要「时间流逝」但不想真睡的测试用 ``tests/conftest.py`` 的
  ``FakeClock`` ：把实例同时注入 ``clock=`` 与 ``sleep=`` 两个注入
  点，逻辑时间随 sleep 推进，一整天的 child 重试窗口在毫秒级跑完。
* 文档示例由 ``tests/unit/doc/test_doc_examples.py`` 钉住：文档里的行为声
  明应有对应测试，改动核心行为破坏了文档示例会让这里失败。

覆盖率门槛
----------

覆盖率门槛在 CI （ ``.github/workflows/ci.yml`` ）中按范围检查。
每个 Python 版本的门槛复用其默认 pytest 步骤写出的覆盖率数据。

* ``takler/query.py`` 、 ``serialization/query.py`` 、 ``takler/client/*`` 、
  ``server/transport.py`` 、
  ``server/grpc_transport.py`` 、 ``server/http_transport.py`` 、
  ``server/handlers.py`` 、
  ``server/protocol/*`` 与 ``takler/protocol/*`` 合计不低于 ``85%`` 。
* 序列化、schema 和 ``server/flow_replace.py`` 各自不低于 ``85%`` 。
* ``server/auth.py`` / ``zombie.py`` / ``audit.py`` / ``tls.py`` **逐
  模块** 不低于 ``85%`` 。
* TUI 不低于 ``60%`` 。

本地复现：

.. code-block:: console

    $ uv run pytest tests/unit --strict-markers --cov=takler --cov-report=term --durations=30
    $ uv run coverage report \
        --include="src/takler/query.py,src/takler/serialization/query.py,src/takler/client/*,src/takler/server/transport.py,src/takler/server/grpc_transport.py,src/takler/server/http_transport.py,src/takler/server/handlers.py,src/takler/server/protocol/*,src/takler/protocol/*" \
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
  行为由 ``tests/unit/doc/test_doc_examples.py`` 钉住。
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

    :memo: Add events tutorial page with step8 example

    - new tutorial page doc/source/tutorial/going-further/events.rst
    - runnable example step8_events.py and task1_with_event.takler
    - tests assert event gating and requeue reset

    Event reports let downstream tasks start before the upstream
    task completes, e.g. t1 releasing t2 halfway through its run.

CI
--

``.github/workflows/ci.yml`` 在 Python 3.11 与 3.12 上用
``uv sync --locked`` 还原环境。推送和 PR 运行 Ruff、``tests/unit/``
及覆盖率门槛，并输出最耗时的 30 个测试阶段。

本地配对检查与发布验证
~~~~~~~~~~~~~~~~~~~~~~

本地联调时，把两个仓库检出到同一父目录，并从 ``takler-client/``
运行以下命令。

.. code-block:: bash

    export TAKLER_REPO=../takler
    (
      cd "$TAKLER_REPO"
      uv sync --locked --all-groups --extra http
      TAKLER_CLIENT_REPO=../takler-client bash scripts/check_proto.sh
    )
    make proto-check wire-check
    make http-contract load-contract replace-contract show-contract
    make query-a-contract query-b-contract

这些命令核对当前检出的 Python proto、共享 schema 和向量，以及
Python / Go 客户端通过 gRPC 和 HTTP 运行命令和查询的行为。
安装冒烟可从 ``takler/`` 执行
``TAKLER_INSTALL_PYTHON=3.11 bash scripts/check_install_matrix.sh``，
将版本改为 ``3.12`` 可复现另一个安装作业。

协议变更先重新生成 Python bindings，再从 Go 仓库运行
``make proto-sync`` 并提交生成文件。可在候选分支手动运行
`Takler CI <https://github.com/cemc-oper/takler/actions/workflows/ci.yml>`_
做配对验证。``peer_sha`` 取对端完整小写提交 SHA，留空时取对端
``main``。运行摘要记录两边实际检出的 SHA。

发布前，在 ``main`` 上手动运行 Takler CI 的完整检查并等待成功，再
为同一提交打标签。发布门槛检查该提交最新的手动 CI 运行，并逐项核对
双版本单元测试、双版本安装冒烟和配对契约。
