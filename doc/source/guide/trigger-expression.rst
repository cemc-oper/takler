trigger 表达式参考
==================

本页是触发器表达式的完整语法参考，供写表达式时查阅；入门介绍见
:doc:`/tutorial/triggers` 。表达式通过
:py:meth:`Node.add_trigger <takler.core.node.Node.add_trigger>`
（满足才允许运行）与
:py:meth:`Node.add_complete_trigger <takler.core.node.Node.add_complete_trigger>`
（满足时节点直接判为 ``complete`` ）挂到任意节点上，两种触发器共用
同一套语法。

基本结构
--------

表达式由比较与逻辑组合构成：比较的一边是节点路径或变量路径，另一边是
状态词、 ``set`` / ``unset`` 、整数或另一个路径；多个比较用 ``and`` /
``or`` 组合，用括号改变优先级：

.. code-block::

    ./t1 == complete
    (./t1 == aborted or ./t2 == aborted) and ./t3 == complete
    ./t1:meter1 >= 4 and ./t1:event1 == set

节点路径
--------

三种写法：

* ``/test/group1/t2`` ：绝对路径。第一段必须是 flow 名，因此触发器
  **不能** 跨 flow 引用节点
* ``./t1`` ：相对路径。 ``.`` 表示当前节点的父容器， ``./t1`` 即当前
  节点的兄弟节点
* ``../t3`` ： ``..`` 表示父容器的上一级

相对路径先拼接到当前节点的父容器路径上，再从 flow 根逐段向下查找；
``find_node`` 找不到对应节点时，首次求值抛出 ``NodeNotFoundError`` 。

.. note::

    节点名只能由字母、数字、下划线组成，且必须以字母或数字开头。
    单独的 ``t1`` （不带 ``/`` 、 ``./`` 或 ``../`` 前缀）不是合法路径。

状态比较
--------

节点路径的取值是它的 :py:class:`~takler.core.NodeStatus` ，用 ``==``
（或等价的 ``eq`` ）与状态词比较。状态词只有三个，大小写不敏感：

* ``complete`` ：已完成
* ``aborted`` ：异常终止
* ``active`` ：正在运行

.. important::

    ``unknown`` 、 ``queued`` 、 ``submitted`` **不是** 合法的状态词，
    写了会在解析时报语法错误。触发器语义上只关心「跑完了没、在跑没、
    出错没」，这三个状态都是任务尚未真正开始运行的中间状态。

事件与标尺
----------

用 ``路径:变量名`` 的写法引用节点上的变量。

**事件 (event)** 与 ``set`` / ``unset`` 比较（大小写不敏感），
``set`` 相当于 1 ， ``unset`` 相当于 0 ：

.. code-block::

    ./t1:event1 == set
    ./t2:event2 == unset

**标尺 (meter)** 与整数比较，支持 ``==`` 、 ``>`` 、 ``>=`` 、 ``<`` 、
``<=`` ：

.. code-block::

    ./t1:meter1 >= 4

数字字面量只接受整数，写小数（如 ``4.5`` ）是语法错误。

**参数 (parameter)** 用同样的写法引用，取值为它的 ``value`` 。整数
参数可以直接与整数比较，任意参数都可以与另一个变量比较：

.. code-block::

    ./t1:THRESHOLD <= ./t2:meter1

表达式里没有字符串字面量，因此字符串参数不能与字面量比较，只能与
另一个变量的取值比较。

同一节点上事件、标尺、参数同名时，查找顺序是事件 → 标尺 → 参数
（ :py:meth:`Node.find_variable <takler.core.node.Node.find_variable>`
的顺序）。变量在**每次求值时重新查找**，运行期间修改取值立即生效；
变量不存在时首次求值抛出 ``ExpressionSyntaxError`` 。

运算符与优先级
--------------

.. list-table::
    :header-rows: 1
    :widths: 20 35 45

    * - 优先级（高 → 低）
      - 运算符
      - 说明
    * - 1
      - ``(`` ``)``
      - 括号分组
    * - 2
      - ``==`` 、 ``eq`` 、 ``>`` 、 ``>=`` 、 ``<`` 、 ``<=``
      - 比较。 ``eq`` 与 ``==`` 等价； ``eq`` 、 ``and`` 、 ``or``
        均大小写不敏感
    * - 3
      - ``and``
      - 逻辑与，两侧都满足才满足
    * - 4
      - ``or``
      - 逻辑或，任一侧满足即满足

``and`` 比 ``or`` 结合更紧： ``a or b and c`` 等价于
``a or (b and c)`` 。比较不能连写， ``a == b == c`` 是语法错误。

加法 ``+`` 可以把两个变量路径的取值相加；参与比较时相加的部分要用
括号括起来：

.. code-block::

    (./t1:m1 + ./t2:m2) >= 10

    ./t1:m1 + ./t2:m2 >= 10

第一行合法，第二行是语法错误（ ``+`` 的运算结果不能直接作为比较的
左操作数）。

求值语义
--------

* 调度器每一轮调度检查都会重新求值所有未满足节点的触发器
* 表达式默认在首次求值时才解析（延迟解析）。
  ``add_trigger(..., parse=True)`` 会在挂接时立即解析，把语法错误
  与路径错误提前暴露出来
* complete 触发器先于普通触发器检查：两者都定义时， complete 触发器
  满足则节点直接完成，不再检查普通触发器（见 :doc:`/tutorial/triggers` ）
* 被 ``free-dep`` 命令释放的表达式（
  :py:class:`~takler.core.expression.Expression` 的 ``free`` 标记）
  恒为满足，不再真正求值

语法错误定位
------------

解析失败抛出 ``ExpressionSyntaxError`` ，异常对象携带三个属性：

* ``expression`` ：原始表达式文本
* ``line`` 、 ``column`` ：出错位置，从 1 开始计数；错误发生在表达式
  末尾（例如 ``and`` 后面没有内容）时，底层解析器报告不了位置，两个
  属性都是 ``None``

常见写法错误：

.. list-table::
    :header-rows: 1
    :widths: 40 60

    * - 错误写法
      - 问题
    * - ``t1 == complete``
      - 路径必须以 ``/`` 、 ``./`` 或 ``../`` 开头
    * - ``./t1 == queued``
      - ``queued`` / ``submitted`` / ``unknown`` 不是合法状态词，
        只有 ``complete`` / ``aborted`` / ``active``
    * - ``./t1 = complete``
      - 相等判断是 ``==`` 或 ``eq`` ，单个 ``=`` 非法
    * - ``./t1:m1 == 4.5``
      - 数字字面量只接受整数
    * - ``./t1:m1 + ./t2:m2 >= 10``
      - ``+`` 参与比较时两侧要加括号
    * - ``./t1 == ./t2 == complete``
      - 比较不能连写，拆成 ``./t1 == ./t2 and ./t2 == complete``
        之类的形式
