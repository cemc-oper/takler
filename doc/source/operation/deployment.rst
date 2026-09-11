部署 takler-server
==================

本页面向运维人员，覆盖 takler 服务进程的完整生命周期：安装、启动前
检查、前台与常驻运行、优雅停机与健康检查。连接配置文件
(``connect.yaml``) 的字段细节见 :doc:`/operation/connect-config` ，
TLS 与鉴权的配置见 :doc:`/operation/security` 。

安装
----

服务端与 Python 客户端同属一个包：

.. code-block:: bash

    pip install takler

安装后得到三个命令： ``takler-server`` （服务端）、
``takler-client-py`` （命令行客户端）与 ``takler-tui`` （终端界面，
需 ``pip install takler[tui]`` ）。 Go 客户端 ``takler_client``
是独立仓库，单独构建，见 :doc:`/guide/cli` 。

无法连接互联网的机器（如 HPC 登录节点）需要先在有网络的机器下载源码
或发布包再拷贝过去安装，步骤见 :doc:`/tutorial/hpc-appendix` 的
「离线安装」一节。

安装是否成功用 ``--help`` 验证：它打印全部启动选项并以退出码 ``0``
结束：

.. code-block:: bash

    takler-server --help
    python -m takler.server --help   # 等价写法

takler-server 选项
------------------

``takler-server`` 没有子命令，全部选项直接跟在命令名后：

.. list-table::
    :header-rows: 1
    :widths: 24 76

    * - 选项
      - 含义
    * - ``--host``
      - 向客户端与作业脚本宣告的主机名。缺省取 ``connect.yaml`` 的
        ``server.address.hostname`` ，再缺省为 ``localhost``
    * - ``--port``
      - gRPC 服务监听端口。缺省取 ``connect.yaml`` 的
        ``server.address.port`` ，再缺省为 ``33083``
    * - ``--config``
      - ``connect.yaml`` 的路径，优先于环境变量
        ``TAKLER_CONNECT_FILE``
    * - ``--checkpoint-file``
      - 检查点文件路径，覆盖 ``connect.yaml`` 的 ``checkpoint.file``
        ；都未配置时为当前工作目录下的 ``takler.check``
    * - ``--checkpoint-interval``
      - 两次快照之间的秒数，覆盖 ``connect.yaml`` 的
        ``checkpoint.interval`` ；都未配置时为 ``120`` 。小于 ``10``
        的取值被拒绝并回退到 ``120``
    * - ``--exception-policy``
      - 未预期异常的处理策略： ``resilient`` （默认，记日志后恢复）
        或 ``fail_fast`` （记日志后干净退出）。也可用环境变量
        ``TAKLER_EXCEPTION_POLICY`` 设置
    * - ``--tls-cert`` / ``--tls-key``
      - 服务端证书与私钥路径，两者必须同时给出，只给一个会以退出码
        ``1`` 终止启动。优先于 ``connect.yaml`` 的 ``security`` 段，
        详见 :doc:`/operation/security`

每个设置的完整取值优先级链见 :doc:`/operation/connect-config` 。

.. note::

    ``--host`` / ``--port`` 携带一个运维前提：从仍有 submitted /
    active 作业的快照恢复时，本次地址必须与快照记录的一致——那些在途
    作业的作业脚本里写死了旧地址，会继续向旧地址上报。该前提对命令行
    与配置文件来源的地址同样生效；恢复流程与地址不一致时的表现见
    :doc:`/tutorial/going-further/zombies-and-restart` 。

启动前检查清单
--------------

#. **工作目录。** 先 ``cd`` 到一个为服务专设的目录再启动。两个默认
   路径都相对它解析：检查点文件 ``./takler.check`` ，以及 bunch 级
   变量 ``TAKLER_HOME`` 的默认值 ``"."`` ——不显式设置时，全部作业
   文件与作业输出都落在服务的工作目录下（见
   :doc:`/guide/job-management` ）。生产部署应在 flow 或 bunch 上
   把 ``TAKLER_HOME`` 指向专设目录。
#. **目录权限。** 服务账户需要对 ``TAKLER_HOME`` 及检查点文件所在
   目录的写权限；检查点文件与审计文件由服务以 ``0600`` 创建
   （含在途作业口令，见 :doc:`/operation/security` ）。
#. **umask 。** 作业脚本的读写位由服务进程的 umask 决定。启用鉴权
   时若 umask 允许同组 / 其他用户读新建文件，启动时会记一条 WARNING
   ——口令会随作业脚本泄露，鉴权形同虚设。启用鉴权前把 umask 设为
   ``0077`` ，详见 :doc:`/operation/security` 的「作业脚本权限与
   umask 」。
#. **安全文件可读。** 配置了 TLS 或鉴权时，确认证书、私钥、密钥与
   白名单文件路径正确且服务账户可读；配置不可用时服务拒绝启动
   （向 stderr 输出一行，退出码 ``1`` ），不会降级为明文或无鉴权
   启动。

前台与常驻运行
--------------

直接执行即为前台运行，日志输出到控制台：

.. code-block:: bash

    cd /home/oper/takler-server
    takler-server --config /home/oper/takler/connect.yaml

服务没有内置的守护进程模式，常驻运行交给外部工具。简单场合用
``nohup`` ：

.. code-block:: bash

    cd /home/oper/takler-server
    TAKLER_LOG_FILE=/home/oper/takler-server/takler.log \
        nohup takler-server --config /home/oper/takler/connect.yaml &

生产环境建议用 systemd 托管，下面是一个最小单元文件：

.. code-block:: ini

    [Unit]
    Description=takler workflow server
    After=network.target

    [Service]
    Type=simple
    User=oper
    WorkingDirectory=/home/oper/takler-server
    Environment=TAKLER_LOG_FILE=/home/oper/takler-server/takler.log
    Environment=TAKLER_CONNECT_FILE=/home/oper/takler/connect.yaml
    ExecStart=/home/oper/takler-venv/bin/takler-server
    # 优雅停机：默认的 SIGTERM 即触发关机快照
    TimeoutStopSec=30

    [Install]
    WantedBy=multi-user.target

日志级别与去向由环境变量 ``TAKLER_LOG_LEVEL`` 与 ``TAKLER_LOG_FILE``
控制，未配置文件时输出到控制台；完整的日志配置面见
:doc:`/operation/logging` 。

停机与关机快照
--------------

向服务进程发送 ``SIGTERM`` 或 ``SIGINT`` （前台运行时按
``Ctrl+C`` ）即触发优雅停机：两个信号都被路由到正常的关闭流程——
先停网络服务与调度器，最后由检查点管理器写出**最终快照**。关闭流程
只执行一次，重复发信号无害。日志出现 ``stop server...done`` 且检查
点文件的修改时间已更新，即为停机完成：

.. code-block:: bash

    systemctl stop takler        # 或：kill <pid>；前台运行按 Ctrl+C
    ls -l takler.check           # 修改时间应停在停机时刻

下一次启动会从该快照恢复节点树，在途任务保持 submitted / active
状态，其作业上报仍被接受（口令随快照还原）。

.. warning::

    ``SIGKILL`` （ ``kill -9`` ）不给服务写最终快照的机会，重启后
    只能恢复到最近一次周期快照（默认最多丢失 120 秒的状态变化），
    周期快照不可用时再退到备份文件 ``takler.check.b`` 。请始终用
    ``SIGTERM`` 停机。

在不支持信号处理器的平台（ Windows ）上，信号保持默认行为，关机
快照尽力而为。

健康检查
--------

``ping`` 免鉴权、不读节点树，适合直接接入监控：

.. tab-set::

    .. tab-item:: takler_client

        .. code-block:: bash

            takler_client ping

    .. tab-item:: takler-client-py

        .. code-block:: bash

            takler-client-py ping

服务可达时打印往返耗时并以退出码 ``0`` 结束；重试窗口耗尽仍连不上
时退出码为 ``4`` 。用 systemd 时可配合 ``ExecStartPost`` 或
看门狗脚本定期检查。
