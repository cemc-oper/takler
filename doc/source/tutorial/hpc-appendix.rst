附录：HPC 环境
==================

本附录收录教程正文中省略的 HPC 特定内容，正文的 :doc:`getting-started/index`
已经改为面向普通 Linux 服务器，不依赖本附录中的任何内容即可完整走完教程。

使用预安装环境（以 CMA-HPC2023-SC3 为例）
--------------------------------------------

在中国气象局国家级气象超算子系统 3 (CMA-HPC2023-SC3) 上，如果有人已经用 ``module`` 方式发布了 takler 环境，可以直接加载，跳过手动安装 Python 包的步骤：

.. code-block:: bash

    export MODULEPATH=/g1/u/wangdp/modules:$MODULEPATH
    module load wangdp/share/tool/takler/latest

上述路径与模块名只是一个示例，实际的 ``MODULEPATH`` 与模块名取决于超算上负责发布该环境的账户，请向管理该环境的团队确认。

离线安装
------------

在无法直接连接互联网的超算计算节点或登录节点上，需要先在有网络的机器上下载 takler 与 takler-client 的源码或发布包（压缩包形式），再通过内部文件传输方式（如 ``scp``、共享文件系统）拷贝到 HPC 环境中安装：

.. code-block:: bash

    # 在有网络的机器上下载
    git clone https://github.com/cemc-oper/takler
    git clone https://github.com/cemc-oper/takler-client

    # 打包后传输到 HPC 环境，再在 HPC 环境中执行
    cd takler && pip install .
    cd takler-client && make

安装完成后，后续步骤与教程正文一致。

作业调度系统集成
--------------------

takler 已面向 HPC 共享文件系统使用，HPC 调度系统交互由 orvix 承担。
takler 负责依赖调度、job 脚本生成及 child 状态回报；orvix 负责向目标调度器
提交作业。``ShellRunner.spawn()`` 在服务节点执行提交命令，并不意味着
计算作业只能在该节点运行。

默认 ``TAKLER_SHELL_JOB_CMD`` 直接执行 job 文件；HPC 部署可以将其配置为
orvix 提交命令，例如已配置 Slurm 资源指令的脚本可使用：

.. code-block:: python

   flow.add_parameter(
       "TAKLER_SHELL_JOB_CMD",
       'orvix submit --scheduler slurm "{{ TAKLER_JOB }}"',
   )

orvix 必须安装在服务账户的 PATH 中，脚本需包含部署所需的 ``#ORVIX``
资源指令；队列、账户和资源配置由站点决定。计算节点需能访问脚本及客户端，
并能向 takler 回报 init/complete/abort。提交命令退出成功不等于计算作业完成。
本教程不连接真实 HPC 队列进行验收。

takler CLI/TUI 当前没有 kill 命令。终止队列作业应使用 orvix 或调度系统工具，
随后核对任务状态；不要将调度器作业 ID 一律当作本地进程号。

.. note::

    与 ecFlow 相比的完整能力差异清单（含作业提交方式）集中在用户指南的
    :doc:`/guide/ecflow-differences` 一页。
