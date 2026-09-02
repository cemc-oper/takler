附录：HPC 环境
==================

本附录收录教程正文中省略的 HPC 特定内容，正文的 :doc:`getting-started/index`
已经改为面向普通 Linux 服务器，不依赖本附录中的任何内容即可完整走完教程。

使用预安装环境（以 CMA-HPC2023-SC3 为例）
--------------------------------------------

在中国气象局国家级气象超算子系统 3 (CMA-HPC2023-SC3) 上，如果有人已经用
``module`` 方式发布了 takler 环境，可以直接加载，跳过手动安装 Python 包的步骤：

.. code-block:: bash

    export MODULEPATH=/g1/u/wangdp/modules:$MODULEPATH
    module load wangdp/share/tool/takler/latest

上述路径与模块名只是一个示例，实际的 ``MODULEPATH`` 与模块名取决于超算上
负责发布该环境的账户，请向管理该环境的团队确认。

离线安装
------------

在无法直接连接互联网的超算计算节点或登录节点上，需要先在有网络的机器上
下载 takler 与 takler-client 的源码或发布包（压缩包形式），再通过内部文件
传输方式（如 ``scp``、共享文件系统）拷贝到 HPC 环境中安装：

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

需要说明的是，Takler 当前的作业提交方式只有本地 shell 后台运行
（``ShellRunner.spwan()`` 经 ``anyio.run_process`` 执行 ``/bin/sh -c``），
没有面向 PBS / Slurm 等作业调度系统的提交实现，也没有 kill 实现
（``TAKLER_SHELL_KILL_CMD`` 已定义但全库无引用）。

在 HPC 环境中运行时，任务脚本仍然是在提交 Takler 服务所在的那台节点上
以本地进程方式执行，不会经由 ``qsub`` / ``sbatch`` 提交到计算节点排队系统。
如果需要把作业提交到调度系统排队执行，需要自行在任务脚本中调用相应的
提交命令并自行处理与调度系统的交互，Takler 本身不提供这层封装。

.. note::

    与 ecFlow 相比的完整能力差异清单（含作业提交方式）会集中在用户指南的
    「与 ecFlow 的差异与限制」一页中给出（本方案批次 K，尚未发布）。
