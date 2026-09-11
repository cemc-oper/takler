简介
======================

本节介绍使用 takler 的基本步骤。

编写工作流定义
----------------------

工作流定义描述如何运行任务并与其进行交互。
*Task* 可以组成 *NodeContainer*，*NodeContainer* 可以属于另外的 *NodeContainer* 或属于 *Flow*。
所有的实体 (Task, NodeContainer, Flow) 都叫做 *Node*，构成一个层次树。

takler 使用 Python 编写工作流定义。

编写任务脚本
------------------

takler 中的每个 Shell 脚本任务都对应一个 takler 脚本文件。
脚本定义任务需要执行的主要工作，包含子命令 (child command) 调用、模板特殊命令等。
当前 takler 的 Shell 脚本任务仅支持 Jinja2 模板格式。

子命令 (child command) 是 *takler_client* 的命令子集，实现与 takler 服务的通讯。
这些命令通知服务某任务已经开始，完成，出错，或设置某些事件 (event、meter 等)。

运行 takler 服务
-------------------

启动 takler 服务后：

* takler 服务开始调度工作流 (flow)
* 每隔一段时间 (默认情况下为 10 秒)，调度器会检查工作流中每个任务的依赖关系。
  如果满足依赖关系，服务会提交该任务 (task)。
  这个过程被称为作业生成 (job creation)。任务对应的运行过程被称为作业 (job)。

运行中的作业使用子命令 (child command) 与服务通信，会导致：

- 服务中保存的节点 (node) 状态 (NodeStatus) 改变
- 更新节点的树形，比如事件 (event)，标尺 (meter)

使用 CLI 交互
---------------------------

使用 takler 的命令行工具 *takler_client* 可以查询工作运行状态并控制工作流：

- 查询工作流状态 (show)
- 改变节点状态 (force)
- 控制工作流 (requeue, suspend, resume, run)
