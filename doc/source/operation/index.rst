运维
==================================

本章面向部署与运维 takler 服务的读者。

**部署与配置** ：:doc:`deployment` 覆盖 ``takler-server`` 的安装、
启动前检查、常驻运行与优雅停机； :doc:`connect-config` 是
``connect.yaml`` 的全量字段参考与取值优先级链。

**状态持久化** ：:doc:`checkpoint` 说明快照内容、原子写与备份、
启动恢复流程与排查； :doc:`zombie` 说明三个判定条件、三种处置策略
与 requeue 后旧作业上报的典型场景。

**安全** ：:doc:`security` 覆盖 TLS 、鉴权、作业口令、密钥轮换
与升级路径。

**日志与韧性** ：:doc:`logging` 说明日志级别、格式、落盘与审计
分流； :doc:`audit` 说明审计记录的字段、文件与查询方法；
:doc:`resilience` 说明异常策略、按 flow 的故障隔离与干净停机。

.. toctree::
   :hidden:
   :maxdepth: 2

   deployment
   connect-config
   checkpoint
   zombie
   安全部署 <security>
   logging
   audit
   resilience
