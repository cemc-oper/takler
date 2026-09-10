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
与审计日志。

.. toctree::
   :hidden:
   :maxdepth: 2

   deployment
   connect-config
   checkpoint
   zombie
   安全部署 <security>
