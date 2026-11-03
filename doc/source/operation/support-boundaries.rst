版本配对与支持边界
==================

本页更新于2026-11-03，区分已交付接口、历史验证与尚未完成的验收。
当前只做运维文档和小数据示例核对，不代表完整发布或容量认证。

升级与消费者配对
----------------

服务端和Python/Go客户端应配套升级。DefinitionDocument v1、checkpoint v3、
QueryDocument v1分别用于定义、恢复和公开查询，不能互换。全量详细快照还
要求服务端声明 ``full_snapshot_v1``；仅有QueryDocument版本号不足以判断支持。
旧show RPC/HTTP端点已删除，不提供旧混合JSON转换或任意版本混用承诺。

本次文档核对的Python源码为
``ed1e0fd28934351cb7a909bda539c28731d2d09a``，Go客户端为
``04272786c9f774cf936adeb39ed9a18a8728b564``。这是源码核对基线，不是新的
已发布版本号、托管CI配对或“高于此版本均兼容”的最低版本声明。

历史R3-13基础local演练使用以下配对；原报告记录Python还有当项工作树修改，
因此不能仅checkout这四个SHA就声称重建了完整验收环境：

.. list-table:: 历史local演练参考
   :header-rows: 1

   * - 项目
     - 历史源码
   * - takler
     - ``0ede86267de3ebeadde6c59d18a2940c4de730ad`` 加当项修改
   * - takler-client
     - ``04272786c9f774cf936adeb39ed9a18a8728b564``
   * - takflow
     - ``f5cac7da699fc30ce282fb12eaaf6da8699f1c0f``
   * - orvix
     - ``0142466162143a9d4d07c8af89344b5fc45d6617``

选择下游版本至少要核对这些能力：takflow导出DefinitionDocument v1，提交
模板保留Jinja双花括号并使用 ``shquote``，传播本轮attempt凭据，未支持的
late/repeat_day明确报错；orvix按约定解析脚本和默认info sidecar。本轮没有
重新验证当前下游HEAD，也没有据历史证据指定新的最低版本号。

当前未知提交保护识别直接 ``orvix submit``；包装shell、前置环境赋值等
不能假定同样识别。默认info路径为 ``<TAKLER_JOB>.info.yaml``，自定义
``--output-info`` / ``--output-script`` 未纳入现有配对。local的
``FINISHED`` 本身没有提供原进程启动身份及成功退出码，不能单独用它证明
原作业成功。目标HPC、共享存储及长期运行仍需独立验证。

值班时应保留的语义
------------------

* 提交结果未知时，先用 ``show --scope /flow/task --show-all`` 查询当前
  attempt、runtime原因及artifact引用，再核对外部作业。submitted未知状态
  保留limit占用；不要靠requeue、强制run或服务resume释放。只有确认外部
  未接收或已终结后，才按 :doc:`troubleshooting` 对单任务做非递归终态force。
  外部取消受理不等于终止确认，当前takler没有作业取消接口。
* 摘要与实时详情不是同一时点；需要一致结果使用 :doc:`/guide/query` 的
  完整快照。租约失效/reset需重建，不能把旧视图当作最新状态；流式结果必须
  等待最终完成证明，错误不能转为空树。
* checkpoint恢复默认halted。先查 ``server-status``、在途身份和外部作业，
  再决定是否 ``server-resume``。保存故障暂停只能在成功保存后清除，人工和
  recovery暂停不会随保存成功自动解除，见 :doc:`checkpoint`。
* 写命令成功仅表示内存生效；客户端不自动重试写操作。响应丢失要查询核实，
  attempt不是通用幂等键。checkpoint是恢复唯一来源，无WAL、数据库日志重放
  或持久请求去重，最后快照之后的变化可能丢失。

当前验证结论
------------

R3-01至13保留实现、局部正确性及基础local链路通过记录。R3-14混合容量
验收和R3-16最终配对/阶段退出暂停且未完成；R3-15文档收尾不替代它们。
后续功能开发仍需与改动相关的正常回归，不必为写文档重跑容量矩阵。

2026-11-01的限定100k运行保存约4–7秒，已有归档与各自固定版本一致。
2026-11-02的CK-R-02对五个原文件及一次损坏主文件备份回退，均完成正式
halted恢复及100101节点内容比对。正式恢复约16–17秒，恢复加完整比对约
35–36秒，同时RSS采样峰值约1.256GiB；这些是单机、已有标准树归档的成本
记录，不是SLO、目标部署容量或所有树形的保证。

原混合运行仍有84个METER未发槽、4个查询未发槽及响应越线，整体仍FAIL；
不能将这些问题直接归因checkpoint，也不能因恢复通过改写混合结论。
完整作业生命周期、完整R3和生产容量仍未验收。目标100k机器通常有上百GB
内存，512MiB不是正式恢复的产品门槛；这不取消客户端/快照内部预算。
业务允许的数据丢失窗口和恢复时间尚未给定，不承诺生产RPO/RTO。

原始证据保留在工作区 ``doc/2026-10-09-r3/evidence/`` 及
``doc/2026-10-25-100k-product-optimization/evidence/``。后者的
``20261101T130542Z/report-CK-ACCEPT-01.md`` 和
``20261102T124124Z/report-CK-R-02.md`` 分别记录混合失败与恢复补证，
不能互相替代。本轮不重跑历史性能、HPC或完整生命周期测试。
