.. takler documentation master file, created by
   sphinx-quickstart on Tue Jun 28 10:09:45 2022.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

欢迎来到 takler 文档
==================================

**takler** 是面向数值天气预报模式的工作流调度软件，仿照 ECMWF 的开源项目 ecFlow 开发相关功能。


历史
======

takler 最初于 2014 年由 perillaroc 在 NWPC/CMA 工作期间开发（当时是其入职第二年），
作为一个工作项目，目标是实现一个借鉴 `ecFlow <https://github.com/ecmwf/ecflow>`_ 基本功能的轻量级任务调度工具。

自 2022 年起，项目经历了彻底的重构，
现基于 `asyncio <https://docs.python.org/3/library/asyncio.html>`_ 和 `gRPC <https://grpc.io/>`_ 构建。
当前的目标是提供一个同时支持业务运行与试验研究两类场景的工作流管理系统。


.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: 用户

   教程 <tutorial/index>


.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: 用户指南

   guide/index


.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: 运维

   operation/index


.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: 开发者

   develop/index
