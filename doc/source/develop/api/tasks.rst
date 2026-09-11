任务
=====

本页收录 ``takler.tasks.shell`` 的 shell 任务实现：任务类本身、脚本渲
染、作业派生与定义校验。各对象的协作机制见 :doc:`/guide/job-management`
与 :doc:`/develop/extending` 。

任务类与生成变量
----------------

.. autosummary::
   :toctree: generated

   takler.tasks.shell.ShellScriptTask
   takler.tasks.shell.ShellScriptTaskGeneratedParameters

脚本渲染与作业派生
------------------

``ShellRender`` 用 Jinja2 把任务脚本渲染成作业文件； ``ShellRunner``
是派生作业子进程的唯一入口，持有在途 ``asyncio.Task`` 的强引用。

.. autosummary::
   :toctree: generated

   takler.tasks.shell.shell_render.ShellRender
   takler.tasks.shell.shell_runner.ShellRunner

定义校验
--------

``check_job_creation`` 在不真正提交作业的前提下渲染整棵 flow 树，发现
问题抛 ``JobSubmissionError`` ，用法见 :doc:`/guide/defining-flows` 。

.. autosummary::
   :toctree: generated

   takler.tasks.shell.check_job_creation
