异常
=====

takler 自有异常全部定义在 ``takler.exceptions`` ，根类是
``TaklerError`` 。服务端把异常翻译成 ``ServiceResponse.flag`` 分类码
（确切类型查表，不走继承链），客户端再按分类码决定退出码；查表规则见
:doc:`/develop/protocol` ，码值全表见 :doc:`/operation/reference` 。

异常层次
--------

* :py:exc:`~takler.exceptions.TaklerError`

  * :py:exc:`~takler.exceptions.InvalidRequestError` 

    * :py:exc:`~takler.exceptions.NodeNotFoundError`
    * :py:exc:`~takler.exceptions.InvalidNodePathError`
    * :py:exc:`~takler.exceptions.NodeTypeError`
    * :py:exc:`~takler.exceptions.UnsupportedValueError`
    * :py:exc:`~takler.exceptions.FlowStateError`

  * :py:exc:`~takler.exceptions.ExpressionSyntaxError` 
  * :py:exc:`~takler.exceptions.JobSubmissionError`
  * :py:exc:`~takler.exceptions.ZombieError`
  * :py:exc:`~takler.exceptions.TransportError`

    * :py:exc:`~takler.exceptions.ClientConnectionError`

  * :py:exc:`~takler.exceptions.ServerResponseError`
  * :py:exc:`~takler.exceptions.PermissionDeniedError`
  * :py:exc:`~takler.exceptions.SecurityConfigError`

没有专属分类码的 ``TaklerError`` 子类归 ``takler_error`` （ ``1`` ），
非 takler 异常归 ``internal_error`` （ ``99`` ）。

全部异常
--------

.. autosummary::
   :toctree: generated

   takler.exceptions.TaklerError
   takler.exceptions.InvalidRequestError
   takler.exceptions.NodeNotFoundError
   takler.exceptions.InvalidNodePathError
   takler.exceptions.NodeTypeError
   takler.exceptions.UnsupportedValueError
   takler.exceptions.FlowStateError
   takler.exceptions.ExpressionSyntaxError
   takler.exceptions.JobSubmissionError
   takler.exceptions.ZombieError
   takler.exceptions.TransportError
   takler.exceptions.ClientConnectionError
   takler.exceptions.ServerResponseError
   takler.exceptions.PermissionDeniedError
   takler.exceptions.SecurityConfigError
