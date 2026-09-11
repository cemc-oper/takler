{{ fullname | escape | underline }}

{# autodoc 会把以函数为默认值的 dataclass 字段同时扫描成方法，产生
   duplicate object description 警告；RetryPolicy 的 clock / sleep 已在类
   docstring 的 Attributes 一节中说明，这里排除重复收录。 #}
.. autoclass:: {{ fullname }}
   :members:
   {%- if fullname == "takler.client.retry.RetryPolicy" %}
   :exclude-members: clock, sleep
   {%- endif %}
