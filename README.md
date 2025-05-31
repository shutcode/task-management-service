# task-management-service

一个通用的任务管理服务， 主要分为：
- 任务管理功能task-manager, 提供任务管理相关的RESTful API， 功能包括：
  - 创建任务
  - 查询任务详情
  - 列举任务
  - 更新任务（包括更新任务结果和状态）
  - 添加任务模板（每个模板标识一种任务类型）， 不同类型的任务有不同的参数模板，结果参数模板， 有不同的任务执行器
  - 删除任务模板
- 任务执行功能, task-worker服务根据注册的不同的任务启动不同的执行器
  - task-worker中有多个不同任务类别的执行器 goroutine
  - 执行器考虑用golang 插件的方式设计，可以动态注册
  - 执行器执行完成后，结果事件发送到kafka 消息队列
- 任务调度（任务模板中指定）
  - 支持crontab 类型的任务调度
  - 也支持实时异步任务调度，用户调用任务创建接口后持久化到数据库后，直接写到对应类别的执行器的kafka队列中

task management service defines api to create/delete/list/get tasks,
