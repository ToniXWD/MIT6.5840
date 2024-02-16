# 1 整体结构
1. 结构: 1个`controller` + 多个`replica groups`
2. 为了负载均衡 -> 能够迁移分片
3. 需要处理请求和配置项更新几乎同时到达的情况
4. 建议的实现: raft也要保存配置项
5. 配置更新后, 新的负责某分片的`replica groups`需要从旧的`replica groups`复制旧分片

# 2 Shard controller
1. `clients` 和 `servers` 都会询问 `controller` 最新的配置信息
2. `RPC`
   1. `Join`:
      1. 重新分片时尽量平均
      2. 重新分片时移动的分片尽量少
      3. 允许重用`GID`
   2. `Leave`
      1. 重新分片时尽量平均
      2. 重新分片时移动的分片尽量少
   3. `Move`
      1. 将某个`shard`分配给某个`group`
   4. `Query` 
      1. 返回配置信息
      2. 必须反映在其之前做出的配置信息更改
3. 需要滤除重复的`RPC`
4. 执行碎片再平衡的状态机中的代码需要具有确定性