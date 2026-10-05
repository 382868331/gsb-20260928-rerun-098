# 可配置应用运行框架

## 接入模块

每个可热替换组件实现 `Module`：

1. `Name` 返回唯一模块名，`Dependencies` 声明依赖模块。
2. `Validate` 只检查配置，不创建外部资源。
3. `Prepare` 按依赖顺序创建新资源，失败会按反序关闭本次已经创建的资源。
4. 资源实现 `Resource.Close`，关闭顺序始终与依赖启动顺序相反。

依赖图按拓扑顺序启动：被依赖者先准备，依赖者后准备；关闭时反向执行。

## 发布和恢复

- 首次启动调用 `Start`，配置只有在全部模块校验和准备成功后才进入发布；配置文件通过临时文件原子替换，随后立即切换活动版本。
- 已启动后再次调用 `Start` 返回 `runtime already started`，不会改写持久化配置。
- 后续发布调用 `Reload`；校验、准备、落盘或状态切换失败都保留当前活动版本和磁盘上的上一份成功配置。
- 成功 `Reload` 后，新版本立即对新租约可见；旧版本资源等待所有在途租约 `Release` 后关闭。
- 进程重启时调用 `Open`，只会恢复最近一次真正发布成功的配置并重新准备资源。

### 联合发布

多个关联模块可使用 `PrepareBatch(ctx, batchID, modules, updates)` 准备一个联合批次：

1. `updates` 是部分配置，只包含本次显式列出的模块；未列出的模块自动继承当前活动配置。
2. 批次始终形成一份完整配置：新租约要么拿到旧版本，要么拿到批次版本，不会看到模块交错的中间态。
3. 模块可实现 `CheckCompatibility(ctx, config, dependencies)`，在资源创建后、整体确认前校验与依赖模块新配置的兼容性；失败会回滚本批次已新增资源。
4. `Batch.Confirm` 或 `manager.ConfirmBatch` 原子落盘并发布；基线 revision 变化时返回 `ErrBaselineChanged`，旧批次不能覆盖新发布。
5. `Batch.Abandon` 或 `manager.AbandonBatch` 只释放本批次新增资源，仍被活动版本、其他候选或旧租约引用的资源保留。
6. 联合发布也进入普通历史；对发布前 revision 执行 `Rollback` 会准备并发布完整旧配置，生成新的 revision。

`CleanupRecords()` 将清理结果区分为 `added`（新增但未发布/已放弃或失败）、`retained`（仍有其他引用）、`released`（最后引用消失并关闭成功）和 `release-failed`（关闭失败，错误随记录返回）。

## 候选生命周期

需要先准备、稍后再决定是否上线时，使用显式候选 API：

1. `candidate, err := manager.Prepare(ctx, cfg)`：校验并准备全部资源，候选固定在当前活动版本（基线 revision）上。**准备成功不代表生效**：活动配置、磁盘文件和新租约都保持旧版本。
2. `manager.Candidates()` 查询仍开放的候选；`manager.ActiveConfig()` 查询当前配置；`manager.History()` 查询每次实际发布（含配置快照、revision 和来源）。
3. `candidate.Publish(ctx)` 原子落盘并切换活动版本。只有当候选基线仍是当前 revision 时才允许发布；若基线已被其他发布推进，返回 `ErrBaselineChanged`，必须放弃后针对新基线重新 `Prepare`，旧候选永远不能覆盖新版本。
4. `candidate.Abandon(ctx)` 按依赖反序释放候选资源并从候选列表移除，当前服务不受影响；重复调用幂等，不会二次关闭资源。
5. `manager.Rollback(ctx, revision)` 用历史发布中的配置执行一次真实切换，生成新的 revision 并落盘，旧记录仍保留在历史中；不存在的 revision 返回 `ErrRevisionMissing`。
6. `manager.Attempts()` 返回对照记录：每次实际切换（`Switched=true`）以及未生效的尝试——校验/准备失败、落盘失败、基线冲突、显式放弃——都带时间、类型、基线与错误信息。

准备或发布失败会释放本次候选已创建的资源，原活动版本保持不变；放弃、冲突或管理器关闭都不会在候选列表中残留状态。`Start`/`Reload` 内部等价于“准备 + 发布（+失败自动放弃）”。

## 使用租约

执行业务操作时通过 `Lease` 固定一个完整配置版本：

```go
lease, err := manager.Lease()
if err != nil {
    return err
}
defer lease.Release()

db := lease.Resource("db")
cfg := lease.Config("db")
```

不要把 `lease.Resource` 返回的资源保存到租约生命周期之外。重配置可能已经让该资源等待关闭；租约释放后，框架才会关闭旧资源。`Close` 同样会等待活动版本上的全部在途租约结束，然后再关闭资源。

## 失败语义

- 校验失败不会调用任何 `Prepare`。
- 准备失败只关闭本候选已经创建的资源，不影响当前版本。
- 候选被放弃或发布回退时，关闭操作幂等，成功关闭过的资源不会再次关闭。
- 重复启动、准备失败、上下文取消和管理器关闭都不会用失败配置覆盖磁盘。
- 只有完成资源准备、原子配置落盘和活动版本切换的配置，才可在下次启动时恢复。
