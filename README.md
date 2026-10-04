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

## 候选生命周期

需要先准备、稍后再决定是否上线时，使用显式候选 API：

1. `candidate, err := manager.Prepare(ctx, cfg)`：校验并准备全部资源，候选固定在当前活动版本（基线 revision）上。**准备成功不代表生效**：活动配置、磁盘文件和新租约都保持旧版本。
2. `manager.Candidates()` 查询仍开放的候选；`manager.ActiveConfig()` 查询当前配置；`manager.History()` 查询每次实际发布（含配置快照、revision 和来源）。
3. `candidate.Publish(ctx)` 原子落盘并切换活动版本。只有当候选基线仍是当前 revision 时才允许发布；若基线已被其他发布推进，返回 `ErrBaselineChanged`，必须放弃后针对新基线重新 `Prepare`，旧候选永远不能覆盖新版本。
4. `candidate.Abandon(ctx)` 按依赖反序释放候选资源并从候选列表移除，当前服务不受影响；重复调用幂等，不会二次关闭资源。
5. `manager.Rollback(ctx, revision)` 用历史发布中的配置执行一次真实切换，生成新的 revision 并落盘，旧记录仍保留在历史中；不存在的 revision 返回 `ErrRevisionMissing`。
6. `manager.Attempts()` 返回对照记录：每次实际切换（`Switched=true`）以及未生效的尝试——校验/准备失败、落盘失败、基线冲突、显式放弃——都带时间、类型、基线与错误信息。

准备或发布失败会释放本次候选已创建的资源，原活动版本保持不变；放弃、冲突或管理器关闭都不会在候选列表中残留状态。`Start`/`Reload` 内部等价于“准备 + 发布（+失败自动放弃）”。

## 联合批次

多个关联模块可以组成一个候选批次，只在全部必需模块就绪且兼容条件满足时一次性整体生效：

1. `batch, err := manager.PrepareBatch(ctx, BatchSpec{Modules: []BatchModule{...}, Compatible: fn})`：每个模块用 `BatchModule{Name, Raw, Required}` 声明。批次固定在当前 revision 上，并为其整个生命周期钉住基线中未变更模块的资源，因此确认后的集合永远是一组彼此兼容的配置，不会与其他模块的新版本拼接。
2. `batch.Modules()` 查看每个模块的准备状态（`ready`/`unchanged`/`failed`/`skipped`）、占用的资源和失败原因；`batch.Ready()` 只有在所有必需模块就绪且兼容条件成立时才为真。可选模块（`Required: false`）准备失败时降级为 `skipped`，沿用基线资源，不阻塞批次；必需模块失败或兼容条件不满足时批次进入 `failed`/`incompatible` 状态，返回错误并保留可查询记录。
3. `Compatible func(view map[string]ModuleConfig) error` 针对“新模块配置 + 其余基线配置”的完整视图声明跨模块兼容条件，准备完成后和确认前各检查一次。
4. `batch.Confirm(ctx)` 原子落盘并整体切换；必需模块未就绪返回 `ErrBatchNotReady`，基线 revision 已被其他发布推进返回 `ErrBaselineChanged`。读者通过租约只能看到旧的完整集合或新的完整集合。
5. `batch.Abandon(ctx)` 释放本批次新增资源，幂等；仍由旧批次、在途租约或其他候选批次使用的资源不会被关闭。`batch.Resources()` 返回每个资源的处理记录，区分新增（`new`）、旧批次仍占用（`kept`）和清理失败（`cleanup-failed`）；`manager.BatchRecords()` 保留所有批次（含失败、放弃、已确认）的可查询记录。
6. 两个候选批次可以交错准备；确认前若基线变化，未确认批次只能放弃后按新基线重新准备。`manager.Rollback(ctx, revision)` 同样是一次完整集合的联合切换，生成新 revision，旧资源等待在途租约结束后按依赖反序关闭。

资源按引用计数共享：活动版本、开放候选/批次和在途租约都持有引用，资源在最后一个引用释放时才关闭且只关闭一次。已经开始的读取通过 `Lease`（可用 `lease.Revision()` 查看归属）固定其批次归属，释放任何候选批次都不会破坏尚未结束的读取或其他候选批次的资源。

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
