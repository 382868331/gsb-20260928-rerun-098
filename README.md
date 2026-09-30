# configurable-runtime

一个可配置应用运行框架：按依赖顺序启动模块、原子切换配置版本、优雅关闭资源，并把当前发布的配置持久化到磁盘以便进程重启后恢复。

## 核心类型

- `Module`：模块实现 `Name`、`Dependencies`、`Validate`、`Prepare`。`Prepare` 返回该配置版本持有的 `Resource`。
- `Manager`：管理唯一的活动版本与磁盘记录，入口为 `Open` / `Start` / `Reload` / `Rollback` / `Lease` / `Close`。
- `Candidate`：由 `Prepare` 产生的、已准备好但尚未发布的候选版本，可 `Commit` 或 `Abort`。
- `Lease`：在途使用者持有的租约。通过它读取配置与资源，用完必须 `Release`。

## 接入步骤

1. 为每个模块实现 `Module`，并保证 `Prepare` 返回的 `Resource.Close` 幂等、可配合 `context` 超时。
2. 用 `NewManager(configPath, modules...)` 注册模块（依赖必须指向已注册模块，且不能成环）。
3. 首次启动用 `manager.Start(ctx, cfg)`；已有进程改配置用 `manager.Reload(ctx, cfg)`。
4. 每次处理请求前调用 `manager.Lease()`，在租约内通过 `lease.Resource(name)` / `lease.Config(name)` 取资源，结束时 `Release()`。不要把 `Resource` 缓存到租约生命周期之外。
5. 退出时调用 `manager.Close(ctx)`：等待所有在途租约结束，再按依赖逆序关闭活动版本与所有已退役版本。
6. 新进程用 `runtime.Open(ctx, configPath, modules...)` 启动，会恢复磁盘上**最后一次真正发布成功**的配置。

## 候选版本：准备、放弃、回退

```go
candidate, err := manager.Prepare(ctx, newCfg) // 校验 + 建好全部资源，不碰活动版本、不写盘
if err != nil { /* 当前版本与磁盘记录均未受影响，可继续使用旧版本 */ }
if changedMind {
    _ = candidate.Abort(ctx) // 放弃：按依赖逆序关闭候选资源
    return
}
if err := candidate.Commit(ctx, true); err != nil { // 发布；false 用于首次启动，true 用于替换活动版本
    // 提交失败时候选会自动放弃，活动版本与磁盘记录保持原状
}
```

`manager.Rollback(ctx, cfg)` 准备 `cfg` 并切换为活动版本，但**不覆写磁盘记录**，用于把内存状态回退到一份已知已持久化的配置；被替换版本作为在途使用者的资源会保留到最后一个租约释放。

## 失败语义保证

- `Validate` / `Prepare` 失败：已建好的候选资源按逆序关闭，活动版本与磁盘文件完全不变。
- 重复 `Start`（返回 `ErrAlreadyStarted`）、未启动时 `Reload`（`ErrNotStarted`）、管理器已关闭（`ErrClosed`）：拒绝发生在写盘之前，磁盘记录不被污染。
- 持久化写盘失败（原子的 temp-file + rename）：候选被放弃，活动版本与磁盘记录均不变。
- 只有写盘成功且活动指针完成切换，配置才算发布；因此 `Open` 恢复的永远是真实发布过的配置，而不是某次失败操作留下的半成品。
- 切换发布后，旧版本资源不会早于在途 `Lease` 结束而关闭；所有租约释放后按依赖逆序关闭。

## 验证

```bash
go test -race ./...
```

测试会直接读取磁盘上的持久化文件（而非只检查内存里的 `ActiveConfig`），核对准备失败、重复启动、发布后立即退出（重新 `Open` 恢复）等场景下磁盘记录与资源关闭版本是否一致。
