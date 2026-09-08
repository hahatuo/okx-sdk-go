# OKX Go SDK

OKX v5 REST 与 WebSocket SDK。最低 Go 版本为 1.27；JSON 使用固定版本的 `github.com/go-json-experiment/json`，WebSocket 使用 `github.com/coder/websocket`。

```go
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
client := okx.NewClient()
tickers, err := client.Market.Ticker(ctx, okx.TickerRequest{InstID: "BTC-USDT"})
```

可运行的订单簿示例见 [examples/orderbook](examples/orderbook/main.go)：`go run ./examples/orderbook`，按 Ctrl+C 关闭。

**连接与数据所有权**

- `Connect` 的 context 管理整个连接生命周期；`ConnectWithTimeout` 单独限制初次连接等待。连接认证及已有订阅恢复收到 ACK 后，`Ready()` 才为 true；订单簿是否可用另看 `Book.Ready()`。
- 回调直接运行于读循环，必须及时返回。回调内不要调用等待 ACK 的订阅、交易方法或同步 `Close()`。关闭时调用非阻塞 `RequestClose()`，由拥有者调用 `WaitClosed(ctx)` 或 `Close()` 等待退出。
- 原始私有回调保留原始 data，推送 arg 缺少筛选字段时需自行筛选行；typed 辅助方法负责行过滤。
- 原始 `Message.Data` 仅在回调期间有效；typed 回调的 `Data` 是独立解码结果，可保留。`WSLocalOrderBookMessage.Book` 及其 bids/asks 为借用的只读视图，仅在回调期间访问；跨 goroutine 使用需要复制并同步发布。
- 同一参数的重复订阅会被拒绝。订阅 handle 只关闭自己的订阅；已经开始的回调可能继续完成。
- 序号缺口、非法深度数据和断连会使本地订单簿失效。客户端通过现有重连循环重新认证、订阅并等待新快照；同连接上的其他订阅也会恢复。系统事件 `reconnecting`、`ready`、`disconnected` 通过 `SystemEvents()` 提供；事件队列满时会记录日志并丢弃通知，当前状态应通过 API 查询。
- 默认完整消息上限为 1 MiB，可用 `WithWSReadLimit` 调整。请求超时默认 10 秒，覆盖限流、写入及 ACK 等待，可用 `WithWSRequestTimeout` 调整。

**交易与规则**

REST/WS 都需检查逐行 `OrderAck.APIError()`；外层成功不能代替检查 `sCode`。批量最多 20 条。批量 `PartialError` 会保留返回数据。现有单条/数组返回签名保持兼容。

订单发送后的超时或断连表示结果可能未知。使用唯一 `clOrdId`，通过订单查询或订单推送核实结果后再决定是否重试。

`InstrumentRules.PreparePlaceOrder` 核对标的身份，采用精确商余数对齐步长。现货 quote 数量及杠杆市价买单的 quote 数量不能直接套用 base 的 lot/min 规则，因此拒绝自动规范化。

产品配置更新由调用方管理：通过 `Public.Instruments` 获取初始列表并调用 `InstrumentCatalog.Update`，随后用 instruments 推送或定期 REST 查询更新。`catalog.Rules` 返回独立规则及版本号，旧快照不会随更新改变。下单前是否接受旧版本由策略决定。

使用 `instIdCode` 且启用限流时，将对应环境的 catalog 通过 `WithInstrumentCatalog` / `WithWSInstrumentCatalog` 注入，以便 REST/WS 将两种标识映射到同一额度。未加载的代码或不匹配的标识会在发送前报错。实盘、模拟盘使用各自的 catalog。六个类型化 WS 交易方法仅发送 `instIdCode`：只传 `InstID` 时从 catalog 补全代码，未加载映射则在本地报错；REST 请求保留原有字段。低层 `WSClient.Do` 原样发送参数，调用方负责使用当前 wire 契约。

**限流范围**

默认 limiter 在进程内共享滑动窗口：公共接口按 IP 端点规则计费，交易按操作、标的（期权按系列）、单条/批量计费；下单与改单另计子账户 1000 orders/2s 总额度；catalog 中明确标记为 SPOT/MARGIN 的订单豁免该总额度，仍受品种额度约束。混合批次只统计非豁免行。请为 REST/WS 加载一致的产品类型信息；未加载或类型未知时保守计入总额度。WS 连接尝试按 3/s，login/subscribe/unsubscribe 按每连接 480/h 计费。

默认以 API key 的哈希区分私有额度。同一用户的多个 key 应在 REST/WS 设置相同的 `WithRateLimitScope` / `WithWSRateLimitScope`。跨进程额度需要注入共享协调的 `PolicyRateLimiter`；SDK 无法发现其他进程或应用的流量。公共 IP 默认按本进程合并，多出口场景会较保守。

默认采用普通账户基线。带单标的的 4/2s 限额、VIP 动态提升等账户特定策略需自行配置。`DefaultRateLimiter().Set(key, rate, burst)` 可按规范键覆盖为保守窗口；例如 `user:UID:trade:place:single:BTC-USDT`。共享同一作用域的客户端应使用一致配置。

`NewRateLimiter` 提供独立自定义 token bucket，多维请求按顺序等待，取消不会回退已消费额度。兼容旧 `Wait(ctx,key)` 接口，旧适配器按成本逐次调用，无法提供多维原子准入；需要该保证时使用默认 limiter，或实现具有原子准入语义的 `PolicyRateLimiter.WaitRequests`。传入 nil 可关闭 SDK 限流。未知 WS 操作采用 10/s 的保守默认值，不代表已核对该操作的交易所额度。

**验证**

```sh
make build vet test-contract race
make bench
```

普通目标只运行离线测试，不读取账户密钥。官方样本及来源、日期、原文修正记录位于 [testdata/contracts/manifest.json](testdata/contracts/manifest.json)。深度 Apply 有预热后零分配断言；解码与完整 dispatch 的成本由 benchmark 单独报告，不能视为网络端到端零分配。

集成测试凭据放在已忽略的 `.okx/credentials.json`，格式见 [无凭据样本](testdata/credentials.example.json)。联网测试按用途显式选择 Makefile 目标：`test-public-rest`、`test-live-readonly`、`test-demo-trading`、`test-demo-market-roundtrip`、`test-demo-ws-private`、`test-ws-public`。其中 demo market roundtrip 会成交。
