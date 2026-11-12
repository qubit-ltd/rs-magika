# rs-magika 0.19.0 详细设计：有界采样与受管理异步推理

- 状态：待评审设计，尚未实现。
- 日期：2026-11-11。
- 当前版本：qubit-magika 0.18.0；下一破坏性版本：0.19.0。
- 与 qubit-mime 0.22.0 的异步后端契约配套。

## 0. 实施前置条件：先实现 rs-operation

**必须先实现并验收 rs-operation 0.1.0 的基础协议，再开始本设计的库实现。** 只有设计文档或空 crate 不满足此前置条件；本轮仅修订设计，不修改实现或 Cargo 清单。

上游入口为 [rs-operation 设计](../../rs-operation/doc/design-0.1.0.zh_CN.md)。开始实施前，必须固定其已验收版本/不可变提交并验证：同步/异步观察同一操作、取消树与单调期限、子操作 reserve/attach/commit、ResourceUse、OwnedCleanup、恢复所有权、结果与释放分离、Drop 托管及 shutdown 超期继续观察。若上游缺少必需能力，应先在 rs-operation 补齐契约和测试，不在下游复制一套替代机制。

### 0.1 统一职责与接口规则

- 本库直接依赖 `qubit-operation = "^0.1"`；同步托管接口也使用它，不把通用协议仅放在 async feature 后。具体领域异步 API 仍可由本库 async feature 控制；导入 crate 或构造纯配置不启动服务线程。
- 应用创建并注入共享 `OperationSupervisor`；本库创建受管理子 scope，不因领域服务各异而再创建私有通用 supervisor。应用关闭共享服务，本库只关闭自己受理范围内的子操作；显式独占的便利构造必须说明其服务归属。
- 同步/异步共用 `OperationContext`、`OperationHandle<T,E>`、`OperationObserver`、`ResourceUse` 和 `SettlementReport`。领域包装仅增加强类型结果/领域方法，不创建另一套 token、deadline、detach、恢复注册表或调度循环。
- `result()/result_blocking()` 一次性取业务结果；`released()/released_blocking()` 观察稳定终结。等待 future Drop 不取消仍持有的 handle，handle Drop 请求停止，detach 返回公共 observer；Unconfirmed 只能观察或超期返回快照，不能视为 Released。
- 请求提交必须先预留公共登记槽和领域容量，attach 领域 control 后 commit 才能产生外部副作用。停止信号和启动竞争使用公共线性化协议，未启动受理失败归还 owned 输入。
- 领域 worker、进程 reaper、原生 Session 和领域字节预算仍由本库管理；通用 supervisor 不执行阻塞 I/O 或推理。子任务从父 scope 切分配额，不重复争抢 root 槽，也不将同一资源登记两次。
- `OperationContext` 只携带共有上下文；领域 options 保留权限、版本、原子性、输出或采样策略。总期限向下收紧，不重置；清理不复用已取消的业务 token。生命周期停止请求不撤销已发生副作用。
- 不增加进度接口、progress feature 或事件总线依赖。业务自行使用 rs-progress / rs-event-bus；本库状态与诊断仅用于正确终结，不自动上报业务阶段。

### 0.2 共用验收与发布门禁

先通过 rs-operation 核心测试，再用本库假执行器验证：取消先于 commit 不启动、业务结果早于资源释放、队列满仍能停止/清理、Unconfirmed 持有资源、恢复 owner 不丢、借用对象不逃逸、外部 driver 丢失不伪造释放、公共 handle 的同步/异步对等。以下领域专属验收在此基础上叠加，不能用领域测试替代上游前置验收。

## 1. 为什么需要独立版本设计

需要。仅升级文件与命令 API 不能解决模型推理阻塞。当前异步路径在异步采样结束后调用同步 `guess_from_extracted`，其内部获取 `std::sync::Mutex<Session>` 并执行 `identify_features_sync`。模型锁竞争和 CPU/native 推理都可能占住调用方异步线程。字节内容路径还会继承 MIME 同步默认 async hook。

此外，构造 detector 会初始化模型；若在请求线程同步创建，同样产生阻塞。因此下一版必须覆盖准备、采样、推理、会话所有权、排队和关闭，不能只补一个 async 检测方法。

目标是运行时无关的异步调用、有限模型/线程/内存数量、保留随机访问与一致性保证、明确不可中断推理的取消语义。模型准确率调整、新模型格式、GPU 调度、多进程推理服务与动态自动扩容不在本版范围。

## 2. 当前实现依据

- `src/magika_mime_detector_builder.rs` 创建 session，并由 detector 持有 Mutex。
- `src/magika_mime_detector.rs::guess_from_extracted` 对 ruled 结果直接映射，对 features 加锁同步推理。
- `guess_from_async_input` 调用 `FeaturesOrRuled::extract_async` 后回到同步推理。
- `internal/async_provider_input.rs` 已区分保留随机访问 session 与 legacy 路径读取，并累计 ReadBudget；这部分设计应保留。
- 上游本地已安装 magika 1.1.0 的 Features 拥有 Vec<i32>，可作为 owned 工作数据；跨线程能力需通过 Send 编译断言固定，不能将 Session 锁 guard 或借用 provider 一并送往 worker。
- 模型和 ONNX Runtime 初始化错误目前属于 ProviderFailure 初始化失败；不得错误映射为“provider 缺失”来触发 OnAbsence fallback。

## 3. 架构和职责

```text
请求容量准入 → 输入范围/版本检查 → 有界异步采样
→ 特征提取/规则分类 → 有界推理队列 → worker 独占 session → MIME 候选
→ rs-mime 异步细化 → 终结报告
```

拆为三个域：输入采样负责对象一致性与读取预算；特征计算负责 CPU 操作；推理 worker 独占 session。网络/文件等待期间不占 session，也不持有模型锁。MIME 结果选择与 ffprobe 细化继续由 rs-mime 负责。

新增 `MagikaRuntime`，注入 OperationSupervisor，并只管理领域 worker 与模型容量；共享有限请求容量、特征 worker、推理 worker、模型准备状态；通用诊断、操作/恢复注册表归公共 supervisor。detector/provider 都持有 runtime handle；Clone 不复制模型、不扩容 worker。同步和异步入口共享同一个 runtime 容量与 session，不建立两套隐藏模型。

## 4. 公共 API 草案

以下是目标 API 摘要，不是当前版本可编译示例。

```rust,ignore
impl MagikaRuntime {
    fn with_supervisor(self, supervisor: OperationSupervisor) -> Self;
    fn builder() -> MagikaRuntimeBuilder;
    async fn prepare_async_in(&self, scope: &OperationScope) -> MimeResult<()>;
    fn prepare_in(&self, scope: &OperationScope) -> MimeResult<()>;
    async fn shutdown(&self, mode: MagikaShutdownMode) -> MagikaShutdownReport;
}
impl MagikaMimeDetectorBuilder {
    fn runtime(self, runtime: Arc<MagikaRuntime>) -> Self;
    fn build(self) -> MimeResult<MagikaMimeDetector>; // 同步，显式允许阻塞
    async fn build_async(self) -> MimeResult<MagikaMimeDetector>;
}
impl MagikaMimeDetectorProvider {
    fn with_runtime(runtime: Arc<MagikaRuntime>) -> Self;
}
```

新增 `MagikaExecutionOptions`：请求容量、采样并发、特征并发、推理队列容量、session 数、队列字节预算和 worker 内线程配置。所有配置在 runtime 构造时固定；请求只能收紧预算，不改变其他请求使用的 session 参数。

实现 rs-mime 0.22.0 的同步 backend 与独立 AsyncMimeDetectorBackend；异步字节、prefix、reader/provider 路径、细化和创建入口均接收公共 OperationScope/OperationContext，且都必须显式实现或明确拒绝不支持。Complete 输入要求沿用当前模型需要的完整逻辑资源，prefix 不能假扮完整对象。

异步 provider 工厂必须复用注入 runtime；未注入 runtime 的便利构造必须显式配置公共 supervisor，并建立明确归 detector 所有的领域服务，文档提示每次构造有模型成本。注册与查询 descriptor 不初始化模型；推荐服务器启动时建立共享 runtime 并执行 prepare_in，而不是每次请求 build。

## 5. 模型准备与线程归属

每个推理 worker 在线程内部创建并独占 Session，从创建到销毁不跨线程移动 Session，不依赖 Session 实现 Sync/Send。初始化发生在 worker，prepare_async_in 只等待结果；同步 prepare_in 可阻塞调用方。

相同 runtime 的并发 prepare_in 合并为一次准备过程，全部 session 准备成功后才 Ready；部分失败停止新受理并回收已创建 session，返回 typed 初始化错误。失败后不自动无限重试；显式 retry_prepare 在没有活动请求时重新尝试。每个 runtime 记录一次失败原因但脱敏展示。

保留现有 bundled-onnxruntime feature 和显式 runtime 加载配置。准备不增加业务请求路径上的自动网络下载；构建阶段下载行为继续由原 feature 控制。native runtime 的线程设置只在其允许的作用域设置一次，不能由多个 runtime 相互覆盖全局配置；不支持局部设置时报告实际限制，拒绝冲突配置。

## 6. 采样、特征提取与对象一致性

优先保留现有 AsyncRandomAccessReader session，使用打开时长度和同一对象的范围读取。只有 provider 明确报告不支持 session 时，才允许显式配置的 legacy route；session 打开失败、权限失败、版本不匹配不能降级为重新按路径打开。

legacy 路线要求 ResourceVersion 时每次读取都应用它；没有版本且允许 best-effort 时在报告中标记一致性未保证。保留 session 不等于内容不可变，报告真实的 provider 一致性级别，不宣称跨范围原子快照。

ReadBudget 在 I/O 前按每次逻辑采样请求预留一次；同一请求内的短读、provider 分段及最多 8 次连续无进展 Interrupted retry 不重复计费。一次请求完成后，新的重叠逻辑请求仍重新计费。第 9 次连续无进展 Interrupted 直接失败；超预算必须在发起 I/O 前失败。provider 的单段上限与读取总预算同时满足。大逻辑资源可用有限采样识别，不把文件全量搬入内存。超大文件长度/offset 使用检查算术，短读、零进展和超范围作为 typed 输入失败。

为避免 extract_async 内的 CPU 部分继续占用调用方线程，本版采用采样桥接：特征 worker 执行上游同步特征提取，借助有界请求通道向前台异步采样器请求 range；前台只对 provider 做异步读取，将 owned 有限字节块回送。特征 worker 可阻塞等待，但不占 session，不阻塞 async poll。每个采样桥只有一个未完成 range，通道容量 1，range 按 provider max_segment 拆分，总字节仍由同一个预算管理。

这种方式复用上游的取样算法，不复制其私有逻辑，也不要求上游暴露特征布局。规则分类也在特征 worker 完成；ruled 结果绕过推理队列。Future 丢弃时关闭桥接通道，唤醒等待的 worker；禁止 worker 无限等一个已经销毁的异步采样器。range 通知和取消通知使用独立控制信号；worker panic、通道断开和 provider 失败均关闭两端并归还预算，不能依赖有空位才能发送取消。

对于已有 owned bytes/同步 reader，特征 worker 直接使用 owned 输入或明确同步入口，不经异步 provider 桥。异步借用 reader 必须由前台桥接，worker 不持有非静态借用。

## 7. 推理容量与内存

建议默认每 runtime：活动检测最多 4 个、已受理请求队列 64 个、等待者 64 个、特征 worker 2 个、推理 session/worker 1 个、推理队列 16 项、owned 数据总额 16 MiB。模型本身的 native 内存单独测量和报告，不冒充受该 payload 预算限制。

输入准备前先取得检测容量，避免无限请求先采样出 features 再争抢队列。推理队列同时按任务数和 features 的实际内存计量；读取缓冲、feature 数据和待分类 bytes 共享 runtime owned 上限。上游隐藏临时分配通过测量报告，文档不声称整个进程内存被严格限制为 16 MiB。

默认一个 session 延续当前串行推理语义；提升 session 数显式增加模型副本和 native 内部线程成本。启用更多 session 之前报告模型内存、特征时间、队列等待和推理吞吐，禁止按 CPU 核数默认复制模型。初版不做自动 batch；将来的 batch 必须有等待期限和每请求取消语义。

特征 worker、推理 worker 和通知控制独立，不使用 rs-command 的命令槽或 rs-local-files 的 I/O 槽。推理 worker 不阻塞等待自身队列中的任务；先取得推理许可再转移 feature 所有权，避免队列相互等待。

## 8. 取消、期限与结果交付

状态为：排队、采样、提取特征、等待推理、推理中、细化、终结。

| 时点 | 取消行为 |
| --- | --- |
| 尚未受理/排队 | 不读取、不初始化额外模型、不推理 |
| range 读取中 | 停止发出后续请求；按 provider 契约结束已启动读取 |
| 等待桥接结果 | 关闭通道并唤醒 worker，返回取消 |
| 特征计算中 | 协作检查，当前不可中断计算完成后丢弃结果 |
| 等待推理 | 从队列移除，不占用 session |
| native 推理中 | 不强杀线程/Session，等待返回，结果按交付规则丢弃 |
| ffprobe 细化中 | 交由 MIME 的命令取消与清理 scope 管理 |

请求总期限由公共 OperationContext 传入，覆盖容量等待、采样、推理排队、推理和细化；MimeOperationOptions 只负责领域预算收紧；子阶段只使用剩余期限。超时或取消不承诺中断 native 推理。推理提交先 reserve 子操作和领域许可，attach 实现 ManagedOperationControl 的 MagikaInferenceControl，再 commit。公共 handle 的 result 可以先确定停止，released 必须等 worker 不再访问输入/features；观察等待超期返回公共 observer/Unconfirmed，领域 Session 和资源许可继续持有。

结果交付采用单一线性化点：若 native 结果到达后先成功提交结果，稍晚取消只记伴随事实；若取消先被 operation 状态机接受，则后到结果不交付为成功。缓存不得写入取消请求的结果；本版不新增推理结果缓存。

同步调用在正常线程等待同一 runtime 作业；不能在 async poll 内调用同步检测。同步和异步均计入容量并遵循相同预算。调用方主动以阻塞同步方法运行是显式选择。

## 9. 错误隔离、关闭与观察

- 采样错误保留 provider/fs source 与 offset/范围事实，默认不泄漏原始路径。
- 预算、容量、取消和期限映射到 MIME 的对应 typed 变体，不当作“未识别”。
- worker panic 不导致所有等待者永久挂起：当前请求失败，session 标为不可复用并销毁；runtime 进入 Degraded，通过公共子操作报告 Panicked/领域错误，拒绝超出剩余容量的作业。恢复需显式 prepare/rebuild，不自动反复重建。
- native 库进程崩溃不在进程内隔离保证中；本版不声称可从 segfault 恢复。
- shutdown 仅停止本 runtime 领域受理，使用公共 scope 的 Drain/Cancel 与 observer 等待；领域 worker 最后退出。应用注入的共享 supervisor 不由本库关闭。超期不销毁 Session，不建立私有停止状态机。
- runtime 领域句柄 Drop 不 join；公共 OperationHandle 的 Drop 遵守 rs-operation 协议；worker 拥有必要生存引用，退出后自行销毁 Session。请求取消标记与作业注册表由注入的公共 supervisor 维护，不依赖调用方继续 poll；native 推理退出后即使结果接收者已销毁也会归还容量并记录未观察失败。可靠进程退出需要显式 shutdown。

记录初始化耗时、采样实际字节/次数、特征耗时、推理等待/执行耗时、活动模型数和未观察失败。默认日志不包含文件内容、features、完整路径或 native 错误原文；原始 source 可供应用显式检查。

## 10. 依赖、迁移与发布顺序

先实现并验收 rs-operation ^0.1，再落实 qubit-fs ^0.4、qubit-mime ^0.22 的托管 SPI/async backend，最后实现本库 ^0.19。qubit-io 与上游保持一致；magika/ort 不因该设计主动升级，用锁定版本的 Send/特征/线程测试固定兼容性。rs-command 和 local-files 是 MIME 的领域依赖，不拿其 worker 执行推理。

MagikaRuntime 通过公共 supervisor 创建服务子 scope，model prepare、特征桥接、inference 分别登记领域 control；Session/owned features 通过 ResourceUse 持有至真实释放。迁移前的 Mutex<Session> 变为领域 worker 独占 Session，但不再实现独立的 Magika token、操作注册表、detach 或恢复机制。

采样桥的前台 Future 和特征 worker 都是受管理子操作。前台绑定外部运行时时登记 driver lease；取消关闭桥后 worker 返回，才能确认其输入引用已释放。同步与异步共享 runtime 容量和公共上下文，但推理队列、模型内存和 native 线程数仍由本库配置。

直接本地异步路径依赖迁移后的 rs-fs-local 或显式本地服务，不可偷偷同步 File::open。新适配器尚不可用时返回明确 Unsupported；owned bytes/远程 provider 路线可独立验证。

发布链：operation → command/local-files/fs → 适配器与 mime → magika → 全链消费测试。复用 scope 不能双重登记或重复预算。保持同步 MIME 映射、对象采样一致性和初始化错误分类；不增加 progress/event-bus 依赖。

关联设计：[mime](../../rs-mime/doc/design-0.22.0.zh_CN.md)、[fs](../../rs-fs/doc/design-0.4.0.zh_CN.md)、[local-files](../../rs-local-files/doc/design-0.6.0.zh_CN.md)。

## 11. 验收与性能证据

1. 用假 Session 闸门阻塞初始化、特征计算、锁等待等价阶段和推理，异步执行器心跳仍能推进；不以真实推理足够快替代验证。
2. 同步与异步对同一测试集产生相同 MIME/None 和 typed 错误；ruled 结果不占 session。
3. 巨大逻辑文件只读取有限 ranges；session 路线不因路径替换换对象，legacy 版本变化正确失败；累计重试不能绕过预算。
4. 采样桥通道关闭、provider 取消、短读、零进展和单段限制不会令 worker 永久等待；队列满仍能传递取消。
5. 在各状态取消，验证排队作业不推理，native 运行作业取消后资源仍持有，终结后容量准确归还。
6. 并发 prepare 只创建配置数量的模型；部分初始化失败、worker panic、服务关闭和新请求竞争均有确定结果。
7. 与 MIME file/ffprobe 同时运行时不同服务容量相互隔离，取消精细分类后临时资源正确清理。
8. feature 默认/关闭 bundled runtime、inventory 开关与文档消费者构建均通过；加入 owned Features 的 Send 断言，Session 不要求跨线程。

性能记录冷启动/热调用、每模型 native 内存、特征阶段线程、推理线程、峰值 payload 和 native RSS、队列等待、取消到真正终结的延迟。先比较 0.18 单 Session 基线，再评估更多 session；不预设异步接口会提高单次模型计算速度。
