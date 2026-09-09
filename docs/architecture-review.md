# dubbo-py 架构评审与改进项清单

> 评审对象：`feiyuw/dubbo-py`（master @ fc18b77）
> 评审依据：
> - Hessian 2.0 Serialization Protocol（http://hessian.caucho.com/doc/hessian-serialization.html）
> - Apache Dubbo TCP 协议规范（https://dubbo.apache.org/zh-cn/overview/reference/protocols/tcp/）
> - 仓库源码与现有测试
>
> 说明：本文只列问题与建议，**不修改任何代码**，供评审后决策。

---

## 0. 结论先行

项目定位清晰（dubbo 协议编解码 + provider/consumer 模拟，用于功能自动化测试），代码量小（核心约 1000 行）、单一依赖（kazoo），已实现 dubbo 帧头 + hessian2 常用标量/容器/对象的基本编解码，并有可运行的端到端示例与 13 个通过的单元测试。

但从"与主流 Java dubbo 服务互通"和"架构可演进"两个维度看，存在一批**会造成编解码结果错误或崩溃的协议符合性缺陷**，以及若干架构级短板。核心风险集中在：

1. **Hessian2 三张引用表未分离实现**（值引用 / 类定义 / 类型引用），导致循环引用、共享引用、类型复用全部不可用；
2. **长字符串（>64K）不分块、二进制分块解码崩溃、对象长形式 `O` 未实现**等 spec 边界 case 未处理；
3. **整数超 int32 范围时静默截断**而非自动升为 long；
4. **客户端响应无 request_id 关联**，并发/乱序下串包；
5. **类型系统靠污染 `__builtins__` 的 `long`/`double` hack**支撑，脆弱且不可演进。

建议优先级：**先修 P0 互通性缺陷 → 补 P1 功能缺口 → 再做 P2 架构与工程质量重构**（见 §6）。

---

## 1. 当前架构概览

```
                 ┌────────────────────────────────────────────┐
                 │                dubbo/codec/hessian2.py      │
                 │   (776 行，职责严重混合)                     │
                 │  ┌──────────────────────────────────────┐   │
                 │  │  Dubbo 帧头编解码 (magic/flag/id/len) │   │
                 │  ├──────────────────────────────────────┤   │
                 │  │  Hessian2 序列化 (encode_object/     │   │
                 │  │  Decoder._read_object/_read_int...)  │   │
                 │  ├──────────────────────────────────────┤   │
                 │  │  消息模型 (DubboRequest/Response/     │   │
                 │  │  HeartBeat)                           │   │
                 │  ├──────────────────────────────────────┤   │
                 │  │  JVM 描述符转换 (_desc_to_cls_names)  │   │
                 │  ├──────────────────────────────────────┤   │
                 │  │  namedtuple 动态对象工厂 (new_object) │   │
                 │  └──────────────────────────────────────┘   │
                 └────────────────────────────────────────────┘
                       ▲                    ▲
        ┌──────────────┴──────┐   ┌────────┴──────────────┐
        │  dubbo/client.py    │   │  dubbo/server.py       │
        │  DubboClient        │   │  DubboService /        │
        │  (同步 TCP + Queue) │   │  ThreadingTCPServer    │
        └──────────────┬──────┘   └────────┬──────────────┘
                       │                   │
                 ┌─────┴─────┐       ┌─────┴──────┐
                 │ dubbo      │       │  kazoo     │
                 │ utils.py   │       │  (ZK 注册) │
                 └───────────┘       └────────────┘
        ┌───────────────────────────────────────────────┐
        │  dubbo/java_class.py : Java 类型→Python 映射    │
        │  dubbo/__init__.py  : 向 __builtins__ 注入     │
        │                     long/double               │
        └───────────────────────────────────────────────┘
```

**模块职责清单**

| 模块 | 当前职责 | 存在的职责越界 |
|---|---|---|
| `codec/hessian2.py` | 帧头 + hessian2 + 消息模型 + desc 转换 + 对象工厂 | 一个文件承担了 codec / protocol / model 三层 |
| `client.py` | 连接、收发循环、心跳、telnet 命令 | 收发循环与请求关联耦合，无连接管理 |
| `server.py` | 服务注册、TCP 服务、handler 分发、心跳 | 注册逻辑内嵌，与网络层耦合 |
| `utils.py` | 字节/整数/时间/IP 工具 | 无（基本纯粹），但 `int_to_bytes` 缺固定宽度语义 |
| `java_class.py` | Java 类型→Python 类型映射 | 依赖 `__init__` 注入的 `long` |

---

## 2. 协议符合性缺陷（对照 Spec）

### 2.1 Dubbo 帧头（TCP 协议头 16 字节）

帧头结构本身实现正确（`0xdabb` magic、flag 的 req/twoway/event/serialization-id 位域、8 字节 invoke id、4 字节 body length 均与 spec 一致）。

| ID | 严重级 | 位置 | 问题 | 建议 |
|---|---|---|---|---|
| **P0-F1** | 高 | `hessian2.Decoder.decode()` | 读取 `proto = flag & 0x1f` 后**仅打日志，未校验序列化 ID==2**。fastjson(6)/jdk(1)/gson 等其它序列化的包会被错误地按 hessian2 解析，报错信息误导。 | 显式校验 `proto == HESSIAN2`，否则抛"unsupported serialization id"异常 |
| **P0-F2** | 高 | `hessian2._decode_response_body()` | `status_code == 0`（RESPONSE_WITH_EXCEPTION）被赋值给 `data` 而非 `error`。**已复现**：OK 头 + 异常体 → `(data='boom', error=None)`。 | `0→error`、`1→data`、`2(NULL)→data=None` 三分支 |
| **P1-F3** | 中 | `hessian2.DubboResponse._get_body()` | 异常路径直接 `encode_object(error)`，**未加 RESPONSE_WITH_EXCEPTION(0) 状态字节**；与帧头 status 语义隐式耦合。status=OK 却带 error 时会产出非法字节流。 | 让响应体编码规则与 spec 完全一致：error 非空→`0x90`+error，否则 `0x91/0x92`+data |
| **P1-F4** | 中 | `hessian2.DubboResponse` / `server._get_dubbo_request_handler` | 仅定义 `OK=20`、`UnknownError=90`，缺 30/31/40/50/60/70/80/100 等 spec 状态码；服务端将一切异常统一映射为 90，丢失 BAD_REQUEST(40)/SERVICE_NOT_FOUND(60) 等语义。 | 补齐状态码常量；服务端按异常类别细化映射 |

### 2.2 Hessian2 序列化（核心）

| ID | 严重级 | 位置 | 问题 | Spec 依据 | 建议 |
|---|---|---|---|---|---|
| **P0-H1** | 高 | `encode_object()`（str 分支） | 字符串长度 > 65535 **不分块**，`b'S'+int_to_bytes(len)` 且 `int_to_bytes` 无固定宽度，产生 >2 字节长度前缀，破坏 16-bit length 字段。**已复现**：70000 字符 → 3 字节长度头。 | `string ::= 'R'(非终块)+'S'(终块)`，每块 64K | 按 64K 分块，首块用 `R`、末块用 `S`，长度恒 2 字节大端 |
| **P1-H2** | 中 | `encode_object()`（str 分支） | 长度语义混用：spec 的长度单位是 **UTF-16 字符数**，实现用 Python `len()`（码点数）配 `field.encode()`（UTF-8 字节），对非 BMP/代理对分支不一致。 | spec 4.12「length 是 16-bit 字符数」 | 统一以 UTF-16 单元数或编码后字节数为准，二者择一并文档化 |
| **P0-H3** | 高 | `Decoder._read_object()`（0x52/0x53 分支） | 解码将 `R`(非终块) 与 `S`(终块) 同等当作单块返回，**未实现 R 后延续读**，>64K 字符串解码结果错误。 | `string ::= 'R' ... string`（递归） | 循环读块直到 `S` 终块 |
| **P0-H4** | 高 | `Decoder._read_object()`（0x41/0x42 分支） | 二进制分块分支对 `bytes` 与 `int` 做 `data >= 0` 比较 → **TypeError 崩溃**；且未正确维护 A(非终)/B(终) 块延续。**已复现**。 | `binary ::= 'b' <data> binary | 'B' <data>` | 修比较、按 int 字节流读齐、区分终块/非终块 |
| **P0-H5** | 高 | `Decoder._read_object()` | 对象长形式 `'O'`(0x4f) 写成了 `tag == ord(b'0')`（0x30=数字零，实为紧凑字符串），导致 `object ::= 'O' int value*` 返回 `unknown code "79"`。**已复现**。 | spec 4.10.2「object ::= 'O' int value*」 | 改为 `ord(b'O')`，按 class-def idx 读取字段值并实例化 |
| **P0-H6** | 高 | `Decoder` 整机 | **三张引用表未分离**。仅用单个 `self._refs` 存类定义；`0x51`(value ref) 直接 `self._refs[idx]` 拿到的却是 class-def dict。循环引用/共享引用解码必然错。**已复现**：`H 0x91 0x51 0x90 Z`（自引用 map）→ IndexError。 | spec §5：值引用 / 类定义 / 类型引用三张独立表 | 分离三表；list/map/object **读取时立即入值引用表**，ref 按值表下标取 |
| **P1-H7** | 中 | `Decoder._read_map/_read_list` | **类型引用表未维护**：map/list 的 type 字符串被读取后丢弃不入表，后续 int 类型引用无法解。 | spec §5.3「type 必须入类型表」 | 维护 type 表，支持 int 类型引用 |
| **P1-H8** | 中 | `Decoder._read_object()` | 变长 list 两种形式 `x55`(typed) / `x57`(untyped) 直接 `raise 'unimplemented'`。 | `list ::= 'U' type value* 'Z' | 'W' value* 'Z'` | 按变长形式实现，读到 `Z` 终止 |
| **P1-H9** | 中 | `Decoder._read_map()` | typed map（`M`）读 type 后**丢弃并统一返回 dict**；spec 中 typed map 应反序列化为对象，untyped 才 dict。与强类型 Java 服务（期望 `Car` 而非法 `dict`）不互通。 | spec 4.8「type 描述 map 类型，可代表对象」 | 按 type 构造对应 Java 对象 |
| **P1-H10** | 中 | `Decoder._read_map()` | 实现用 `except RuntimeError: code=b'Z'` 等**异常做控制流**，逐字节状态机缠绕、脆弱，typed/untyped 分支逻辑混乱。 | — | 重构为显式 state machine |
| **P1-H11** | 中 | `encode_object()`（namedtuple/class-def 分支） | encode 侧**从不生成 `0x51` 引用**，且 `cls_names` 每次调用以 `[]` 新建，跨对象不累积 → 同类对象在每个位置都重复输出 `C` 类定义，不用 0x60-0x6f 短形式；循环结构会无限递归。 | spec 4.10「类定义只序列化一次」 | 让类定义表/类型表跨对象共享，重复对象发引用 |

### 2.3 类型系统与数值

| ID | 严重级 | 位置 | 问题 | 建议 |
|---|---|---|---|---|
| **P0-T1** | 高 | `dubbo/__init__.py` | 向 `__builtins__` 注入 `long`/`double` 子类，全局副作用、依赖 import 顺序、篡改进程内所有代码命名空间，后续维护风险极高。Python3 `int` 本身无界，int/long 本无区别，此 hack 纯为蹭 Python2 语义。 | 移除 builtins 注入；改用显式类型标注（如 `JavaLong`）或按**值范围**决定 int/long 编码 |
| **P0-T2** | 高 | `encode_object()`（int 分支） + `utils.int_to_bytes` | 用 `isinstance(field, long)` 分派，普通 Python int 恒走 4 字节 `I`；**int32 范围外不自动升 long**，`struct.pack('>I')` 对 2^31~2^32 的值**静默按有符号截断**。**已复现**：`encode(3_000_000_000) → I b2 d0 5e 00`，Java 侧读作 `-1294967296`。 | 按取值范围自动选择 int/long 编码，与 Java 输出对齐 |
| **P2-T3** | 低 | `_desc_to_cls_names()` | `I/J/S/B` 全映射为 `int`，`D/F` 全映射为 `float`，`$invoke` 泛化调用还原参数时丢失 long/short/byte/double 精度。 | 按原始描述符保留类型精度 |
| **P2-T4** | 低 | `_cls_names_to_desc()` / `_desc_to_cls_names()` | 数组/泛型描述符用 `replace` 占位（`'['→e.replace('.','/')`），多维数组处理粗糙（多处 TODO）。 | 完善 JVM 描述符解析 |
| **P1-T5** | 中 | 整体 | 常见 Java 类型缺映射：`datetime→x4a/x4b`（decode 有、encode 无）、`bytes→binary`、`byte/short/char`、Java enum、`BigDecimal`、`URI/URL` 等。 | 建立完整的 Java↔Python 类型映射表（decode 与 encode 对称） |

---

## 3. 架构级改进项

### 3.1 客户端（`client.py`）

| ID | 严重级 | 问题 | 建议 |
|---|---|---|---|
| **P1-A1** | 中 | **响应与请求无 request_id 关联**：响应仅按 FIFO 队列返回，`send_request_and_return_response`/`get_services` 等并发或 msg_queue 中混入心跳消息时会串包。 | 按 invoke_id 建立 pending map，收到响应按 id 分派（未来事件驱动回调的基础） |
| **P2-A2** | 中 | **连接生命周期不健壮**：构造函数立即 `connect`，失败即抛；断开无重连/惰性连接。 | lazy connect + 重试 + 连接健康检查 |
| **P2-A3** | 中 | 阻塞式 IO；`_timeout` 类级固定 5s 不可配置；无连接池。 | 参数化超时；考虑 asyncio 可选后端 |
| **P2-A4** | 低 | 心跳 60s 硬编码、以睡眠线程实现；心跳 id 与业务 id 共用 `itertools.count`。 | 可配置 interval，独立 id 空间 |
| **P2-A5** | 低 | `get_services`/`get_methods` 走 telnet 明文命令与二进制协议混用同一 socket，响应获取依赖"下一条恰是该响应"。 | 独立管理通道或明确重定向机制 |

### 3.2 服务端（`server.py`）

| ID | 严重级 | 问题 | 建议 |
|---|---|---|---|
| **P1-A6** | 中 | `register()` 每次 new `KazooClient` + start 后**从不 close**，重复注册累积连接/会话；provider 节点用 `ensure_path` 建**持久**节点，服务下线不自动清理（dubbo 应为 **ephemeral** 节点）。 | 复用单例 KazooClient、ephemeral 节点、`stop()` 时 close |
| **P2-A7** | 中 | `ThreadingTCPServer` 每连接一线程 + 每连接一 `_heartbeat_loop` 线程，无上限、无慢客户端防护；handle 线程与心跳线程并**发写同一 socket** 存在交错风险。 | 读写互斥或统一发送队列；连接/线程上限 |
| **P2-A8** | 中 | handler 分发不校验方法签名/参数类型：`handler(*msg.args)`，异常统一 90。 | 按 desc 校验参数个数/类型，返回类型化错误 |
| **P2-A9** | 低 | handler 返回 dict 编码为 untyped map，而 Java 消费者期望 typed；返回 Java 对象需显式 `new_object`，体验差。 | 提供更友好的对象构造/注解机制 |
| **P2-A10** | 低 | 收到 heartbeat request 一律回 `DubboHeartBeatResponse(msg.id)`，未回显 twoway 位。 | 保留 twoway 语义 |

### 3.3 可扩展性

| ID | 严重级 | 问题 | 建议 |
|---|---|---|---|
| **P2-E1** | 中 | 序列化器硬编码 hessian2，无插件/SPI 接口，与 dubbo 的序列化扩展机制（id 0~31）不对应。 | 抽象 `Serializer` 接口，按 serialization id 路由 |
| **P2-E2** | 中 | codec 与 transport 耦合：解码器直接吃 socket/stream，帧头与 body 一次性读入内存（`_read(body_length)`→BytesIO），大包不适用。 | 分层 transport / codec，支持流式与批量两种模式 |

---

## 4. 工程质量与可维护性

| ID | 严重级 | 问题 |
|---|---|---|
| **Q1** | 中 | **单文件职责过重**：`hessian2.py`(776 行) 混合 codc/protocol/model/types 四层。建议拆 `hessian_codec` / `dubbo_frame` / `message` / `types` / `desc` 模块 |
| **Q2** | 中 | `_read_bytes` 与 `_read_object` 约 70% 逻辑重复（`_read_bytes` 残留了 double/date 等无意义的复制粘贴分支），存在漂移风险 |
| **Q3** | 中 | `encode_object(field, idx=0, cls_names=[])` 使用**可变默认参数**；`cls_names` 并发不安全（注释已自我承认 "XXX: not thread safe"） |
| **Q4** | 低 | `logging.warn` 已弃用（Python 3.13 起 DeprecationWarning），应改 `logging.warning`；日志裸用 root logger |
| **Q5** | 低 | 无类型标注、公共 API（`DubboClient`/`DubboService`/`DubboRequest`…）无 docstring；`errors.py` 仅一个异常类 |
| **Q6** | 低 | `setup.py` `python_requires='>=3.5'` 但代码使用 f-string（需 3.6）；测试依赖（pytest、kazoo）未声明为 extras |
| **Q7** | 低 | 无 CI（`travis` 已停、azure pipelines 已删），版本停在 0.2.2，2020 年后无迭代 |

---

## 5. 测试与验证

| ID | 问题 | 建议 |
|---|---|---|
| **T1** | 仅 13 个 codec 单测 + 2 个简易测试，**无畸形字节流/fuzz 测试**，P0 级边界缺陷（长字符串、变长 list、ref、二进制分块）全未覆盖 | 按 spec 建 golden byte 测试向量，做 round-trip 与 fuzz |
| **T2** | **无与真实 Java dubbo provider/consumer 的互操作测试**（当前只验证自编自解） | 建 Java 侧对端做双向互通测试，这是本项目核心价值的验证 |
| **T3** | 无性能/大包/并发/长连接稳定性测试 | 补充基准与大包、并发串包回归 |

---

## 6. 优先级排序建议

### P0 —— 互通性正确性（先修）
| 顺序 | 项 | 理由 |
|---|---|---|
| 1 | H6（引用表分离）+ H5（`O` 对象）+ H4（二进制）+ H3（分块字符串）+ H1（长字符串 encode） | 与 Java 双向互通的核心正确性 |
| 2 | F2（异常映射）+ T2（int32 溢出）+ T1（移除 builtins hack） | 响应的对错与整数精度，直接影响业务结果 |
| 3 | F1（序列化 ID 校验） | 避免误解析造成迷惑性错误 |

### P1 —— 功能缺口（关键场景）
H7/H8/H9/H11、T5、F3/F4、A1（响应关联）、A6（注册泄漏/ephemeral）、E1（序列化 SPI）

### P2 —— 健壮性 / 质量 / 演进
A2~A5、A7~A10、E2、Q1~Q7、T1~T3

> 建议演进路线：**P0 修复 + T2 引入 Java 互通测试（验证修复）→ P1 功能补全 → P2 架构拆分与重构**。拆分（Q1）宜放在 P0/P1 之后，避免在大改结构中修复缺陷。

---

## 附录 A：已复现缺陷证据（Python 3.13.5）

| 缺陷 | 复现输入 | 实际输出 |
|---|---|---|
| H4 二进制分块崩溃 | `b'B\x00\x03\x01\x02\x03'` | `TypeError: '>=' not supported between instances of 'bytes' and 'int'` |
| H1 长字符串 | `encode_object('a'*70000)` | 头部 `b'S\x01\x11p...`（3 字节长度，非法） |
| H5 对象 `O` | `...C Car... O\x90\x03red` | `RuntimeError: unknown code "79"` |
| H6 值引用 | `b'H\x91\x51\x90Z'`（自引用 map） | `IndexError: list index out of range` |
| F2 异常映射 | OK 头 + `0x90`(异常) + `'boom'` | `status=20, data='boom', error=None` |
| T2 int 溢出 | `encode_object(3_000_000_000)` | `b'I\xb2\xd0^\x00'`（Java 侧读作 -1294967296） |