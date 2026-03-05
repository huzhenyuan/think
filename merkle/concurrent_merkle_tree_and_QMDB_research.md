# Concurrent Merkle Tree (Solana) 与 QMDB (LayerZero) 深度技术研究

> 研究日期：2025年3月5日  
> 来源：Solana SPL 官方文档、Helius 技术博客、arXiv:2501.05262 (LayerZero Labs)

---

## 一、Solana Concurrent Merkle Tree（并发 Merkle 树）

### 1.1 解决的核心问题

传统 Merkle 树有一个致命瓶颈：**每次更新叶节点，都需要重新计算从叶到根的整条哈希路径**，且更新后所有正在飞行中的 proof 立刻失效。在 Solana 这样的高吞吐链上（同一个 slot 内可能有数十笔交易同时针对同一棵树做修改），这导致了**证明路径碰撞（proof collision）**：

- Alice 用旧 root `R0` 构造了 proof，提交时树已被 Bob 更新为 `R1`，Alice 的 proof 作废；
- 顺序重试代价极高，并发吞吐量几乎降为 0。

存储成本问题也是直接触发因素：
- Solana 账户存储（Account State）按字节收取 rent，规模化铸造 NFT 成本随数量线性增长；
- 100 万个 NFT 若每个存在独立账户，费用超过百万美元；
- 解法：只将 **Merkle root (32 bytes)** 存链上（账户内），将真实数据存链下（ledger/indexer）。

### 1.2 核心数据结构：Concurrent Merkle Tree

SPL `ConcurrentMerkleTree` 在链上账户中存储三个部分：

```
ConcurrentMerkleTreeAccount {
    root: [u8; 32],           // 当前 Merkle 根哈希
    changelog: ChangelogBuffer, // 环形缓冲区，保存最近 maxBufferSize 次变更
    canopy: CanopyLayer,      // 缓存靠近根部的若干层节点哈希
}
```

#### Changelog Buffer（变更日志缓冲区）

这是 Concurrent Merkle Tree 的核心创新：

```
ChangelogEntry {
    root: [u8; 32],           // 该次变更后的新 root
    path: [[u8; 32]; MAX_DEPTH], // 从叶到根的完整路径哈希（proof path）
    index: u32,               // 被修改的叶索引
}
```

缓冲区大小为 `maxBufferSize`（最小 8，最大 2048），以**环形队列**方式运作。每次成功写入树，就向 changelog 追加一条记录。

**Proof 快进（Fast-Forward）机制**：

当验证者收到一个用旧 root `R_old` 构造的 proof 时，若 `R_old` 正好等于 changelog 中某条记录的 root，则可用 changelog 中记录的路径将该 proof **重放（replay）** 到最新 root，而无需拒绝交易。

形式化描述：
- 设交易提交的 proof 基于 root `R_k`
- changelog 保存了 `[R_k, R_{k+1}, ..., R_n]` 的路径
- 通过逐步应用 changelog 中被改变的路径分支，验证者可将 proof 从 `R_k` 推进到当前 root `R_n`
- 只要 proof 针对的叶与 changelog 中的变更**不重叠**（即不修改同一叶），快进就会成功

这就解释了名字中"Concurrent"的含义：同一 slot 内最多可有 `maxBufferSize` 笔修改**并发有效**（实际上仍是串行执行，但串行执行后仍可验证之前基于旧 root 的 proof）。

#### Canopy（冠层缓存）

Canopy 将 Merkle 树靠近根部的若干层节点哈希**缓存在链上账户中**，减少交易中需要附带的 proof 节点数量：

```
proof_required_in_tx = maxDepth - canopyDepth
```

例如：`maxDepth=14, canopyDepth=10` → 每笔交易只需附带 4 个哈希节点（而非 14 个），显著减小交易体积。

### 1.3 参数体系

创建树时需指定三个不可变参数：

| 参数 | 含义 | 影响 |
|------|------|------|
| `maxDepth` | 树的最大深度 | 叶节点数量 = `2^maxDepth`，最大 30（≈ 10 亿叶） |
| `maxBufferSize` | changelog 缓冲区槽位数 | 同一 slot 内可并发生效的最大 proof 数量 |
| `canopyDepth` | 链上缓存的层数 | 减少 tx 中的 proof 体积，提升可组合性 |

有效的 `(maxDepth, maxBufferSize)` 组合是有限集合，由 `ALL_DEPTH_SIZE_PAIRS` 常量定义（最小 `(3,8)`，最大 `(30,2048)`）。

账户所需空间（因而 rent 成本）随参数增大，且在创建时一次性锁定。

### 1.4 State Compression 与压缩 NFT（cNFT）架构

整个系统由三个互相配合的层组成：

```
                  ┌─────────────────────────────────────────┐
                  │           Solana Chain (链上)              │
                  │  ┌──────────────────┐                    │
                  │  │ ConcurrentMerkle │ ← 只存 32-byte root │
                  │  │ TreeAccount       │                    │
                  │  └──────────────────┘                    │
                  └─────────────────────────────────────────┘
                          ↓ 交易 log（不可篡改）
                  ┌─────────────────────────────────────────┐
                  │        Solana Ledger（账本/历史交易）      │
                  │  每笔 mint/transfer/update 的完整数据      │
                  │  以 SPL Noop log 形式永久记录              │
                  └─────────────────────────────────────────┘
                          ↓ Indexer 实时解析账本
                  ┌─────────────────────────────────────────┐
                  │       Off-chain Indexer + DAS API        │
                  │  存储完整 NFT 元数据、当前树状态、proof    │
                  │  提供 getAsset / getAssetProof 接口       │
                  └─────────────────────────────────────────┘
```

**cNFT 的 leaf 哈希结构**：
```
leaf_hash = hash(
    owner_pubkey,
    delegate_pubkey,
    data_hash,    // = hash(metadata)
    creator_hash, // = hash(creators_array)
    nonce,        // leaf sequence number
    leaf_id,
)
```

只有这 32 字节存在树的叶节点位置，元数据本身通过 SPL Noop 程序以 log 形式写入账本（每笔链上 tx 的 log 是免费的，不计租金）。

**Transfer 流程**（说明 proof 验证机制）：
1. 用户通过 DAS API 获取 `{ root, proof[], data_hash, creator_hash, leaf_id }`
2. 构造 transfer tx，proof 节点作为"剩余账户"附在指令中（部分可由 canopy 补足）
3. on-chain 程序用 proof 从叶哈希一路哈希到 root，与账户中存储的 root 比对
4. 验证通过则更新叶哈希，记录 changelog，写入新 root

### 1.5 性能优势与成本对比

铸造 1000 万个压缩 NFT 的 cost 对比（2023 年数据）：

| 方案 | 成本 |
|------|------|
| 传统 NFT（每个独立账户） | ~100+ 万美元（约 4,600 SOL/千个 × 10000）|
| 压缩 NFT（cNFT）| 约 57.67 SOL（含 tree 创建租金 ~7.67 SOL + mint tx ~50 SOL）|

每个 cNFT 成本降至约 **$0.000005**，降低约 5 个数量级。

### 1.6 局限性与缺陷

#### 1.6.1 并发性是"伪并发"
"Concurrent"具有误导性。同一 slot 内的多笔更新仍由 validator **串行处理**，只是串行处理后旧 proof 仍然可复用。真正的并行写树是不支持的。

#### 1.6.2 对链下基础设施的强依赖
- 读取 cNFT 元数据必须依赖 Indexer / DAS API（如 Helius）；
- 若 Indexer 宕机，虽然数据不会丢失（可从账本重放），但用户无法实时访问资产；
- 链上验证需要在 tx 中附带 proof，但 Solana tx 最大 1232 字节，proof 长度受限；
- 可组合性受 proof size 限制：`maxDepth - canopyDepth ≤ 10` 才能被大多数协议支持（Tensor 的规范）。

#### 1.6.3 参数不可变性
树的 `maxDepth`、`maxBufferSize`、`canopyDepth` 在创建时固定，无法后续调整，必须提前规划好容量需求。

#### 1.6.4 proof size 随树深增长
proof 路径长度 = `maxDepth - canopyDepth`，最大树（depth=30）若不缓存则 proof 需 30 个 32-byte 哈希值（960 字节），几乎占满整个 tx。

#### 1.6.5 状态恢复成本高
从头重建完整树状态需要回放所有历史交易，计算量不小；不支持快速状态快照同步。

#### 1.6.6 Changelog 溢出风险
若单个 slot 内针对同一棵树的修改次数超过 `maxBufferSize`，多余的 proof 就无法快进，交易将失败。高热点树（如大型链上游戏的装备树）需要谨慎设计 `maxBufferSize`。

#### 1.6.7 证明大小不随 Key 数量 O(log K) 而是随总更新数 O(log U) 增长
树是 append-only 追加的，proof 路径长度取决于更新的总次数 U，而非唯一键数量 K。长期运行后路径会比纯键数量估算的更深（通过 GC/compaction 缓解）。

---

## 二、QMDB：Quick Merkle Database（LayerZero Labs）

> 论文：arXiv:2501.05262，作者：Isaac Zhang, Ryan Zarick, Daniel Wong et al.（LayerZero Labs）  
> 提交：2025年1月9日；修订：2025年2月1日

### 2.1 解决的核心问题

现代区块链状态管理的 I/O 瓶颈根源在于：

**MPT（Merkle Patricia Trie）的写放大问题（Write Amplification）**

在 N 个状态条目的数据库中，MPT 更新单个状态条目：
- MPT 本身路径修改：$O(\log N)$ 次
- 存储在 RocksDB 等通用 KV 引擎：$O(\log N)$ 次 SSD IO

两层叠加 → 每次状态更新需 $O((\log N)^2)$ 次 SSD IO，这是核心瓶颈。

此外：
- KV 状储层与 Merkle 证明层**分离**，导致数据重复存储；
- AVL 树虽然改善了平衡，但每次更新仍需 $O(\log N)$ 节点修改；
- NOMT（当时 SOTA）虽做了 flash 优化，但仍是对 MPT 的常数因子优化，未解决渐近复杂度问题；
- 状态必须大量放在 DRAM 中，否则随机 SSD 读取成为瓶颈，加大了验证节点的硬件门槛。

QMDB 的设计目标：
- 读：单次 SSD I/O
- 写（更新）：$O(1)$ SSD I/O（均摊）
- Merkleization（计算 Merkle 根）：**零 SSD I/O**，完全在内存中完成
- DRAM 占用：**~15.4 bytes/entry**（默认内存索引）或 **~2.3 bytes/entry**（混合索引）

### 2.2 核心数据结构

#### Entry（条目）

QMDB 的基本存储单元，字段设计兼顾 CRUD 和各类 Merkle 证明：

| 字段 | 大小 | 用途 |
|------|------|------|
| `Id` | 8 bytes | 全局单调递增的唯一标识符（类似 nonce） |
| `Key` | ≤28 bytes | 应用层键（hashed uniformly） |
| `Value` | ≤224 bytes | 状态值 |
| `NextKey` | ≤28 bytes | 按字典序的下一个 Key（用于排除证明） |
| `OldId` | 8 bytes | 上一个含相同 Key 的条目的 Id（时间链） |
| `OldNextKeyId` | 8 bytes | 上一个含 NextKey 的条目的 Id（删除链） |
| `Version` | 8 bytes | 创建该条目的区块高度 + 交易索引 |

`OldId` 和 `OldNextKeyId` 形成一张**时间图（temporal graph）**，允许沿时间轴回溯任意 key 在任意区块高度的状态，支持历史证明和状态重建。

#### Twig（枝杈）

Twig 是 QMDB 核心的抽象，是 Merkle 树中的固定大小子树（每个 twig 含 **2048** 个叶条目）：

```
Twig {
    root_hash: [u8; 32],       // 该 twig 子树的 Merkle 根哈希
    active_bits: [u8; 256],    // 256 字节 = 2048 bit 位图，标记哪些条目仍"活跃"
}
```

单个 twig 将 2048 个完整条目（≥256 KB）压缩为 **32 + 256 = 288 字节**（压缩率 >99.9%），这是 QMDB 能在内存中完成全部 Merkleization 的关键。

Twig 的生命周期（四态机）：

```
Fresh ──(达到2048条目)──► Full ──(所有条目失效)──► Inactive ──(垃圾回收)──► Pruned
  │
  │ 始终只有一个 Fresh twig per shard
  │ 新条目总是追加到 Fresh twig
```

| 状态 | 条目在 DRAM | Twig结构在 DRAM |
|------|------------|----------------|
| Fresh | ✓（全部） | ✓（root hash + ActiveBits）|
| Full  | ✗（已刷到 SSD） | ✓（288 bytes）|
| Inactive | ✗ | ✓（288 bytes，但 512 字节以下）|
| Pruned | ✗ | ✗（只剩 32-byte 根哈希）|

#### Merkle 树的整体结构

```
Global Root (32 bytes)
    ├── Shard Root 0
    │       ├── Upper Node ...
    │       │       ├── Twig 0  (root_hash + active_bits)
    │       │       │     └── Entry 0..2047  (存 SSD，按 Id 顺序追加)
    │       │       └── Twig 1
    │       └── Upper Node ...
    └── Shard Root 1
            └── ...
```

关键设计：
- 所有 Upper Node（twig 以上的内部节点）**完全在 DRAM 中**，不写 SSD；
- 重启时从 SSD 读取所有 twig root hash，在内存中重建 upper node，复杂度 $O(T)$（T = twig 数量），毫秒级完成；
- 对于 $2^{30}$ 条目（约 10 亿），所需 DRAM 仅约 **160 MB**。

#### Indexer（索引器）

Indexer 维护 `Key → SSD 文件偏移` 的映射：

- **默认内存索引**：B-tree，15.4 bytes/entry，支持有序迭代（排除证明所需）
  - 仅使用 Key 哈希的最高 9 字节（前 2 字节作分片键，后 7 字节存 key，另 8.4 字节存 SSD offset + 开销）
  - 16 GB DRAM 可索引 10 亿条目
- **混合索引**：SSD + DRAM 缓存，2.3 bytes/entry，每次查找多一次 SSD 读
  - 适合 DRAM 受限但 SSD 充足的场景，理论可扩展至 2800 亿条目

索引器使用细粒度读写锁（按 Key 哈希前 2 字节分锁）减少并发争用。

### 2.3 CRUD 操作的 I/O 复杂度

所有写操作都是"追加到 Fresh twig"，每当 Fresh twig 满（2048 条），一次性大顺序写入 SSD，每个条目的均摊写入代价为 $\frac{1}{2048}$ 次 SSD I/O。

| 操作 | SSD 读 | SSD 写（均摊） | 说明 |
|------|--------|---------------|------|
| Read | 1 | 0 | 查索引得 offset → 单次 SSD 读 |
| Update | 1 | 1/2048 | 读旧 Entry → 追加新 Entry（含 OldId 链） |
| Create | 1 | 2/2048 | 读前驱 → 追加新 Entry + 更新前驱的 NextKey |
| Delete | 2 | 1/2048 | 读要删的 + 前驱 → 追加前驱新 Entry（含新 NextKey）|

Merkleization 成本：**0 SSD 读写**。只在 DRAM 中更新受影响的 twig root hash 和 upper nodes。

### 2.4 证明系统

QMDB 通过条目字段支持多种证明：

```
包含证明（Inclusion Proof）：
  提供 Entry E 的 Merkle proof π，使得 E.Key = K
  → 用路径哈希一直到 root 验证

排除证明（Exclusion Proof）：
  提供 Entry E 的 Merkle proof，使得 E.Key < K < E.NextKey
  → 证明 K 不在当前状态中

历史包含/排除证明（Historical Proofs）：
  在区块高度 H 处，从 OldId 链回溯，找到 Version ≤ H 的最新 Entry
  → 证明特定历史时间点的状态（QMDB 首创，其他 ADS 不支持）

状态重建：
  沿 OldId 和 OldNextKeyId 指针图，可重建任意区块高度的完整 Merkle 树
```

历史证明的特性使 QMDB 支持新型应用，如在链上计算 **TWAP（时间加权平均价格）**。

### 2.5 并行化：Sharding + Pipelining

**Sharding**：用 Key 哈希的最高 N 位将 key space 划分为 $2^N$ 个 shard（例如 N=4 → 16 shards），每个 shard 独立管理一棵子树，并行处理不同 shard 的操作。

**三阶段流水线（Prefetch-Update-Commit）**：

```
Block N-1：[Prefetch] ──► [Update] ──► [Commit]
Block N  ：              [Prefetch] ──► [Update] ──► [Commit]
Block N+1：                           [Prefetch] ──► ...
```

- **Prefetcher**：从 SSD 预读 Delete/Create 操作所需的 Entry 到 EntryCache（DRAM）
- **Updater**：追加新 Entry，更新 Indexer（全在 DRAM/EntryCache 中）
- **Committer**：异步 Merkleize（DRAM only），将满 twig 批量顺序写入 SSD

N+1 串行化保证：Prefetcher 不能为第 N 块开始预读，直到第 N-1 块的 Updater 完成，保证状态一致性。

### 2.6 性能基准测试结果

实验环境：AWS 实例（c7gd.metal：64 vCPU，2 SSD）

**对比 RocksDB（不含 Merkleization）**：

| 数据集大小 | QMDB updates/s | RocksDB updates/s | 倍数 |
|-----------|---------------|-------------------|------|
| 6 billion entries | 601,000 | ~100,000 | **6×** |

**对比 NOMT（含 Merkleization，归一化后）**：

| Keys | QMDB normalized ups | NOMT normalized ups | 倍数 |
|------|--------------------|--------------------|------|
| 4M   | 614,948 | 162,190 | ~3.8× |
| 256M | 346,843 | 42,277  | ~8.2× |
| 4B   | 294,349 | 37,057  | ~7.9× |

读延迟：QMDB 30.7 µs，NOMT 55.9 µs（均接近 SSD 读延迟下界）。

**io_uring + O_DIRECT 优化后（14 billion entries）**：

| 实例 | SSD 数 | 更新速度 |
|------|--------|---------|
| i8g.metal-24xl | 6 | **2.28 M updates/s** |
| i8g.8xlarge | 2 | 697K updates/s |

2.28M updates/s 足以支持 **>1M 原生代币转账/秒**（每笔转账 2 次 KV 更新）。

**硬件扩展性**：

| 实例/配置 | DRAM | SSD 容量 | 最大可扩展条目（混合索引） |
|----------|------|---------|------------------------|
| c7gd.metal | 128 GB | 3.8 TB | 18B |
| i3en.metal | 768 GB | 60 TB | **280B（2800亿）** |
| Mini PC（$540） | 64 GB | 4 TB NVMe | >4B（4 billion）|

Mini PC 实测：1 billion entries 时 150K updates/s，4 billion entries 时 >100K updates/s。

**内存占用对比**：

| 系统 | DRAM per entry |
|------|---------------|
| NOMT | 1–2 bytes |
| QMDB（内存索引）| ~15.4 bytes |
| QMDB（混合索引）| ~2.3 bytes |
| 以太坊 Geth（MPT + RocksDB）| 数十 bytes |

与 NOMT 相比，QMDB 内存占用更高，但性能提升 8× 为此做了合理。

### 2.7 关键创新总结

| 创新点 | 描述 |
|--------|------|
| **Twig 抽象** | 将 2048 个条目压缩为 288 字节（root hash + ActiveBits），使 Merkleization 完全在内存中完成 |
| **Append-Only 设计** | 所有更新追加写，消除随机写，SSD 写仅在 twig 满时批量顺序完成（均摊 O(1)）|
| **统一 KV + Merkle** | 单一数据结构同时服务 KV 查询和 Merkle 证明生成，消除数据重复 |
| **历史状态证明** | OldId/OldNextKeyId 指针图，支持任意历史区块高度的包含/排除证明 |
| **完全内存 Merkleization** | Upper nodes 永不写 SSD，重启毫秒级重建，Merkleization 零磁盘 I/O |
| **分片并发** | Key space sharding 实现真正并行处理 |
| **三阶段流水线** | Prefetch-Update-Commit 跨块流水，更充分利用 CPU 和 SSD 带宽 |
| **TEE 就绪** | 首个支持 TEE（Intel SGX）的可验证数据库，用 AES-GCM 加密持久化数据 |

### 2.8 局限性与不足

#### 2.8.1 空间局部性损失
QMDB 不保留 key 的时序和空间局部性（相关 key 可能写入不同 twig），无法利用传统工作负载的访问局部性优化缓存。论文辩称区块链必须假设最坏情况的工作负载，但这对 EVM 中的 storage array 等顺序访问模式确实是一个劣势。

#### 2.8.2 Proof 大小随更新次数增长
Merkle proof 路径长度与 $\log_2(U)$（总更新次数）成正比，而非 $\log_2(K)$（唯一键数量），长期运行后 proof 会比纯基于 key 数量估算的更长。日积月累（假设 10K TPS × 5 updates/tx × 365天 → tree depth ≤ 41 层），需要 ZK proof 压缩 proof witness 来缓解。

#### 2.8.3 Compaction 的确定性要求复杂
在共识系统中，GC（垃圾回收）线程必须确定性执行，否则不同节点的 Merkle 根可能不一致。论文中提到的 compaction 策略细节（"active entry ratio 维持在阈值以上"）需要在共识层面严格对齐。

#### 2.8.4 不支持高效的键迭代
QMDB 明确放弃了通用 KV 数据库的 range/prefix query 特性（如 RocksDB 的 seek/scan），换取 SSD 写优化。对于需要状态迭代的应用（如 state trie dump、统计类查询）不适用。

#### 2.8.5 Reorg 支持不完整
论文承认参考实现故意省略了区块链 reorg 优化，建议各链在 QMDB 之上构建缓冲层、只写确认数据。对于快速 finality 以外的链（如需支持较长 reorg 的 PoW 链）需要额外工程工作。

#### 2.8.6 Remove 操作成本偏高
Delete 操作需要 2 次 SSD 读（读被删键 + 前驱键），相比 Update 多一次，在删除密集型工作负载下有额外开销。

#### 2.8.7 内存索引 DRAM 开销
默认内存索引（15.4 bytes/entry）比 NOMT（1–2 bytes/entry）高出约 8–15 倍。虽然混合索引可降至 2.3 bytes，但每次查询额外多一次 SSD 读取，将读延迟从 ~30 µs 增加。

#### 2.8.8 系统成熟度
截至论文发表（2025年1月），QMDB 仍为预发布状态，部分高级 I/O 优化（io_uring、O_DIRECT）也在开发中；基准测试数据对比 NOMT 时存在环境不一致因素（NOMT 不支持客户端级流水线）。

---

## 三、横向对比

| 维度 | Solana Concurrent Merkle Tree | QMDB |
|------|------------------------------|------|
| **核心场景** | NFT 压缩存储，降低链上 rent 成本 | 通用区块链世界状态管理 |
| **链上存储** | 只存 Merkle root（32 bytes）| 存完整 KV 状态（SSD）+ 树结构（DRAM）|
| **链下存储** | 需要 Indexer + DAS API | 完全自包含（SSD + DRAM）|
| **Merkle 类型** | 完全二叉 Merkle 树（按叶序号）| 二叉 Merkle 树（按 append order）|
| **并发写支持** | Changelog buffer（串行执行，证明可复用）| Sharding + Pipelining（真并行）|
| **历史状态** | 需重放账本 | 原生支持（OldId 指针链）|
| **删除操作** | 软删（标记失效）| 通过追加新 Entry 实现（NextKey 链调整）|
| **SSD 优化** | 不直接相关（链上数据） | 核心优化目标（O(1) IO，顺序写）|
| **适用对象** | Solana dApp 开发者 | 区块链基础设施开发者（EVM 等）|

---

## 四、参考资料

1. Solana SPL Account Compression 文档：https://spl.solana.com/account-compression
2. Helius 技术博客《All You Need to Know About Compression on Solana》：https://www.helius.dev/blog/all-you-need-to-know-about-compression-on-solana
3. Metaplex Bubblegum 文档：https://developers.metaplex.com/bubblegum
4. QMDB 论文：Isaac Zhang, Ryan Zarick et al., "QMDB: Quick Merkle Database", arXiv:2501.05262, 2025. https://arxiv.org/abs/2501.05262
5. SPL ConcurrentMerkleTree 白皮书（草稿）：https://drive.google.com/file/d/1BOpa5OFmara50fTvL0VIVYjtg-qzHCVc/view
