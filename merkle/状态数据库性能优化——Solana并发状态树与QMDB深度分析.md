# 状态数据库性能优化——Solana 并发状态树与 QMDB 深度分析

> 这篇文档从「为什么」出发，深入剖析两个方向截然不同的工程解法：
> Solana 的 Concurrent Merkle Tree 解决的是**高并发写冲突**问题，
> LayerZero 的 QMDB 解决的是**全局状态树读写放大**问题。
> 两者都不是银弹，但各自在特定场景下提供了量级级别的改进。

---

## 一、先说背景：区块链状态存储的核心矛盾

区块链的状态存储需要同时满足两件在工程上相互矛盾的事：

1. **读写要快**：每个区块要处理成百上千笔交易，每笔交易要读写状态，延迟不能高。
2. **读写要可证明**：任意第三方（轻客户端、跨链合约）可以用一个小小的根哈希，验证「这个值确实在链上」。

传统方案（以太坊 MPT）把这两个需求塞进同一棵树：用 Merkle Patricia Trie 同时承担 KV 存储和证明生成。结果是两件事都没做好：

- 每次修改一个 key，要沿树向上更新约 15 个节点，产生 15 次随机 SSD 写（**写放大**）。
- 树节点散布在 SSD 的随机位置，顺序读的局部性极差（**读放大**）。
- 树体积庞大，必须在 DRAM 里缓存大量节点，内存压力巨大（**内存放大**）。
- 以太坊当前约 15 亿条状态条目，全节点需要数百 GB DRAM 才能跑出合理性能。

Solana 和 LayerZero 从完全不同的角度切入，试图解决这个矛盾。

---

## 二、Solana 并发状态树（Concurrent Merkle Tree）

### 2.1 Solana 要解决什么问题？

Solana 做 NFT（以及各类链上资产）时遇到了一个具体的工程痛点：

> 在 Solana 上存储 100 万个 NFT，每个 NFT 的 metadata 要占用一个链上 Account，
> 每个 Account 要缴存租金，100 万个 NFT 的存储成本约 **100 万美元**。这对绝大多数应用根本不可行。

**解法思路**：只在链上存一棵 Merkle 树的**根哈希**（32 字节），把真实数据存在链下数据库里。用户需要时，提交数据 + Merkle Proof，链上程序验证后即可操作。

这样 100 万个 NFT 的链上成本，从 100 万美元**降到约 57 SOL**（约几百美元）。

但这里有一个非常棘手的并发问题。

### 2.2 朴素 Merkle 树在并发下会崩

想象一棵有 100 万个叶子的 Merkle 树，链上只存根哈希 `R`。

现在 100 个用户同时要修改各自的 NFT（各自修改一个叶子）：

- 用户 A 构造了一个 Proof：「我的叶子的路径，在根 `R` 下是合法的」。
- 用户 B 也构造了同样基于 `R` 的 Proof。
- 用户 A 的交易先执行，修改了叶子，**树根变成了 `R'`**。
- 用户 B 的交易提交时，它的 Proof 是基于 `R` 的，但现在根是 `R'`——验证失败，交易打回。

**结果**：在高并发下，几乎所有 Proof 都会失效，系统实际上退化为串行处理，并发吞吐为零。

### 2.3 Concurrent Merkle Tree 的解法：Changelog Buffer

核心思想：**不要在树根变了之后拒绝旧 Proof，而是想办法把旧 Proof「快进」到新根**。

链上 Account 存储三个结构：

```
ConcurrentMerkleTree {
    root: [u8; 32],             // 当前最新的树根
    
    changelog: RingBuffer<{     // 环形缓冲区，保存最近 N 次变更
        old_root: [u8; 32],     // 变更前的根
        path: [[u8;32]; depth], // 本次变更的完整路径（从叶到根的所有哈希）
        leaf_index: u32,        // 哪个叶子被修改了
    }>,
    
    canopy: Vec<[u8;32]>,       // 树顶部 K 层的节点缓存（可选）
}
```

**快进（Proof Rebase）机制**：

当用户提交「基于旧根 `R_old` 的 Proof」时，系统不是直接拒绝，而是：

1. 在 changelog 里找到 `R_old` 对应的那条变更记录。
2. 用该记录里保存的路径，检查：**本次修改的叶子**，和**用户想操作的叶子**，是否在路径上有重叠？

   - **没重叠**：说明这两次修改互不干扰。把用户的 Proof 沿 changelog 里的路径更新，得到在新根 `R_current` 下有效的新 Proof——合法，交易执行。
   - **有重叠**：同一个叶子被两个用户争抢，这是真正的冲突——拒绝。

**图示**：

```
时间线：
  t1: 根 = R1，用户A的NFT在叶子5，用户B的NFT在叶子99，用户C的NFT在叶子5
  t2: 用户A基于R1修改叶子5 → 根变成R2，changelog记录[R1, path_of_leaf5, 5]
  t3: 用户B提交基于R1的Proof（修改叶子99）
       → 在changelog找到R1
       → 检查：path_of_leaf5 和 path_of_leaf99 有交叉吗？
       → 二叉树上，不同叶子只在顶部节点有交叉，但具体节点各自独立
       → 无冲突 → Proof快进到R2下 → 执行成功 ✅
  t4: 用户C提交基于R1的Proof（也修改叶子5）
       → 在changelog找到R1
       → 检查：path_of_leaf5 和 path_of_leaf5 完全重叠
       → 冲突 → 拒绝 ❌
```

**Canopy（冠层缓存）**：

Merkle Proof 的大小 = 树的深度 × 32 字节。深度 20 的树，Proof 就是 640 字节，而 Solana 单条交易的大小上限是 1232 字节，proof 太大就装不下了。

Canopy 把树的顶部 K 层节点缓存在链上，用户提交 Proof 时只需提供下面 `depth - K` 层，大大压缩了 Proof 尺寸。

### 2.4 实际应用：Compressed NFT

- 1 亿个 NFT，只需链上一个 Account（约 200 字节 changelog + canopy）。
- 真实数据存在链下 DAS（Digital Asset Standard）API 节点（Helius、Triton 等提供）。
- 用户转让时：提供叶子数据 + Merkle Proof，链上验证后更新叶子，生成新根。
- **成本对比**：1 亿个普通 NFT ≈ 1 亿美元；1 亿个 Compressed NFT ≈ 110 SOL（约几千美元）。

### 2.5 Solana 并发状态树的缺陷

#### 缺陷 1：「并发」是假并发

Changelog 的 Proof Rebase 只是让「不冲突的串行交易」看起来可以并发提交，实际链上执行**仍然是串行的**，changelog 里的每条记录必须按顺序处理。

缓冲区大小（`maxBufferSize`）决定了同一时间可以有多少个「飞行中的 Proof」。超过这个数量，老的记录被覆盖，尚未落地的 Proof 全部失效。

在极端高并发下（同一个树每秒有大量修改），仍然会出现交易失败。

#### 缺陷 2：强依赖链下索引器

链上只有根哈希和 changelog，真正的数据在链下。用户查询自己的资产，必须依赖第三方 DAS API 节点。如果这些节点宕机或作恶，**用户甚至找不回自己的资产数据**（尽管链上的根哈希保证了「谁拥有什么」不可篡改）。

这是一个去中心化的降级：数据可验证性（链上保证）和数据可访问性（链下依赖）被分离了。

#### 缺陷 3：树参数不可变

树的深度、缓冲区大小（`maxBufferSize`）、canopy 深度在创建时确定，**之后无法更改**。设计不当会造成空间浪费或功能受限（例如树深度不够后无法继续追加叶子）。

#### 缺陷 4：受 Solana 交易大小限制

Solana 交易上限 1232 字节，深树（深度 > 30）的 Proof 即使经过 canopy 优化，仍可能接近上限，限制了树的规模。

#### 缺陷 5：只解决了「资产存储」这一个特定场景

Concurrent Merkle Tree 是一个专用结构，解决的是「链上存根，链下存数据」的模式。它并没有解决全局状态 KV 存储的读写性能问题——Solana 的全局账户状态本身并不用这个结构。

---

## 三、QMDB（Quick Merkle Database）

> 论文：arXiv:2501.05262，作者来自 LayerZero Labs，2025 年 1 月发布。

### 3.1 QMDB 要解决什么问题？

以太坊 MPT + RocksDB 的双层架构，本质上是**两个 `O(log N)` 叠加**：

- MPT 每次插入：`O(log N)` 次树节点更新
- RocksDB 存储 MPT 节点：每次写 `O(log N)` 次 LSM 层合并

两者叠加 → 每次状态更新 = `O((log N)²)` 次 SSD IO

Ethereum 有约 15 亿条状态（N ≈ 1.5B），`(log₂ 1.5B)² ≈ 31² ≈ 961`，也就是每次状态更新可能触发近千次 SSD 操作。为了规避这个代价，节点必须把 MPT 大量缓存在 DRAM 里，这需要数百 GB 内存，普通硬件无法参与验证，**损害去中心化**。

**QMDB 的目标**：

- 每次状态读：**1 次 SSD IO**（而非 O(log N)）
- 每次状态写（均摊）：**1/2048 次 SSD IO**（约等于零）
- Merkle 化（计算新的状态根）：**0 次 SSD IO**（完全在内存中完成）
- DRAM 占用：**2.3 bytes/entry**（而非当前方案的数十字节甚至数百字节）

### 3.2 QMDB 的核心架构

#### 概念 1：Entry（状态条目）

QMDB 存储的基本单位不是裸的 key-value，而是一个**带元数据的 Entry**：

| 字段 | 大小 | 作用 |
|------|------|------|
| `Id` | 8 字节 | 全局唯一递增 ID（类似物理位置的序号） |
| `Key` | 28 字节 | 应用层的状态 key（存 hash 后的结果） |
| `Value` | 224 字节 | 状态值 |
| `NextKey` | — | 按字典序，当前 key 的下一个存活的 key |
| `OldId` | 8 字节 | 指向上一个版本的同一个 key 的 Entry |
| `OldNextKeyId` | 8 字节 | 指向历史上 NextKey 的上一个版本 |
| `Version` | 8 字节 | 创建此 Entry 的区块高度和交易索引 |

这两个「指向历史」的字段（`OldId`、`OldNextKeyId`）形成了一张**时间指针图**，让 QMDB 可以沿着时间轴回溯，查出任意历史区块高度的状态值——这是以太坊 MPT 没有原生支持的能力。

**NextKey 字段的用途**：证明「key 不存在」（排除证明）。

如果要证明「key K 在数据库里不存在」，只需要找到 K 的前驱 Entry（Key < K），展示它的 `NextKey > K`，说明 K 夹在这两者中间且没有实际记录——K 当然不存在。

#### 概念 2：Twig（枝组，QMDB 最关键的设计）

Twig 是 QMDB 最核心的创新，直接解决了 Merkle 化的 IO 放大问题。

**一个 Twig = 2048 个 Entry 组成的固定大小子树**

关键在于它的**压缩表示**：

```
Twig 的内存占用（Full 状态）= 288 字节
  ├── 32 字节：该子树的 Merkle 根哈希（twig_root）
  └── 256 字节：ActiveBits 位图（2048 bit）
       每一位对应一个 Entry，1 = 这个 Entry 的值仍是最新状态，0 = 已被更新版本覆盖
```

**为什么只需要 288 字节就能表示 2048 个 Entry 的 Merkle 子树？**

因为 Entry 是 append-only 的（写入后永不修改）。一旦一个 Twig 写满 2048 个 Entry 并刷到 SSD，它的 `twig_root`（32 字节哈希）就不会再变——**除非 ActiveBits 发生变化**（有 Entry 被标记为不再活跃）。

重新计算 Merkle 根时，只需要拿出 `twig_root` 和最新的 `ActiveBits`，做一次纯内存的哈希计算，**不需要读取 SSD 上的任何 Entry 数据**。

**一个直观的类比**：

> 把 2048 本书放到一个书架上，书架有一个「摘要卡」（twig_root）记录了所有书的总指纹。
> 现在有几本书被新版替代了（ActiveBits 里对应位从 1 改为 0），
> 需要更新摘要卡时，只需重新算一次——不需要把所有书拿下来重读，
> 因为替换后的书的哈希已经固化，只是活跃标记变了。

#### Twig 的四阶段生命周期

```
Fresh（新鲜）
  - 存在 DRAM 中，正在被追加 Entry
  - 每个分片（shard）只有一个 Fresh Twig
  ↓ 满 2048 个 Entry 后
Full（满载）
  - Entry 内容顺序写入 SSD（一次大批量顺序写，效率极高）
  - DRAM 里只保留 288 字节（twig_root + ActiveBits）
  ↓ 当 Twig 里的所有 Entry 都被新版本覆盖后
Inactive（非活跃）
  - SSD 上的 Entry 数据已经没有任何一条是当前最新状态
  - DRAM 里 299 字节压缩到 64 字节（只保留根哈希）
  ↓ GC 完成搬运后
Pruned（裁剪）
  - 连根哈希都从 DRAM 里删除，换成上级树节点的哈希摘要
  - 释放所有 DRAM 占用
```

**结果**：整棵 Merkle 树在 DRAM 里的占用 = 少量 Full Twig 的 288 字节 + 上层节点的 32 字节哈希。**以 10 亿条 Entry 为例，DRAM 占用仅约 160MB**，而传统 MPT 同等规模需要 GB 级 DRAM。

#### 读写的 IO 分析

| 操作 | SSD IO 次数 | 说明 |
|------|------------|------|
| **读（Read）** | 1 次读 | 建立内存索引（9 字节 key → SSD 文件偏移），1 次跳转读取 |
| **写/更新（Update）** | 1 次读 + 1/2048 次写 | 读当前 Entry，追加新 Entry 到 Fresh Twig（2048 条攒一批写） |
| **新建（Create）** | 1 次读 + 2/2048 次写 | 读前驱 Entry，追加 2 条新 Entry |
| **删除（Delete）** | 2 次读 + 1/2048 次写 | 读被删 Entry + 读前驱 Entry，追加 1 条新 Entry |
| **Merkle 化** | **0 次** | 完全在内存中完成 |

对比以太坊 MPT + RocksDB：每次状态更新 `O((log N)²)` ≈ 数百到数千次 SSD IO。

#### 概念 3：Indexer（内存索引）

QMDB 在 DRAM 里维护一个 B-tree 内存索引：

```
Hash(application_key)[9字节最高有效字节] → SSD 文件偏移（6字节）
```

每条索引记录约 15.4 字节（含 B-tree 元数据均摊）。10 亿条 Entry 的索引大小约 15 GB DRAM。

对于内存更紧张的场景，提供「混合索引（Hybrid Indexer）」：部分索引存 SSD，DRAM 只保留 2.3 字节/条，代价是每次读多 1 次 SSD IO。

### 3.3 并行化：分片 + 流水线

**分片（Sharding）**：

按 key hash 的高位（如前 4 bit 分成 16 个分片）把 key 空间切成若干段，每段独立维护自己的 Fresh Twig + 上层树节点，各分片之间完全并行，互不阻塞。

**三段流水线（Prefetch → Update → Flush）**：

```
区块 N：Prefetcher 从 SSD 预读必要的 Entry 到内存
区块 N：Updater 在内存中执行状态变更，更新索引
区块 N：Committer 异步做 Merkle 化 + 将满的 Twig 顺序写入 SSD

同时：
区块 N+1：Prefetcher 已经在预读下一批数据（N+1 的 Prefetcher 等 N 的 Updater 完成后才启动）
```

这让 CPU、DRAM、SSD 三者同时工作，避免互相等待。

### 3.4 历史状态证明

这是 QMDB 独有的功能。

由于每个 Entry 保存了 `OldId`（指向同一个 key 的上一个版本），所有版本形成一条链：

```
Key=alice.balance:
  Entry#1 (v=100, h=100) --OldId--> null
  Entry#2 (v=150, h=200) --OldId--> Entry#1
  Entry#3 (v=80,  h=350) --OldId--> Entry#2
  Entry#4 (v=80,  h=500) --OldId--> Entry#3（当前最新）
```

要证明「alice.balance 在高度 250 时是 150」：

1. 从当前 Entry 沿 `OldId` 回溯，找到 `Version` 在 250 前后的两个版本：Entry#2（h=200）和 Entry#3（h=350）
2. 提供 Entry#2 在 Twig 里的 Merkle Proof
3. 以 Entry#2 在历史区块 200 时的 state_root 为锚点

这个能力让 QMDB 支持在最新区块头上**查询任意历史高度的状态**——以太坊归档节点才能做到的事，QMDB 在普通全节点上即可完成。

### 3.5 性能数据（来自论文）

| 硬件 | 条目数 | 吞吐量 |
|------|-------|--------|
| AWS c7gd.metal（企业级） | 60 亿 | **601K 状态更新/秒** |
| AWS i8g.metal-24xl（6块SSD） | 140 亿 | **2.28M 状态更新/秒** |
| 540 美元 Mini PC | 40 亿 | **100K+ 状态更新/秒** |

对比：
- vs RocksDB（无 Merkle 化）：**快 6 倍**
- vs NOMT（最先进的可验证数据库之一）：**快 8 倍**
- 支持 100 万代币转账/秒（每次转账 = 2 次状态更新）

### 3.6 QMDB 的缺陷

#### 缺陷 1：Proof 大小随更新次数增长，不随 key 数量增长

这是 append-only 设计的根本代价。

Merkle 树的深度 ∝ log₂(U)，其中 U 是**状态更新次数**（不是唯一 key 数量 K）。

论文估算：以 10,000 TPS、5 次状态更新/tx 计算，运行 1 年后树深度最多是 log₂(10000 × 5 × 3600 × 24 × 365) = log₂(1.58T) ≈ **41 层**。

相比之下，以太坊 MPT 深度固定在约 log₁₆(N_keys) ≈ 8~9 层（只和 key 数量有关，更新不会加深）。

QMDB 长期运行后 Proof 会越来越大，论文的解决方案是用 ZK Proof 压缩，但这引入了额外的计算开销。

实际上 GC（垃圾回收）会定期搬运活跃条目、合并稀疏分支，大幅减缓树的增长，实际深度远低于理论上界，但这一点论文并未给出完整的长期实测数据。

#### 缺陷 2：不支持范围查询和前缀扫描

QMDB 对 key 按 hash 值存储（`Hash(application_key)`），破坏了 key 的字典序关系。

这意味着「查询 alice 下的所有存储槽」、「查询某个合约的完整状态」等范围查询**无法高效实现**。

对于区块链工作负载，论文认为这不是问题（共识系统必须假设最坏情况，不能依赖局部性），但对于需要 `eth_getStorageAt` 范围查询的应用层来说，这是一个实质性限制。

#### 缺陷 3：Delete 操作需要 2 次 SSD 读（比 Update 更贵）

Update 只需读当前 Entry（1 次读），Delete 还要额外读取前驱 Entry（因为要更新前驱的 `NextKey` 字段，维护有序链），共 2 次读。

状态删除（如清空合约、GC 过期状态）在高并发场景下会成为比普通写更重的操作。

#### 缺陷 4：Compaction 必须是确定性的

Compaction（GC）是 QMDB 防止树无限增长的手段，但它涉及将旧 Entry 搬运到新位置，会**改变 Entry 的物理地址**。

在共识系统里，所有节点必须执行完全相同的 Compaction 逻辑，否则各节点的 key → SSD 偏移映射会不同，但最终 Merkle 根仍然相同。这给实现带来了不小的复杂度——Compaction 的触发条件和执行逻辑必须用确定性规则描述，任何随机因素都会导致节点分叉。

#### 缺陷 5：Reorg（链重组）支持未实现，需各链自行处理

QMDB 的参考实现明确不处理 Reorg（区块回滚），要求上层链的实现在 QMDB 之上建缓冲层，只在区块最终确认后才写入 QMDB。

这对 BFT 链（有明确最终性）影响较小，但对 PoW 链或软最终性链（如早期以太坊）来说，需要额外的工程工作。

#### 缺陷 6：预发布，基准测试有争议

QMDB 论文发布时，NOMT 还在 pre-release。论文对比 QMDB 和 NOMT 的 8× 差距，受到一些质疑：两者的 benchmark 负载模型不完全一致（NOMT 默认是 2 读 2 写，QMDB 用了 9 写 15 读 1 创建 1 删除），需要「归一化」后才能对比。

论文作者承认这个局限性，并指出两者都还在持续优化中。真实差距可能不到 8×，但数倍量级的改进应当是真实的。

---

## 四、横向对比

| 维度 | 以太坊 MPT | Solana CMT | QMDB |
|------|-----------|-----------|------|
| **解决的问题** | 全局状态承诺 | 低成本链上资产存储 | 全局状态 KV + Merkle 化性能 |
| **每次状态写 IO** | O((log N)²) | N/A（只存根哈希） | 均摊 1/2048 次 |
| **每次状态读 IO** | O(log N) | N/A（链下读取） | 1 次 |
| **Merkle 化 IO** | 包含在写放大中 | 1 次修改叶子 + 路径更新 | 0 次（内存完成） |
| **DRAM 占用** | ~数百 GB（全节点） | 极小（只存根哈希和 changelog） | 2.3 ~ 15 字节/条 |
| **历史状态证明** | 需归档节点 | 不支持 | 原生支持 |
| **轻客户端支持** | ✅ 完整支持 | ✅（对树结构） | ✅ 完整支持 |
| **范围查询** | ✅（MPT 字典序） | N/A | ❌ |
| **Reorg 支持** | ✅ | ✅ | ⚠️ 需上层实现 |
| **并发写** | 串行 | 有限并发（changelog buffer） | 分片并行 + 流水线 |
| **数据在哪里** | 全量链上 | 链上只有根，数据在链下 | 全量 SSD + 内存索引 |

---

## 五、一句话总结各自的定位

**Solana Concurrent Merkle Tree**：
> 是一个「把数据搬到链下、只在链上留指纹、然后解决并发写冲突」的专用方案。
> 核心价值是**极大降低链上存储成本**，代价是让数据访问依赖链下基础设施。
> 它没有改变全局账户状态的存储方式，解决的是一个特定的「大量同质资产」场景。

**QMDB（Quick Merkle Database）**：
> 是一个「用 append-only + twig 压缩来打破 Merkle 树写放大」的通用状态数据库。
> 核心创新是把 Merkle 化所需的信息压缩到几乎可以全部放进 DRAM，从而把 IO 复杂度从 `O((log N)²)` 降到常数。
> 它适合作为区块链执行层状态存储的底层引擎，是 MPT + RocksDB 架构的潜在替代品。

---

## 六、对「本设计方案」的启示

回到我们在[新 Merkle 数据库设计方案](./新merkle数据库设计方案.md)里讨论的设计（多版本 KV + change_root 小树）：

1. **QMDB 验证了「不用全局树、改用 append-only 结构」这个方向是可行的**。两者思路有相似之处——都是用追加写代替随机修改，通过批量攒写摊销 SSD IO。

2. **QMDB 解决了我们设计里遗留的「轻客户端证明」问题**：QMDB 的 Twig 结构本质上就是一个对全量状态的高效承诺，可以对任意 key 给出 Merkle Proof，这是我们的 change_root 做不到的。

3. **Solana CMT 的教训**：把数据搬到链下确实能解决存储成本问题，但「链下数据可访问性依赖中心化索引器」是一个实质性的去中心化妥协，需要谨慎考虑这个取舍是否可以接受。

---

## 七、补充：Solana 全局账户状态的存储方式（与以太坊 MPT 的深度对比）

> Solana 的并发状态树（第二章）只是一个面向「压缩资产」的专用方案。
> Solana **全局账户状态**本身有一套完全不同的架构，和以太坊有根本性的设计差异。

### 7.1 以太坊 MPT 的做法（回顾）

以太坊把全局账户状态存在一棵 **Merkle Patricia Trie** 里，以 RocksDB 作为底层 KV 存储。

每次账户状态变更：
1. 找到账户对应的 MPT 叶子节点
2. 更新叶子，沿路径向上重新计算约 15 个中间节点的哈希
3. 这 15 个节点各自经过 RocksDB 的 LSM 写入（LSM 内部还有 `O(log N)` 的写放大）

**结果**：每次状态更新 ≈ `O((log N)²)` 次随机 SSD IO，高写放大，需要大量 DRAM 缓存热节点。

### 7.2 Solana 的全局账户存储：AppendVec + AccountsIndex

Solana **完全没有用 MPT**，全局账户状态靠以下两个结构共同实现：

#### 结构一：AppendVec（追加写文件）

AppendVec 是一个**内存映射（mmap）的追加只写文件**：

```
AppendVec 文件结构（每条记录）:
┌──────────────────────────────────┐
│ StoredMeta (48 字节)              │  write_version, pubkey等
│ AccountMeta (56 字节)             │  lamports, owner, executable, rent_epoch
│ ObsoleteAccountHash (32 字节)     │  (已废弃，保留用于格式兼容)
│ data (变长)                       │  账户的实际数据
│ 对齐 padding (到 8 字节边界)       │
└──────────────────────────────────┘
```

核心设计原则：
- **单写多读**：用一个原子的 `offset` 指针实现追加，写者更新 offset、读者通过 mmap 直接访问，**读操作完全无锁**。
- **顺序写**：所有写入都是追加，接近 NVMe 的原始带宽（2700 MB/s）。
- **零拷贝读**：通过 mmap 直接返回内存指针，不需要把数据复制到用户空间。
- 每个 banking/replay 线程有**自己独立的 AppendVec 文件**，多线程并发写互不阻塞。

#### 结构二：AccountsIndex（全内存索引）

```rust
type AccountsIndex = HashMap<Pubkey, AccountMap>;
type AccountMap   = HashMap<Fork, (AppendVecId, u64)>;  // (文件id, 字节偏移)
```

读取一个账户的完整路径：

```
1. 查 AccountsIndex（内存 HashMap）→ 得到 (AppendVecId, offset)     [纯内存，无 IO]
2. 通过 mmap 访问对应文件的指定偏移 → 直接返回引用                    [1 次 mmap，OS 缓存热数据]
```

**没有树、没有路径遍历、没有多次寻道。** 相比以太坊沿 MPT 走 15 层，Solana 的读取本质上是一次哈希表查找 + 一次内存访问。

#### 旧版本处理与 Fork（分叉）回滚

AccountsMap 里保存了每个账户在**每个 Fork 上**的位置。这天然支持回滚：把某个 Fork 的记录删掉即可。

当一个 Fork 被共识确认（squash）后：
1. 把父 Fork 中尚未出现在当前 Fork 的账户「提升」到当前 Fork
2. 余额为零的账户从索引中删除
3. 旧 Fork 的索引项标记为可回收

#### 垃圾回收：Shrink 操作

追加写必然产生大量「死记录」（账户被更新后，旧版本数据停留在原位）。**Shrink** 是后台 GC 线程：

```
扫描碎片率高的 AppendVec 文件
  → 把文件内仍活跃的账户复制到新 AppendVec
  → 更新 AccountsIndex 指向新位置
  → 删除旧文件
```

这和 QMDB 的 Compaction、LSM 的 Compaction 是同一个思路：用后台搬迁代替实时原地修改。

#### 快照（Snapshot）

因为 AccountsIndex 是纯内存结构，重启后需要重建。Solana 提供两种机制：

- **Snapshot**：定期把所有 AppendVec mmap 文件 flush 到磁盘，并序列化 Index。启动时加载 Snapshot 而不是从头回放所有交易。
- **Index Recovery**：如果没有 Snapshot，可以扫描所有 AppendVec 文件的写入版本号（write_version）来重建索引，不依赖文件顺序。

### 7.3 状态根（State Hash）：不是 Merkle Tree

这是 Solana 和以太坊最根本的差异之一。

以太坊的 `state_root` 是 MPT 的根哈希，任何账户状态都可以提供 Merkle Proof。

**Solana 在 2024 年彻底移除了 Merkle 树的账户哈希计算**，改用 **LtHash（Lattice Hash，格哈希）**：

$$\text{AccountsLtHash} = \bigoplus_{i} \text{LtHash}(\text{account}_i)$$

其中 $\bigoplus$ 是特定群上的加法运算（本质上是一种**线性同态哈希**）。

**LtHash 的核心性质**：

```
整体哈希 = account_1的哈希 ⊕ account_2的哈希 ⊕ ... ⊕ account_n的哈希

当 account_k 从旧值 V_old 变成新值 V_new 时：
新整体哈希 = 整体哈希 ⊕ LtHash(account_k, V_old) ⊕ LtHash(account_k, V_new)
             就是把旧的「撤销」，把新的「加进去」
```

**每次账户更新只需 2 次哈希运算和 2 次 XOR，与总账户数无关——O(1)**。

相比之下，以太坊 MPT 每次更新需要重算路径上约 15 个节点的哈希，与 N 的对数成正比。

**LtHash 的代价**：

LtHash 是累加型哈希，没有树结构，**无法生成「某账户在某根下存在」的 Merkle Proof**。这意味着：
- **没有轻客户端支持**。手机钱包必须完全信任节点返回的数据，或者连接全节点。
- **没有无信任跨链状态证明**。目前 Solana 跨链依赖的是多签桥（Wormhole 等），正是因为无法做状态 Merkle Proof。

### 7.4 全面对比：Solana AccountsDB vs 以太坊 MPT

| 维度 | Solana AccountsDB | 以太坊 MPT + RocksDB |
|------|-------------------|--------------------|
| **底层存储** | AppendVec（mmap 顺序写文件） | RocksDB（LSM 树，随机写） |
| **写模式** | 追加写，接近 NVMe 原始带宽 | 随机写，高写放大（15~20×） |
| **每次状态写 IO** | O(1)，一次 append | O((log N)²)，随机寻道 |
| **读路径** | HashMap O(1) + 一次 mmap | 沿 MPT 遍历 ~15 步，多次随机 IO |
| **状态根计算** | LtHash，O(1) 增量更新 | Merkle 根，O(log N) 重算路径 |
| **状态根更新代价** | 2 次哈希 + 2 次 XOR | ~15 次哈希（路径上每层一次） |
| **Merkle Proof** | **不支持** | 完整支持（`eth_getProof`） |
| **轻客户端** | **不可行** | 可行（SPV 验证） |
| **DRAM 需求** | 极高（整个 AccountsIndex 必须在内存，256GB+） | 较低（只缓存热节点） |
| **Fork / 回滚** | AccountsMap 里保存每个 Fork 的指针，O(1) 回滚 | 重放交易或维护旧版本树节点 |
| **GC 机制** | Shrink（后台搬迁死记录） | MPT 旧节点不会被立即删除，需要 state pruning |
| **并发写** | 多线程各写自己的 AppendVec，完全无锁 | MPT 路径需要加锁，竞争激烈 |
| **快照** | 定期 flush AppendVec + Index 序列化 | 通过 EIP-4444 裁剪旧历史，归档另存 |

### 7.5 Solana AccountsDB 的缺陷

#### 缺陷 1：DRAM 需求极高，去中心化受限

AccountsIndex 必须完全放在 DRAM 里才能做到 O(1) 读取。Solana 主网当前账户数约 **1.5 亿个**，索引大小约 **50~100 GB DRAM**，再加上操作系统的 mmap 页面缓存，全节点验证器实际通常需要 **256 GB 以上 DRAM**。

这是一台超过 5000 美元的服务器配置，普通家用机无法参与验证——这造成了 Solana 验证器的准入门槛极高，是去中心化上的重要短板。

#### 缺陷 2：没有 Merkle Proof，没有轻客户端

如前所述，LtHash 的设计决策彻底放弃了轻客户端可验证性。Solana 手机钱包只能信任节点，跨链桥必须依赖多签。这是 Solana 生态一个长期存在的安全假设弱点。

#### 缺陷 3：Shrink GC 会带来性能毛刺

Shrink 操作需要扫描 AppendVec 文件、复制活跃账户、更新索引——这是一个写密集型操作。在主网高峰期，GC 和正常交易处理争抢 NVMe 带宽，可能导致处理延迟上升（**GC pause**，类似 JVM GC 停顿）。

Shrink 的触发策略（什么时候做、做多少）需要精心调优，否则要么迟到（磁盘被死记录占满），要么过早（频繁 GC 抢带宽）。

#### 缺陷 4：快照加载时间长

Snapshot 文件是所有 AppendVec mmap 文件的镜像，主网 Snapshot 大小通常在 **50~100 GB** 以上。从新节点启动或 Snapshot 恢复，下载和加载 Snapshot 本身就需要数小时。加上 Index 重建（扫描所有 AppendVec），启动时间是 Solana 节点运维的已知痛点。

#### 缺陷 5：分层存储已被删除

Solana 曾经实现过「冷热账户分层存储」（Tiered Storage）——把长期未活跃的账户移到更便宜的存储介质。但截至 2025 年初，Anza 已将该功能**从主分支完全删除**。目前所有账户不论活跃与否，都存在同一层 NVMe 上，对磁盘容量不友好。

### 7.6 小结：Solana 的取舍哲学

Solana 在全局账户存储上的取舍和以太坊截然相反：

> **以太坊**：优先保证「任何人都可以用一个小证明验证任意状态」（轻客户端、状态证明），代价是复杂的写放大和 DRAM 压力。
>
> **Solana**：优先保证「全节点能以极高吞吐量读写状态」，代价是必须跑全节点（高 DRAM 门槛），轻客户端不可行，跨链安全假设更弱。

这是两种不同的去中心化哲学：以太坊的世界里，轻客户端可以很便宜地参与验证；Solana 的世界里，验证者门槛更高，但吞吐量上限也远高于以太坊的当前实现。

QMDB 试图走第三条路：用 Twig 压缩把 Merkle 化做到 DRAM 内完成，在保留 Merkle Proof 能力（不像 Solana 那样放弃）的同时，把 IO 复杂度降到和 Solana AppendVec 同一个数量级。这是目前看来最有希望同时兼顾两者的方向。

---

## 八、QMDB 用 Demo 数据说清楚：查询和证明到底是怎么做的

### 8.1 先用一个极简场景建立直觉

假设数据库里只有四个账户，经过若干区块的操作：

| 操作时间 | 操作 | Key | Value |
|---------|-----|-----|-------|
| 区块 100 | 创建 | alice | 50 |
| 区块 100 | 创建 | bob | 80 |
| 区块 200 | 更新 | alice | 120 |
| 区块 200 | 创建 | carol | 30 |
| 区块 350 | 更新 | alice | 90 |
| 区块 350 | 删除 | bob | — |

QMDB 对**每次操作**都 append 一条 Entry（永不原地修改），按时间顺序排列：

```
SSD 文件（按 append 顺序）：

  偏移  Id  Key      Value  NextKey  OldId  Version
  ─────────────────────────────────────────────────────
  0000  #1  alice      50    bob     null   h=100
  0200  #2  bob        80    carol   null   h=100
  0400  #3  carol      30    ∞(末尾) null   h=100  ← 创建carol同时更新bob的NextKey
  0600  #4  bob(更新)  80    carol   #2     h=100  ← 修正bob的NextKey指向carol
  0800  #5  alice     120    bob     #1     h=200  ← alice更新，OldId指向旧Entry#1
  1000  #6  carol      30    ∞       #3     h=200  ← 创建carol需要更新前驱
  1200  #7  alice      90    bob     #5     h=350  ← alice再次更新
  1400  #8  bob_prev(更新) 80  carol #4    h=350  ← 删bob时更新bob前驱(alice)的NextKey
```

> 注：为了清晰，上面做了适当简化。真实中 Key 存的是 `Hash(application_key)` 的高28字节，NextKey 指向字典序后继。

内存里维护的**索引**（只有当前最新状态）：

```
索引（DRAM，B-tree）：
  Hash(alice) → 文件偏移 1200  （指向 #7，最新版本）
  Hash(carol) → 文件偏移 0400  （指向 #3，唯一版本）
  （bob 已被删除，索引中无记录）
```

---

### 8.2 问题一：怎么快速查询某个 Key？

**查询：alice 当前的值是多少？**

步骤极其简单：

```
第一步：查内存索引
  输入：Hash("alice")
  输出：文件偏移 = 1200
  耗时：内存 B-tree 查找，几微秒

第二步：读 SSD
  到文件偏移 1200，读出 Entry #7
  Entry #7 = { Key=alice, Value=90, OldId=#5, Version=h=350 }
  耗时：1 次 SSD 随机读，约 100 微秒

结果：alice = 90
总 IO：1 次 SSD 读
```

**对比以太坊 MPT 的读取**：

```
以太坊读 alice：
  从根节点开始，沿 MPT 路径走
  每一层：在 RocksDB 里查一次节点哈希 → 解码 RLP → 找到下一层指针
  共约 15 层 → 15 次 RocksDB 随机读（每次可能有 LSM 层合并开销）

总 IO：~15 次 SSD 随机读
```

> QMDB 读取快的根本原因：**索引直接记录物理文件偏移，不走任何树结构**。

---

### 8.3 问题二：怎么证明某个 Key 在某个历史高度的状态？

这是 QMDB 更难的创新点。用两个子问题拆解：

---

#### 子问题 A：证明「alice 在高度 250 的值是 120」

此时 alice 经历过：
- h=100 时创建，值=50（Entry #1）
- h=200 时更新，值=120（Entry #5）  ← 高度 250 时的有效版本
- h=350 时更新，值=90（Entry #7）

**构造证明的步骤（节点做的事）**：

```
1. 从当前最新 Entry #7（alice=90, OldId=#5）开始回溯
2. 看 Entry #7.Version = h=350 > 250，继续往前
3. 跳到 OldId → Entry #5（alice=120, Version=h=200, OldId=#1）
4. Entry #5.Version = h=200 ≤ 250，而下一版本 Entry #7.Version = h=350 > 250
5. 确认：高度 250 时，alice 的有效 Entry 是 #5，值=120
```

**证明包含什么**：

```
Proof = {
  entry:        Entry #5 的完整内容（alice=120, Version=h=200）
  twig_proof:   Entry #5 所在 Twig 内的 Merkle 路径（log₂(2048)=11 个哈希）
  upper_proof:  从 Twig 根到全局根的路径（log₂(总Twig数) 个哈希）
  next_entry:   Entry #7 的内容（alice=90, Version=h=350）
  next_proof:   Entry #7 所在 Twig 的 Merkle 路径
}
```

**验证者（轻客户端）如何核验**：

```
✅ 验证 Entry #5 的 Merkle 路径是否能推导出全局状态根 → 确认 Entry #5 真实存在于链上
✅ 验证 Entry #7 的 Merkle 路径是否能推导出全局状态根 → 确认 Entry #7 真实存在于链上
✅ 验证 Entry #7.OldId == Entry #5.Id → 确认 #7 是 #5 的直接后继版本（中间没有其他版本）
✅ 验证 Entry #5.Version = h=200 ≤ 250 < Entry #7.Version = h=350
   → 逻辑闭合：在高度 250，alice 的最新版本确实是 Entry #5，值=120
❌ 如果节点造假，无法同时满足上面所有条件
```

---

#### 子问题 B：证明「bob 在高度 250 时已存在，值=80」（以及 bob 在高度 400 时已被删除）

```
bob 的历史：
  h=100 创建   Entry #2（bob=80）
  h=100 更新   Entry #4（bob=80，修正NextKey）← 同块内为carol创建而做的更新
  h=350 被删除（bob 从索引消失，前驱 alice 的 NextKey 改了）

在高度 250：bob 存在，值=80
在高度 400：bob 不存在（已被删除）
```

**证明「bob 在高度 400 不存在」（排除证明）**：

```
找 bob 字典序上紧邻的前驱和后继的 Entry：
  前驱（此时是 alice）的最新 Entry #8：NextKey = carol（bob 已不在中间了）
  Entry #8.Version = h=350

Proof = {
  predecessor_entry: Entry #8（alice，NextKey=carol，Version=h=350）
  predecessor_proof: Entry #8 的 Merkle 路径 → 全局根
}

验证逻辑：
  ✅ Entry #8 存在于链上（Merkle 路径有效）
  ✅ Entry #8.Key < Hash("bob") < Entry #8.NextKey（bob 夹在 alice 和 carol 之间）
  ✅ Entry #8.Version = h=350 ≤ 400，且前驱 NextKey 已跳过 bob
  → bob 在高度 400 不存在
```

> 这就是为什么 Entry 要存 `NextKey`：通过前驱的 `NextKey` 字段的变化，可以精确地证明某个 key 在某个时刻「消失了」。

---

### 8.4 Twig 是怎么让 Merkle 化零 IO 的？

上面说到「Entry #5 的 Twig 内的 Merkle 路径」，这个路径是怎么计算的？这里用数据把 Twig 结构说清楚。

**假设一个 Twig 包含 8 个 Entry（简化版，实际是 2048 个）**：

```
Twig #0（存在 SSD 上，内容不变：）
┌───────────────────────────────────────┐
│ Entry #0: alice=50,  h=100           │ ← 活跃（ActiveBit=1）
│ Entry #1: bob=80,    h=100           │ ← 已失效（被#3覆盖）
│ Entry #2: carol=30,  h=100           │ ← 活跃（ActiveBit=1）
│ Entry #3: bob=80,    h=100           │ ← 活跃（ActiveBit=1）
│ Entry #4: alice=120, h=200           │ ← 已失效（被#6覆盖）
│ Entry #5: carol=30,  h=200           │ ← 活跃（ActiveBit=1）
│ Entry #6: alice=90,  h=350           │ ← 活跃（ActiveBit=1）
│ Entry #7: alice=80,  h=350           │ ← 活跃（ActiveBit=1）
└───────────────────────────────────────┘

DRAM 里只保留这个 Twig 的：
  twig_root:  Hash(H01, H23, H45, H67)... = 0xABCD...  （32字节）
  ActiveBits: 11011011                                   （8bit，1=活跃）
  共 33 字节（实际2048个Entry需要32+256=288字节）
```

**当 alice 在 h=350 被更新（Entry #6 写入），ActiveBit[4]=0，需要重算 twig_root**：

```
重算过程（完全在 DRAM 内）：

旧 Twig 树：
         root
        /    \
     H0123   H4567
     /   \    /   \
   H01  H23  H45  H67
   / \  / \  / \  / \
  H0 H1 H2 H3 H4 H5 H6 H7

ActiveBit[4] 从 1 变为 0 → H4 变为 NULL_HASH（代表"此Entry已无效"）

重算路径（只需 3 次哈希）：
  H45' = Hash(NULL_HASH, H5)  ← H4 变了，重算 H45
  H4567' = Hash(H45', H67)    ← H45 变了，重算 H4567
  root' = Hash(H0123, H4567') ← H4567 变了，重算 root

全程：3 次哈希计算，0 次 SSD IO
```

**关键**：H0~H7 这些哈希值是在写入 SSD 的时候就算好并缓存在内存里的——Entry 内容 append 即不变，对应的叶子哈希也不变。所以每次只需沿路径重算几个节点，而不需要从 SSD 重新读 Entry 数据。

---

### 8.5 整体流程一张图

```
写入（区块处理）：
  新 Entry → 追加到 Fresh Twig（DRAM）
           → 更新内存索引（Hash(key) → 新偏移）
           → 更新 ActiveBits（旧Entry的bit清零）
           → 在 DRAM 内重新计算 Twig 根和祖先节点（0次SSD IO）
           → 2048条攒满后，整体顺序写入SSD（1次大批写）

读取（查询当前状态）：
  Hash(key) → 内存索引 → 文件偏移 → SSD 1次读 → 返回值

历史状态证明（证明key在高度h的值）：
  当前Entry → 沿 OldId 链回溯 → 找到覆盖h的Entry → 读SSD N次
  → 拼装 Merkle 路径（从 DRAM 中的 Twig/上层节点哈希直接取）
  → 构造 Proof（Entry内容 + Merkle路径 + 相邻版本的Entry）

验证（轻客户端）：
  验证 Merkle 路径 → 验证版本区间 → 验证OldId链 → 结论
```

---

### 8.6 一句话总结 QMDB 解决两个问题的方法

**查询快**：内存索引直接映射到物理文件偏移，1 次 SSD 读，不走任何树。

**历史证明**：Entry 里用 `OldId` 串起每个 key 的完整历史链。证明某 key 在高度 h 的值，就是找到覆盖 h 的那条版本，提供它的 Merkle 路径 + 前后版本的 Entry 佐证。Merkle 路径的计算完全靠 DRAM 里已有的 Twig 哈希和上层节点，无需读 SSD。

