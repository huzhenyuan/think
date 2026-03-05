# Solana AccountsDB 深度技术分析与 Ethereum MPT 对比

> 研究日期：2026年3月  
> 来源：agave/accounts-db 源码、Solana persistent-account-storage 设计文档、append_vec.rs 实际实现

---

## 1. 整体架构总览

### 1.1 Solana 的状态存储体系

Solana 的全局状态（所有账户）由三个层次管理：

```
┌──────────────────────────────────────────┐
│         AccountsIndex (内存)              │ ← 纯 DRAM，HashMap
│  HashMap<Pubkey, HashMap<Fork, (AppendVecId, offset)>>  │
└─────────────────┬────────────────────────┘
                  │ 指针：哪个文件 + 哪个字节偏移
┌─────────────────▼────────────────────────┐
│         AppendVec 文件集合 (NVMe)         │ ← 追加写，memory-mapped
│   slot_0_0.bin  slot_1_5.bin  ...        │
└──────────────────────────────────────────┘
                  +
┌──────────────────────────────────────────┐
│      ReadOnlyAccountsCache (内存)         │ ← LRU 缓存热账户
└──────────────────────────────────────────┘
```

**没有 RocksDB 存储账户数据**。RocksDB 在 Solana 中只用于 Blockstore（ledger/shred 数据）。账户数据完全存在 AppendVec 文件中。

---

## 2. AppendVec：核心存储原语

### 2.1 定义与结构

`AppendVec` 是 Solana 账户存储的基础数据结构，定义在 `accounts-db/src/append_vec.rs`：

```rust
pub struct AppendVec {
    path: PathBuf,                    // 磁盘文件路径
    backing: AppendVecFileBacking,    // Mmap 或 File 两种 backing
    read_write_state: ReadWriteState, // ReadOnly / Writable（含 Mutex）
    current_len: AtomicUsize,         // 已使用字节数（原子变量）
    file_size: u64,                   // 最大容量（最大 16 GiB）
    remove_file_on_drop: AtomicBool,  // Drop 时是否删除文件
    is_dirty: AtomicBool,             // 是否需要 flush
}
```

### 2.2 两种访问模式（StorageAccess）

| 模式 | 描述 | 适用场景 |
|------|------|----------|
| `Mmap`（memory-mapped） | 内存映射，OS 页缓存管理，随机访问零拷贝 | 新写入、活跃 slot |
| `File`（direct file I/O） | 直接文件读写，按需读取，内存占用小 | 老旧/只读 AppendVec |

默认写入使用 Mmap，读取可以是任意一种。新设计趋向更多使用 File I/O 以降低内存压力。

### 2.3 磁盘上的账户记录格式（每条记录布局）

每个账户在 AppendVec 中的存储布局（严格按 64-bit 对齐）：

```
┌─────────────────────────────────────────────────┐
│  StoredMeta (48 bytes)                           │
│    write_version_obsolete: u64  (8 bytes, 已废弃)│
│    data_len: u64                (8 bytes)        │
│    pubkey: Pubkey               (32 bytes)       │
├─────────────────────────────────────────────────┤
│  AccountMeta (56 bytes)                          │
│    lamports: u64                (8 bytes)        │
│    rent_epoch: u64              (8 bytes)        │
│    owner: Pubkey                (32 bytes)       │
│    executable: bool             (1 byte + 7 pad) │
├─────────────────────────────────────────────────┤
│  ObsoleteAccountHash (32 bytes, 全 0 填充)       │
│    [已废弃的旧版每账户哈希字段]                  │
├─────────────────────────────────────────────────┤
│  account data (data_len bytes)                  │
│  [pad to 64-bit boundary]                       │
└─────────────────────────────────────────────────┘

STORE_META_OVERHEAD = 136 bytes（固定元数据部分）
最大 AppendVec 文件大小 = 16 GiB
```

常量 `STORE_META_OVERHEAD = 136` 字节 = 48 + 56 + 32。

### 2.4 并发模型：单写多读

```
写入（append）：
  - Mutex<()> 保证只有一个线程追加
  - atomic offset 在追加完成后用 store(Release) 更新
  - 其他线程立即可用 load(Acquire) 看到新数据

读取：
  - 任意多个线程并发读取，无锁
  - Mmap 模式：返回内存引用（零拷贝）
  - File 模式：read_into_buffer，4KiB 页缓冲
```

写操作通过 `append_ptrs_locked()` 实现：先计算所有字段总大小校验是否有剩余空间，再逐段 `ptr::copy`，最后 `current_len.store(offset, Release)` 使其对读者可见。**整个过程不需要读者持有任何锁。**

---

## 3. AccountsIndex：内存中的倒排索引

### 3.1 数据结构

```rust
type AppendVecId = usize;
type Fork = u64;  // slot 号

struct AccountMap(HashMap<Fork, (AppendVecId, u64)>);
//                                             ↑ offset in AppendVec

type AccountsIndex = HashMap<Pubkey, AccountMap>;
```

含义：给定一个账户的 Pubkey，可以查到它在某个 Fork（slot）下存放在哪个 AppendVec 文件的哪个字节偏移处。

### 3.2 多 Fork 支持（可回滚）

在 Tower BFT 确认最终 root fork 之前，多条分叉并存。索引同时保存所有 fork 的版本，查询时按 fork 号回溯父链，找到最近有效版本：

```rust
pub fn load_slow(&self, id: Fork, pubkey: &Pubkey) -> Option<&Account>
// 先查 fork id，没有则递归查父 fork
```

### 3.3 Root Fork 与 Squash

当 Tower BFT 选定一个 root fork：

- **Squash（压缩）**：把父 fork 中未被当前 fork 覆盖的账户"提升"到新 root，更新索引
- **零余额账户剔除**：lamports = 0 的账户从索引中移除（视为已删除）
- **垃圾回收**：索引更新后，旧的 AppendVec 引用计数变为 0，文件可删除

### 3.4 索引性能

- 单线程 HashMap 更新：~10M 次/秒
- 写需要独占锁；读并发无锁
- 每个 entry 远比 Ethereum MPT 节点轻量：一个 Pubkey → (AppendVecId, offset) 的单个 u64 对

---

## 4. Shrink（账户压缩）操作

### 4.1 为什么需要 Shrink

由于 AppendVec 是追加写，账户每次更新都**在新位置写入新版本**，旧版本留在原处变成"死数据"（dead bytes）。随着时间推移，一个 AppendVec 文件中可能大部分都是过期版本：

```
AppendVec_A:
  [pubkey_X v1 - DEAD]  [pubkey_Y v1 - DEAD]  [pubkey_Z v1 - LIVE]
  [pubkey_X v2 - DEAD]  [pubkey_W v1 - LIVE]
  利用率 = 2/(5 entries) × 字节占比 ≈ 很低
```

### 4.2 Shrink 的执行流程

Shrink 是 Solana 的"垃圾回收"，对应 `ancient_append_vecs.rs`（古老 AppendVec 的整合）：

```
1. 扫描已 root 的旧 AppendVec，找出"活跃账户"密度低的文件
2. 从这些文件中读出仍然存活的账户（最新版本）
3. 将它们写入一个新的（更密集的）AppendVec
4. 更新 AccountsIndex 指向新位置
5. 删除旧的稀疏 AppendVec 文件
```

关键特性：
- **不需要对所有读者加全局锁**：只需在 index 更新时做原子切换
- **可并发进行**：后台线程执行，不阻塞交易处理
- **结合 `dead_bytes_due_to_zero_lamport_single_ref`**：零余额账户的"dead bytes"可以及时释放

### 4.3 Shrink 的触发时机

- 当某个 AppendVec 文件的"死字节"占比超过阈值
- 周期性后台任务（类似 LSM-tree 的 compaction）
- 快照生成前可能触发

---

## 5. 状态哈希：从 Merkle 到 Lattice Hash

### 5.1 旧版：Merkle 树 Hash（已移除）

旧版 Solana 曾使用 SHA-256 树哈希：

- 对所有账户按 Pubkey 排序
- 分批（bin）并行计算叶子哈希
- 构建 Merkle 树

**问题**：
- 每次状态变化需要重新计算受影响的所有 Merkle 路径
- 扩展性差，不支持增量更新
- 大量 CPU 开销（全节点扫描）

### 5.2 新版：Lattice Hash（LtHash）

**2024-2025 年 Anza 用 Lattice Hash 完全替换了 Merkle 树**（参见 commit：`Removes merkle-based accounts hash calculation (#7153)`）。

当前代码（`accounts-db/src/accounts_hash.rs`）极为简洁：

```rust
use solana_lattice_hash::lt_hash::LtHash;

/// 单个账户的 Lattice hash
pub struct AccountLtHash(pub LtHash);

/// 零余额账户的 LtHash（恒等元素）
pub const ZERO_LAMPORT_ACCOUNT_LT_HASH: AccountLtHash =
    AccountLtHash(LtHash::identity());

/// 所有账户的全局 Lattice hash（即状态根）
pub struct AccountsLtHash(pub LtHash);
```

### 5.3 Lattice Hash 的数学原理

LtHash 基于**多项式环上的可交换群操作**（XOR over GF(2^128) polynomials or similar structure）：

$$\text{AccountsLtHash} = \bigoplus_{i} \text{HashAccount}(a_i)$$

其中 $\bigoplus$ 是群操作（可交换、可逆的 XOR-like 运算）。

**关键性质**：

| 性质 | 含义 |
|------|------|
| **交换性** | 账户顺序无关，不需要排序 |
| **增量更新** | 账户更新时：移除旧哈希，加入新哈希，$O(1)$ |
| **可逆** | 支持删除：$H_{new} = H_{old} \oplus H(a_{old}) \oplus H(a_{new})$ |

对比 Ethereum MPT：任何一个账户的变更需要从叶子到根重新计算 $O(\log N)$ 个节点。对于 Solana 每秒处理数百万账户更新，这是质的提升。

### 5.4 ObsoleteAccountHash 字段

AppendVec 中每条记录里仍然有 32 字节的 `ObsoleteAccountHash` 字段，但**全部填 0**（`ZEROED`），该字段是当年旧版每账户哈希的遗留，迁移到 LtHash 后已经废弃但出于文件格式兼容性保留。

---

## 6. 快照（Snapshots）

### 6.1 快照的内容

快照是一个时间点的完整账户状态，内容：

1. **所有 AppendVec 文件**（mmap flush 到磁盘）
2. **AccountsIndex 序列化**（但实际上从文件重建更常见）
3. **LtHash 状态根**

### 6.2 Full vs. Incremental Snapshot

- **Full Snapshot**：完整打包所有账户数据（可达数百 GB）
- **Incremental Snapshot**：只包含自上次 full snapshot 以来变更的账户

代码中有 `IncrementalAccountsHash` 类型与 `AccountsLtHash` 对应。

### 6.3 快照的启动加速（Fastboot）

Agave 支持 fastboot：validator 重启时直接 mmap 加载之前持久化的 AppendVec 文件，无需重新加载账户到内存。只有被访问的页才会实际从磁盘读入（OS 懒加载）。

这就是为什么 `AppendVec` 跟踪 `is_dirty`：只有真正被写过的文件才需要 `flush()`：

```rust
pub fn flush(&self) -> Result<()> {
    let should_flush = self.is_dirty.swap(false, AcqRel);
    if should_flush {
        mmap.flush()?;  // or file.sync_all()
    }
    Ok(())
}
```

---

## 7. 与 Ethereum MPT（Merkle Patricia Trie）的全面对比

### 7.1 数据结构对比

| 维度 | Solana AccountsDB | Ethereum MPT |
|------|-------------------|--------------|
| **底层存储** | AppendVec（追加写文件，mmap） | LevelDB/RocksDB（LSM-tree） |
| **数据结构** | 扁平 AppendVec + 内存索引 | Merkle Patricia Trie（key-value 编码） |
| **账户定位** | Pubkey → (file_id, offset) O(1) | keccak256(address) 路径遍历 O(lg N) |
| **节点类型** | 无节点概念，raw bytes | Branch/Extension/Leaf 节点 |
| **key 编码** | Pubkey 直接作为索引键 | Nibble-encoded hex path |

### 7.2 写 IO 对比

**Solana AppendVec**：

```
写路径：
  交易执行 → 账户更新 → append 到当前 slot 的 AppendVec
  └─ 完全顺序追加写
  └─ 多个 banking 线程各写自己的 AppendVec（完全并行）
  └─ NVMe 顺序写带宽：2700 MB/s
  └─ 32 线程/NVMe 最优
```

**Ethereum MPT**：

```
写路径：
  交易执行 → 叶子更新 → 从叶子到根重新计算/写入所有 dirty 节点
  └─ 随机写（MPT 节点散布在 LevelDB 中）
  └─ LevelDB B-tree 页写放大：通常 4-8x
  └─ 写放大（Write Amplification）极高
```

| 写 IO 指标 | Solana | Ethereum |
|-----------|--------|----------|
| 写模式 | 纯顺序追加 | 随机写（MPT 节点） |
| 写放大 | ~1x（理想）；Shrink 时略高 | 4-30x（LevelDB compaction + MPT） |
| 并发写 | 多线程各自独立 AppendVec | 需锁定 MPT 路径 |
| 顺序带宽利用 | 接近 100% NVMe 带宽 | 受限于随机 IOPS |

### 7.3 读 IO 对比

**Solana**：

```
读路径：
  1. AccountsIndex 查 Pubkey → (AppendVecId, offset)  [纯内存]
  2. mmap 访问 AppendVec[offset]                      [O(1)，可能触发 page fault]
  3. 可能命中 ReadOnlyAccountsCache（热账户 LRU）
```

- 典型路径：1 次内存哈希查找 + 1 次内存地址访问（OS 可能触发 page fault）
- **零拷贝**：Mmap 模式下返回指向 mmap 内存的引用，不复制数据

**Ethereum**：

```
读路径：
  1. keccak256(address) → 16进制 nibble path
  2. 从 MPT 根节点开始遍历（Branch/Extension/Leaf）
  3. 每个节点一次 LevelDB 随机读
  4. 平均路径深度 ~6-8 个节点
```

- 典型路径：6-8 次 LevelDB 随机读（磁盘 IOPS 受限）
- 布隆过滤器可减少一定不存在账户的读放大

| 读 IO 指标 | Solana | Ethereum |
|-----------|--------|----------|
| 单账户读 | O(1) 内存查找 + 1次 mmap 访问 | O(log N) 随机磁盘读 |
| 读放大 | ~1x | 6-8x（MPT 节点解码） |
| 冷启动读 | page fault（OS 懒加载） | LevelDB page cache |

### 7.4 DRAM 使用对比

**Solana**：

| 组件 | 内存占用 |
|------|----------|
| AccountsIndex | **巨大**：每个账户 ~100-200 字节（HashMap entry overhead） |
| ReadOnlyAccountsCache | 可配置，热账户数据 |
| AppendVec mmap | 由 OS page cache 管理，不完全常驻 |
| **总计大约** | 主网 ~100-200GB DRAM（大量账户）|

**Ethereum**：

| 组件 | 内存占用 |
|------|----------|
| MPT 热节点缓存 | LevelDB block cache（可配置，通常 8-16GB） |
| Geth 内存 trie | 可选，大量内存 |
| 状态一般假设 | 磁盘为主，内存为缓存 |
| **总计** | 通常 32-64GB 可运行，但 sync 期间更多 |

Solana 的 AccountsIndex **必须完全在 DRAM 中**，这是最大的内存压力所在。主网有约 4-5 亿账户，索引本身就需要数十 GB。

### 7.5 状态哈希/状态根对比

| | Solana | Ethereum |
|--|--------|----------|
| **数据结构** | Lattice Hash (LtHash) | Merkle Patricia Trie 根哈希 |
| **更新复杂度** | O(1) per account change | O(log N) per account change |
| **有序性要求** | 无（交换律保证） | 按 nibble 路径严格有序 |
| **状态证明** | 无默克尔证明（无法轻节点验证单账户） | 可生成 Merkle Proof |
| **历史查询** | 需要全快照或归档节点 | MPT 可通过根哈希查历史 |
| **增量性** | 天然增量（group operation） | 需要从受影响路径重算 |

**这是 Solana 和 Ethereum 设计哲学的核心分歧**：Solana 选择牺牲轻节点证明能力换取极致的写入和哈希更新性能。Ethereum MPT 设计允许 Merkle Proof，支持 light client。

---

## 8. Solana AccountsDB 的已知弱点

### 8.1 DRAM 需求极高

AccountsIndex 必须全部在 DRAM 中。随着 Solana 链上账户数量增长（目前已超 5 亿），验证器内存需求线性增长：

- 当前建议配置：256GB+ RAM
- 这限制了验证器的普及（成本高）

Ethereum 全节点可以用更少内存运行（MPT 可以只缓存热节点）。

### 8.2 Shrink 带来的 IO 放大（Write Amplification）

Shrink（垃圾回收）实质上是重写仍然存活的账户数据到新文件，会带来额外的写 IO（类似 LSM compaction）：

- 高度活跃的账户集合会导致频繁 Shrink
- Shrink 期间 CPU 和 IO 资源竞争

### 8.3 无 Merkle Proof / 轻节点不可行

LtHash 不支持对单个账户的包含证明（Inclusion Proof）。这意味着：

- **没有轻客户端**：无法向轻节点证明某个账户的状态
- 应用若需要账户状态验证，必须信任 RPC 节点或全节点
- 与 Ethereum 的 eth_getProof 相比是功能缺失

### 8.4 快照体积庞大

每次 Full Snapshot 包含所有账户数据（可达数百 GB）。Incremental Snapshot 虽然减小了大小，但新节点首次同步仍需下载完整快照。

对比 Ethereum：Snap-sync 利用 MPT 结构可以更高效地增量同步。

### 8.5 AppendVec 文件碎片化

长时间运行后可能积累大量小型稀疏 AppendVec 文件，需要 Shrink/Ancient 处理，期间增加后台 IO 压力。

### 8.6 索引重建成本高

若 AccountsIndex 无法从快照恢复（崩溃），需要从 0 扫描所有 AppendVec 文件重建索引，代价极高（O(所有账户数)）。

---

## 9. 近期重大变化：Tiered Storage 的移除

Solana 曾经实现了 **Tiered Storage**（分层存储）：将冷账户专门存储到 HDD 或低速设备，热账户保留在 NVMe。但根据最新 commit 记录：

- `Removes tiered storage (#10706)` - 上周
- `Removes Tiered Storage, take 2 (#10992)` - 6小时前

**Tiered Storage 已从 agave 主分支完全移除**。这是一个重大的架构退步（或简化），意味着当前 Solana 验证器所有账户数据仍然只在一层存储（NVMe）上，没有自动的冷热分层。

---

## 10. AccountsDB 与 LSM-tree 的类比

AccountsDB 的设计在某种程度上类似 LSM-tree（Log-Structured Merge-tree），但有显著差异：

| 特性 | Solana AccountsDB | 标准 LSM-tree (RocksDB) |
|------|-------------------|------------------------|
| **写入** | Append-only（每个 AppendVec）| Append-only（WAL + MemTable → SST） |
| **紧缩** | Shrink（移动 Live accounts） | Compaction（合并 SST 文件） |
| **索引** | 内存 HashMap（全 DRAM）| 每层 Block Index + Bloom Filter |
| **读复杂度** | O(1) DRAM 查找 + 1 次磁盘访问 | O(levels) 次磁盘查找 |
| **写放大** | 低（追加 + 偶发 Shrink） | 中等（多层 compaction） |
| **读放大** | 极低（直接定位）| 取决于层数 |

---

## 11. 总结：设计哲学对比

### Solana 的选择

> **为吞吐量优化**：每秒数千笔交易，每笔涉及多个账户写入，需要尽量减少写 IO 路径的复杂度。

- 顺序追加写 → 接近 NVMe 硬件带宽上限
- 内存索引 → O(1) 账户定位，无磁盘 IO 开销
- Lattice Hash → O(1) 状态根更新，无 Merkle 树遍历
- Mmap → 零拷贝读取，OS 负责页缓存

**代价**：需要大量 DRAM，无 Merkle Proof，快照庞大，轻节点不可行。

### Ethereum 的选择

> **为可验证性优化**：任何状态可以对任何人证明（Merkle Proof），支持轻客户端，历史状态可查。

- MPT 结构 → 每个状态有唯一的 Merkle 根，可生成证明
- LevelDB → 通用 K-V 存储，节点=键
- 状态树 → 天然支持历史状态切换（ snapshots via root hash）

**代价**：随机写 IO，高写放大，MPT 是 Ethereum 性能瓶颈的主要来源之一（这也是 Verkle Trie 提案的动机）。

---

## 附录：关键代码引用

### AppendVec 每条记录的固定头大小
```rust
pub const STORE_META_OVERHEAD: usize = 136;
// = size_of::<StoredMeta>()       // 48 bytes
// + size_of::<AccountMeta>()      // 56 bytes  
// + size_of::<ObsoleteAccountHash>() // 32 bytes
```

### 记录大小计算
```rust
pub fn calculate_stored_size(data_len: usize) -> usize {
    u64_align!(STORE_META_OVERHEAD + data_len)
    // 对齐到 8 字节边界
}
```

### LtHash 状态根（最新实现）
```rust
// accounts-db/src/accounts_hash.rs
use solana_lattice_hash::lt_hash::LtHash;

pub struct AccountLtHash(pub LtHash);
pub const ZERO_LAMPORT_ACCOUNT_LT_HASH: AccountLtHash =
    AccountLtHash(LtHash::identity());
pub struct AccountsLtHash(pub LtHash);
// 全局状态 = ⊕ over all accounts of hash(account)
```

### AppendVec 最大文件大小
```rust
pub const MAXIMUM_APPEND_VEC_FILE_SIZE: u64 = 16 * 1024 * 1024 * 1024; // 16 GiB
```

---

## 参考来源

1. `agave/accounts-db/src/append_vec.rs` — AppendVec 完整实现（2026年3月，最新 master）
2. `agave/accounts-db/src/accounts_hash.rs` — LtHash 状态根实现
3. `agave/accounts-db/src/` 目录结构 — accounts_index.rs, ancient_append_vecs.rs, accounts_db.rs 等
4. `solana-labs/solana/docs/src/implemented-proposals/persistent-account-storage.md` — 原始设计文档
5. `solana-labs/solana/docs/src/validator/blockstore.md` — Blockstore 与 AccountsDB 区分
6. GitHub commit log：`Removes merkle-based accounts hash calculation (#7153)`, `Removes tiered storage (#10706)`
