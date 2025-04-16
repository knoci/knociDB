package diskhash

import (
	"encoding/json"
	"errors"
	"github.com/spaolacci/murmur3"
	"io"
	"knocidb/diskhash/fs"
	"os"
	"path/filepath"
	"sync"
)

const (
	primaryFileName  = "HASH.PRIMARY"
	overflowFileName = "HASH.OVERFLOW"
	metaFileName     = "HASH.META"
	slotsPerBucket   = 31
	nextOffLen       = 8
	hashLen          = 4
)

// MatchKeyFunc用于确定槽位的键是否与存储的键匹配。
// 必须在Put/Get/Delete方法中提供这个函数。
// 当我们在哈希表中存储数据时，我们只存储键的哈希值和原始值。
// 所以当我们从哈希表中获取数据时，即使键的哈希值匹配，由于哈希冲突， 也不意味着键匹配。
// 因此我们需要提供一个函数来确定槽位的键是否与存储的键匹配。
type MatchKeyFunc func(Slot) (bool, error)

// Table是一个存储数据在磁盘上的哈希表。
// 它由两个文件组成，主文件和溢出文件。
// 每个文件被分成多个bucket，每个bucket包含多个槽位。
//
// 哈希表的总体设计基于线性哈希算法。
// 更多信息请参见doc中的Hash.pdf
type Table struct {
	primaryFile  fs.File // 主文件
	overflowFile fs.File // 溢出文件
	metaFile     fs.File // meta存储哈希表的元数据
	meta         *tableMeta
	mu           *sync.RWMutex // 并发锁
	options      Options
}

// tableMeta是哈希表的元数据。
type tableMeta struct {
	// 决定了用多少位来计算bucket的索引，当表进行分裂时，level会增加，从而实现哈希表的动态扩展
	Level            uint8
	SplitBucketIndex uint32
	NumBuckets       uint32
	NumKeys          uint32
	SlotValueLength  uint32
	BucketSize       uint32
	FreeBuckets      []int64
}

// Open打开一个哈希表。
// 如果哈希表不存在，将自动创建，它将打开主文件、溢出文件和元数据文件。
func Open(options Options) (*Table, error) {
	if err := checkOptions(options); err != nil {
		return nil, err
	}

	t := &Table{
		mu:      new(sync.RWMutex),
		options: options,
	}

	// 创建哈希表文件目录路径。
	if _, err := os.Stat(options.DirPath); err != nil {
		if err := os.MkdirAll(options.DirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// 打开Meta读取元数据。
	if err := t.readMeta(); err != nil {
		return nil, err
	}

	// 打开并初始化主文件。
	primaryFile, err := t.openFile(primaryFileName)
	if err != nil {
		return nil, err
	}
	// 如果主文件为空，初始化第一个桶。
	if primaryFile.Size() == int64(t.meta.BucketSize) {
		if err := primaryFile.Truncate(int64(t.meta.BucketSize)); err != nil {
			_ = primaryFile.Close()
			return nil, err
		}
	}
	t.primaryFile = primaryFile

	// 打开溢出文件
	overflowFile, err := t.openFile(overflowFileName)
	if err != nil {
		return nil, err
	}
	t.overflowFile = overflowFile

	return t, nil
}

func checkOptions(options Options) error {
	if options.DirPath == "" {
		return errors.New("dir path cannot be empty")
	}
	if options.SlotValueLength <= 0 {
		return errors.New("slot value length must be greater than 0")
	}
	if options.LoadFactor < 0 || options.LoadFactor > 1 {
		return errors.New("load factor must be between 0 and 1")
	}
	return nil
}

// read从元数据文件中读取元数据信息。
// 如果文件为空，初始化元数据信息。
func (t *Table) readMeta() error {
	file, err := fs.Open(filepath.Join(t.options.DirPath, metaFileName), fs.OSFileSystem)
	if err != nil {
		return err
	}
	t.metaFile = file
	t.meta = &tableMeta{}

	// 初始化meta。
	if file.Size() == 0 {
		t.meta.NumBuckets = 1
		t.meta.SlotValueLength = t.options.SlotValueLength
		t.meta.BucketSize = slotsPerBucket*(hashLen+t.meta.SlotValueLength) + nextOffLen
	} else {
		decoder := json.NewDecoder(t.metaFile)
		if err := decoder.Decode(t.meta); err != nil {
			return err
		}
		// 一旦设置了槽值长度，就不能更改。
		if t.meta.SlotValueLength != t.options.SlotValueLength {
			return errors.New("slot value length mismatch")
		}
	}

	return nil
}

// write将元数据信息以JSON格式写入元数据文件。
func (t *Table) writeMeta() error {
	encoder := json.NewEncoder(t.metaFile)
	return encoder.Encode(t.meta)
}

// Close关闭哈希表的文件。
func (t *Table) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.writeMeta(); err != nil {
		return err
	}

	_ = t.primaryFile.Close()
	_ = t.overflowFile.Close()
	_ = t.metaFile.Close()
	return nil
}

// Sync将哈希表的数据刷新到磁盘。
func (t *Table) Sync() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.primaryFile.Sync(); err != nil {
		return err
	}

	if err := t.overflowFile.Sync(); err != nil {
		return err
	}

	return nil
}

// Put将新的键值对放入哈希表。
func (t *Table) Put(key, value []byte, matchKey MatchKeyFunc) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(value) != int(t.meta.SlotValueLength) {
		return errors.New("value length must be equal to the length specified in the options")
	}

	// 让插槽写入器写入新插槽，根据密钥哈希得到相应的bucket，
	// 然后找到一个空插槽插入，如果没有空槽，将创建一个溢出桶。
	keyHash := getKeyHash(key)
	slot := &Slot{Hash: keyHash, Value: value}
	sw, err := t.getSlotWriter(slot.Hash, matchKey)
	if err != nil {
		return err
	}

	// 把新槽写入桶
	if err = sw.insertSlot(*slot, t); err != nil {
		return err
	}
	if err := sw.writeSlots(); err != nil {
		return err
	}
	//如果槽已经存在，则不需要更新meta
	//因为密钥的数量没有改变
	if sw.overwrite {
		return nil
	}

	t.meta.NumKeys++
	// 如果超过负载系数，则进行拆分
	keyRatio := float64(t.meta.NumKeys) / float64(t.meta.NumBuckets*slotsPerBucket)
	if keyRatio > t.options.LoadFactor {
		if err := t.split(); err != nil {
			return err
		}
	}
	return nil
}

// 寻找插入新槽的位置，返回一个槽写入器。
func (t *Table) getSlotWriter(keyHash uint32, matchKey MatchKeyFunc) (*slotWriter, error) {
	sw := &slotWriter{}
	bi := t.newBucketIterator(t.getKeyBucket(keyHash))
	// 遍历所有桶，包括溢出桶，寻找空位。
	for {
		b, err := bi.next()
		if err == io.EOF {
			return nil, errors.New("failed to put new slot")
		}
		if err != nil {
			return nil, err
		}

		sw.currentBucket = b
		// 遍历当前桶的所有槽来寻找空位。
		for i, slot := range b.slots {
			if slot.Hash == 0 {
				sw.currentSlotIndex = i
				return sw, nil
			}
			if slot.Hash != keyHash {
				continue
			}
			match, err := matchKey(slot)
			if err != nil {
				return nil, err
			}
			// 如果键已经存在，覆盖写。
			if match {
				sw.currentSlotIndex, sw.overwrite = i, true
				return sw, nil
			}
		}
		// bucket中没有空槽位且没有溢出bucket，
		// 设置currentSlotIndex为slotsPerBucket
		// write时会创建一个新的溢出bucket。
		if b.nextOffset == 0 {
			sw.currentSlotIndex = slotsPerBucket
			return sw, nil
		}
	}
}

// Get 从哈希表中获取键的值。
func (t *Table) Get(key []byte, matchKey MatchKeyFunc) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	keyHash := getKeyHash(key)
	startBucket := t.getKeyBucket(keyHash)
	bi := t.newBucketIterator(startBucket)
	// 遍历bucket和溢出bucket中的所有槽位， 找到要获取的槽位。
	for {
		b, err := bi.next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		for _, slot := range b.slots {
			// 如果槽位的哈希值为0，表示后续槽位都是空的，
			// （为什么？当我们写入新槽位时，会从bucket的开始处遍历，找到空槽位插入，
			// 当我们移除槽位时，会将后续槽位向前移动，所以所有非空槽位都是连续的）
			// 因此我们可以跳过当前bucket，移动到下一个bucket。
			if slot.Hash == 0 {
				break
			}

			if slot.Hash != keyHash {
				continue
			}
			if match, err := matchKey(slot); match || err != nil {
				return err
			}
		}
	}
}

// Delete 从哈希表中删除键。
func (t *Table) Delete(key []byte, matchKey MatchKeyFunc) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	keyHash := getKeyHash(key)
	bi := t.newBucketIterator(t.getKeyBucket(keyHash))
	// 遍历bucket和溢出bucket中的所有槽位， 找到要删除的槽位。
	for {
		b, err := bi.next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		for i, slot := range b.slots {
			if slot.Hash == 0 {
				break
			}
			if slot.Hash != keyHash {
				continue
			}
			match, err := matchKey(slot)
			if err != nil {
				return err
			}
			if !match {
				continue
			}
			// 找到后删除键
			b.removeSlot(i)
			if err := b.write(); err != nil {
				return err
			}
			t.meta.NumKeys--
			return nil
		}
	}
}

// 返回哈希表里key的数量
func (t *Table) Size() uint32 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.meta.NumKeys
}

// 获得key的哈希值
func getKeyHash(key []byte) uint32 {
	return murmur3.Sum32(key)
}

// 通过keyhash获取该放到哪个桶
func (t *Table) getKeyBucket(keyHash uint32) uint32 {
	// 先用Level位计算初始bucket索引,
	// 如果这个索引小于当前正在分裂的bucket索引(SplitBucketIndex)，
	// 就用Level+1位重新计算bucket索引 3),
	// 这样确保在分裂过程中，数据能够均匀分布到新旧bucket中。
	bucketIndex := keyHash & ((1 << t.meta.Level) - 1)
	if bucketIndex < t.meta.SplitBucketIndex {
		return keyHash & ((1 << (t.meta.Level + 1)) - 1)
	}
	return bucketIndex
}

func (t *Table) openFile(name string) (fs.File, error) {
	file, err := fs.Open(filepath.Join(t.options.DirPath, name), fs.OSFileSystem)
	if err != nil {
		return nil, err
	}
	// 初始化（虚拟）文件头
	// 文件中的第一个bucket大小未被使用，所以我们只是初始化它
	if file.Size() == 0 {
		if err := file.Truncate(int64(t.meta.BucketSize)); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	return file, nil
}

// 把当前的桶分开。
// 它将创建一个新的bucket，并重写当前bucket和溢出bucket中的所有插槽。
func (t *Table) split() error {
	// 要分开的bucket。
	splitBucketIndex := t.meta.SplitBucketIndex
	splitSlotWriter := &slotWriter{
		currentBucket: &bucket{
			file:       t.primaryFile,
			offset:     t.bucketOffset(splitBucketIndex),
			bucketSize: t.meta.BucketSize,
		},
	}

	// 创建一个新的bucket.
	newSlotWriter := &slotWriter{
		currentBucket: &bucket{
			file:       t.primaryFile,
			offset:     t.primaryFile.Size(),
			bucketSize: t.meta.BucketSize,
		},
	}
	if err := t.primaryFile.Truncate(int64(t.meta.BucketSize)); err != nil {
		return err
	}

	// 增加分裂bucket的索引
	t.meta.SplitBucketIndex++
	// 如果分裂bucket的索引等于1 << level，
	// 重置分裂bucket的索引为0，并增加level。
	if t.meta.SplitBucketIndex == 1<<t.meta.Level {
		t.meta.Level++
		t.meta.SplitBucketIndex = 0
	}

	// 遍历分裂bucket和溢出bucket中的所有槽位，
	// 重写所有槽位。
	var freeBuckets []int64
	bi := t.newBucketIterator(splitBucketIndex)
	for {
		b, err := bi.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		for _, slot := range b.slots {
			if slot.Hash == 0 {
				break
			}
			var insertErr error
			if t.getKeyBucket(slot.Hash) == splitBucketIndex {
				insertErr = splitSlotWriter.insertSlot(slot, t)
			} else {
				insertErr = newSlotWriter.insertSlot(slot, t)
			}
			if insertErr != nil {
				return insertErr
			}
		}
		// 如果分裂的bucket有溢出bucket，并且这些bucket不再使用，
		// 因为所有槽位都已经被重写，所以我们可以释放这些bucket。
		if b.nextOffset != 0 {
			freeBuckets = append(freeBuckets, b.nextOffset)
		}
	}

	// 收集空闲bucket。
	if len(freeBuckets) > 0 {
		t.meta.FreeBuckets = append(t.meta.FreeBuckets, freeBuckets...)
	}

	// 将所有槽位写入文件。
	if err := splitSlotWriter.writeSlots(); err != nil {
		return err
	}
	if err := newSlotWriter.writeSlots(); err != nil {
		return err
	}

	t.meta.NumBuckets++
	return nil
}

// 创建溢出bucket。
// 如果有空闲bucket，它将重用空闲bucket。
// 否则，它将在溢出文件中创建一个新的bucket。
func (t *Table) createOverflowBucket() (*bucket, error) {
	var offset int64
	if len(t.meta.FreeBuckets) > 0 {
		offset = t.meta.FreeBuckets[0]
		t.meta.FreeBuckets = t.meta.FreeBuckets[1:]
	} else {
		offset = t.overflowFile.Size()
		err := t.overflowFile.Truncate(int64(t.meta.BucketSize))
		if err != nil {
			return nil, err
		}
	}

	return &bucket{
		file:       t.overflowFile,
		offset:     offset,
		bucketSize: t.meta.BucketSize,
	}, nil
}
