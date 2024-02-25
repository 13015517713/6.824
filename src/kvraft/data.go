package kvraft

type KVStore struct {
	data map[string]string
}

func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

func (kv *KVStore) Get(key string) (string, bool) {
	value, ok := kv.data[key]
	return value, ok
}

func (kv *KVStore) Put(key, value string) {
	kv.data[key] = value
}

func (kv *KVStore) Append(key, value string) {
	if _, ok := kv.data[key]; ok {
		kv.data[key] += value
	} else {
		kv.data[key] = value
	}
}

func (kv *KVStore) Delete(key string) {
	delete(kv.data, key)
}
