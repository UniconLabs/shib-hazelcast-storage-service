package net.unicon.shibboleth;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.opensaml.util.storage.StorageService;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@SuppressWarnings("unused")
public class HazelcastStorageService<K,V> implements StorageService<K,V> {
    private final HazelcastInstance hazelcastInstance;
    private final Map<String, IMap<K,V>> hzMaps = new ConcurrentHashMap<>();


    @SuppressWarnings("unused")
    public HazelcastStorageService() {
        this(Hazelcast.newHazelcastInstance());
    }

    public HazelcastStorageService(final HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public boolean contains(String partition, K key) {
        return key != null && hzMaps.containsKey(partition) && hzMaps.get(partition).containsKey(key);
    }

    @Override
    public Iterator<String> getPartitions() {
        return new PartitionIterator();
    }

    @Override
    public Iterator<K> getKeys(String partition) {
        if (partition ==  null) {
            return null;
        }
        if (hzMaps.containsKey(partition)) {
            return new PartitionEntryIterator(partition);
        }
        return null;
    }

    @Override
    public V get(String partition, K key) {
        if (key == null) {
            return null;
        }

        if (hzMaps.containsKey(partition)) {
            return hzMaps.get(partition).get(key);
        }

        return null;
    }

    @Override
    public V put(String partition, K key, V value) {
        if (key == null) {
            return null;
        }

        if (!hzMaps.containsKey(partition)) {
            IMap<K, V> iMap = this.hazelcastInstance.getMap(partition);
            hzMaps.put(partition, iMap);
        }

        return hzMaps.get(partition).put(key, value);
    }

    @Override
    public V remove(String partition, K key) {
        if (key == null) {
            return null;
        }

        if (hzMaps.containsKey(partition)) {
            return hzMaps.get(partition).remove(key);
        }

        return null;
    }

    public class PartitionIterator implements Iterator<String> {
        private String currentPartition;
        private final Iterator<String> partitionIterator;

        public PartitionIterator() {
            this.partitionIterator = hzMaps.keySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return this.partitionIterator.hasNext();
        }

        @Override
        public String next() {
            this.currentPartition = this.partitionIterator.next();
            return this.currentPartition;
        }

        @Override
        public void remove() {
            hzMaps.get(this.currentPartition).clear();
            hzMaps.remove(this.currentPartition);
        }
    }

    public class PartitionEntryIterator implements Iterator<K> {
        private final String partition;
        private final Iterator<K> keyIterator;
        private K currentKey;

        public PartitionEntryIterator(String partition) {
            this.partition = partition;
            this.keyIterator = hzMaps.get(partition).keySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return this.keyIterator.hasNext();
        }

        @Override
        public K next() {
            this.currentKey = this.keyIterator.next();
            return this.currentKey;
        }

        @Override
        public void remove() {
            HazelcastStorageService.this.remove(this.partition, this.currentKey);
        }
    }
}
