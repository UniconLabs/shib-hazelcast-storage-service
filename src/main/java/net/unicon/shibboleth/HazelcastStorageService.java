package net.unicon.shibboleth;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.opensaml.util.storage.StorageService;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HazelcastStorageService<K,V> implements StorageService<K,V> {
    private final HazelcastInstance hazelcastInstance;
    private final Map<String, IMap<K,V>> hzMaps = new ConcurrentHashMap<>();


    public HazelcastStorageService() {
        this(Hazelcast.newHazelcastInstance());
    }

    public HazelcastStorageService(final HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public boolean contains(String partition, K key) {
        if (key == null) {
            return false;
        }
        return hzMaps.containsKey(partition) && hzMaps.get(partition).containsKey(key);
    }

    @Override
    public Iterator<String> getPartitions() {
        return hzMaps.keySet().iterator();
    }

    @Override
    public Iterator<K> getKeys(String partition) {
        if (hzMaps.containsKey(partition)) {
            return hzMaps.get(partition).keySet().iterator();
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
            hzMaps.put(partition, (IMap<K, V>) this.hazelcastInstance.getMap(partition));
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
}
