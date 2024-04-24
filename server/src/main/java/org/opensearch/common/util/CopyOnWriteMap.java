/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util;

import java.util.*;
import java.util.function.UnaryOperator;

/**
 * CopyOnWriteMap implements a lazy copying scheme for maps.
 * It will defer making a deep copy of the map until an update is performed.
 * One an update it performed, it will, in perpetuity used that new copy.
 * @param <K> The key of the map
 * @param <V> The value of the map
 */
public class CopyOnWriteMap<K, V> implements Map<K, V> {

    private Map<K, V> base;
    private boolean owned;
    private UnaryOperator<Map<K, V>> copyConstructor;

    public CopyOnWriteMap(Map<K, V> base) {
        this(base, HashMap::new);
    }

    public CopyOnWriteMap(Map<K, V> base, UnaryOperator<Map<K, V>> copyConstructor) {
        Objects.requireNonNull(base);
        Objects.requireNonNull(copyConstructor);

        this.base = base;
        this.owned = false;
        this.copyConstructor = copyConstructor;
    }

    @Override
    public int size() {
        return base.size();
    }

    @Override
    public boolean isEmpty() {
        return base.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return base.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return base.containsValue(value);
    }

    @Override
    public V get(Object key) {
        return base.get(key);
    }

    @Override
    public V put(K key, V value) {
        return owned().put(key, value);
    }

    @Override
    public V remove(Object key) {
        return owned().remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        owned().putAll(m);
    }

    @Override
    public void clear() {
        owned().clear();
    }

    @Override
    public Set<K> keySet() {
        return Collections.unmodifiableSet(base.keySet());
    }

    @Override
    public Collection<V> values() {
        return Collections.unmodifiableCollection(base.values());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return Collections.unmodifiableSet(base.entrySet());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (this == obj || base == obj) {
            return true;
        }
        return base.equals(obj);
    }

    @Override
    public int hashCode() {
        return base.hashCode();
    }

    private Map<K, V> owned() {
        if (this.owned) {
            return this.base;
        }
        synchronized (this) {
            if (this.owned) {
                return this.base;
            }
            this.base = this.copyConstructor.apply(this.base);
            this.owned = true;
            this.copyConstructor = null; // discard the reference, we no longer need it
        }
        return this.base;
    }
}
