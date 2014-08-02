package test;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CountingCacheListener implements PathChildrenCacheListener
{
    private final ConcurrentMap<PathChildrenCacheEvent.Type, AtomicInteger> counts = Maps.newConcurrentMap();

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
    {
        AtomicInteger newCount = new AtomicInteger(0);
        AtomicInteger oldCount = counts.putIfAbsent(event.getType(), newCount);
        AtomicInteger useCount = (oldCount != null) ? oldCount : newCount;
        useCount.incrementAndGet();
    }

    public Map<PathChildrenCacheEvent.Type, Integer> getCounts()
    {
        return Maps.transformValues
        (
            counts,
            new Function<AtomicInteger, Integer>()
            {
                @Override
                public Integer apply(AtomicInteger count)
                {
                    return count.get();
                }
            }
        );
    }
}
