package test;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.RateLimiter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Tester implements AutoCloseable
{
    private final Logger log = LoggerFactory.getLogger(Tester.class);
    private final TestingCluster cluster;
    private final CuratorFramework mainClient;
    private final List<PathChildrenCache> caches = Lists.newArrayList();
    private final List<SinglePathTester> testers = Lists.newArrayList();
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final int pathQty;
    private final int nodesPerPath;
    private final CountingCacheListener listener = new CountingCacheListener();

    public static final String BASE_PATH = "/one/two/three";
    public static final byte[] PAYLOAD = "the quick brown box jumps over the lazy dog".getBytes();
    public static final String NAME_BASE = "node-";

    public Tester(int pathQty, int opsPerSecond, int deletePercent, int nodesPerPath, int clientQty, int serverQty, String connectionString)
    {
        log.info(String.format("pathQty: %d | opsPerSecond: %d | deletePercent: %d | nodesPerPath %d | clientQty: %d | serverQty: % d", pathQty, opsPerSecond, deletePercent, nodesPerPath, clientQty, serverQty));

        if ( connectionString != null )
        {
            log.info("Using external cluster: " + connectionString);
        }
        else
        {
            log.info("Using internal cluster of size: " + serverQty);
        }

        this.pathQty = pathQty;
        this.nodesPerPath = nodesPerPath;
        cluster = (connectionString == null) ? new TestingCluster(serverQty) : null;

        if ( cluster != null )
        {
            connectionString = cluster.getConnectString();
        }
        mainClient = newClient(connectionString);
        for ( int i = 0; i < pathQty; ++i )
        {
            PathChildrenCache cache = new PathChildrenCache(mainClient, makePath(i), true);
            cache.getListenable().addListener(listener);
            caches.add(cache);
        }

        RateLimiter rateLimiter = RateLimiter.create(opsPerSecond);
        for ( int i = 0; i < clientQty; ++i )
        {
            SinglePathTester tester = new SinglePathTester(connectionString, i, rateLimiter, deletePercent, nodesPerPath);
            testers.add(tester);
        }
    }

    public static CuratorFramework newClient(String connectionString)
    {
        return CuratorFrameworkFactory.newClient(connectionString, new ExponentialBackoffRetry(100, 3));
    }

    public CountingCacheListener getListener()
    {
        return listener;
    }

    public void start()
    {
        log.info("Starting");
        try
        {
            executorService.submit(new Topper(listener));
            if ( cluster != null )
            {
                cluster.start();
            }
            mainClient.start();

            log.info("Making initial nodes");
            for ( int i = 0; i < pathQty; ++i )
            {
                for ( int j = 0; j < nodesPerPath; ++j )
                {
                    mainClient.create().creatingParentsIfNeeded().forPath(makeChildPath(i, j), PAYLOAD);
                }
            }
            log.info("Nodes created");

            for ( PathChildrenCache cache : caches )
            {
                cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
            }
            for ( SinglePathTester tester : testers )
            {
                executorService.submit(tester);
            }
        }
        catch ( Exception e )
        {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close()
    {
        log.info("Stopping");

        for ( PathChildrenCache cache : caches )
        {
            CloseableUtils.closeQuietly(cache);
        }
        executorService.shutdownNow();
        try
        {
            if ( !executorService.awaitTermination(10, TimeUnit.SECONDS) )
            {
                log.error("Couldn't stop clients");
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            // ignore
        }
        for ( SinglePathTester tester : testers )
        {
            CloseableUtils.closeQuietly(tester);
        }
        CloseableUtils.closeQuietly(mainClient);
        CloseableUtils.closeQuietly(cluster);
    }

    public static String makePath(int index)
    {
        return ZKPaths.makePath(BASE_PATH, Integer.toString(index));
    }

    public static String makeChildPath(int index, int nodeNumber)
    {
        String basePath = makePath(index);
        return ZKPaths.makePath(basePath, NAME_BASE + nodeNumber);
    }

}
