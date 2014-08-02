package test;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.util.concurrent.ThreadLocalRandom;

public class SinglePathTester implements Closeable, Runnable
{
    private final Logger log;
    private final CuratorFramework client;
    private final int nodesPerPath;
    private final Tester tester;
    private final int index;
    private final RateLimiter rateLimiter;
    private final int deletePercent;

    public SinglePathTester(Tester tester, String connectionString, int index, RateLimiter rateLimiter, int deletePercent, int nodesPerPath)
    {
        this.tester = tester;
        this.index = index;
        this.rateLimiter = rateLimiter;
        this.deletePercent = deletePercent;
        log = LoggerFactory.getLogger(SinglePathTester.class.getName() + "-" + index);

        this.nodesPerPath = nodesPerPath;
        client = Tester.newClient(connectionString);
    }

    @Override
    public void run()
    {
        client.start();

        while ( !Thread.currentThread().isInterrupted() )
        {
            rateLimiter.acquire();

            int nodeNumber = ThreadLocalRandom.current().nextInt(nodesPerPath);
            int randomPercent = ThreadLocalRandom.current().nextInt(100);
            if ( randomPercent <= deletePercent )
            {
                try
                {
                    client.delete().inBackground().forPath(tester.makeChildPath(index, nodeNumber));
                    client.create().inBackground().forPath(tester.makeChildPath(index, nodeNumber), Tester.PAYLOAD);
                }
                catch ( Exception e )
                {
                    log.error("", e);
                }
            }
            else
            {
                try
                {
                    client.setData().inBackground().forPath(tester.makeChildPath(index, nodeNumber), Tester.PAYLOAD);
                }
                catch ( Exception e )
                {
                    log.error("", e);
                }
            }
        }
    }

    @Override
    public void close()
    {
        CloseableUtils.closeQuietly(client);
    }
}
