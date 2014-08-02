package test;

import com.sun.management.UnixOperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.management.ManagementFactory;

public class Topper implements Runnable
{
    private final Logger log = LoggerFactory.getLogger(Topper.class);
    private final CountingCacheListener listener;

    private static final int DISPLAY_SLEEP_MS = 5000;

    public Topper(CountingCacheListener listener)
    {
        this.listener = listener;
    }

    @Override
    public void run()
    {
        UnixOperatingSystemMXBean operatingSystemMXBean = (UnixOperatingSystemMXBean)ManagementFactory.getOperatingSystemMXBean();
        try
        {
            while ( !Thread.currentThread().isInterrupted() )
            {
                Thread.sleep(DISPLAY_SLEEP_MS);
                log.info("ProcessCpuLoad: " + (int)(100 * operatingSystemMXBean.getProcessCpuLoad()) + "%");
                log.info(String.valueOf(listener.getCounts()));
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
    }
}
