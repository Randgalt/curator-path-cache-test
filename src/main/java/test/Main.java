package test;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Main
{
    public static void main(String[] args) throws Exception
    {
        OptionParser parser = new OptionParser();
        OptionSpec<Integer> pathQty = parser.accepts("path-qty", "number of PathChildrenCache instances").withOptionalArg().ofType(Integer.class).defaultsTo(5);
        OptionSpec<Integer> opsPerSecond = parser.accepts("ops-per-second", "number of ops per second").withOptionalArg().ofType(Integer.class).defaultsTo(100);
        OptionSpec<Integer> deletePercent = parser.accepts("delete-percent", "percentage (0-100) of delete ops vs write ops").withOptionalArg().ofType(Integer.class).defaultsTo(25);
        OptionSpec<Integer> nodesPerPath = parser.accepts("nodes-per-path", "number of nodes to have in each path").withOptionalArg().ofType(Integer.class).defaultsTo(3000);
        OptionSpec<Integer> clientQty = parser.accepts("client-qty", "number of clients to use for ops").withOptionalArg().ofType(Integer.class).defaultsTo(15);
        OptionSpec<Integer> serverQty = parser.accepts("server-qty", "number of ZooKeeper instances for internal cluster. Ignored for external clusters.").withOptionalArg().ofType(Integer.class).defaultsTo(3);
        OptionSpec<String> connectionString = parser.accepts("connection-string", "If using an external cluster, the connection string").withOptionalArg().ofType(String.class);
        final OptionSpec<Integer> testLength = parser.accepts("test-length", "test length in seconds").withOptionalArg().ofType(Integer.class).defaultsTo(60);
        parser.accepts("help", "prints this help");

        final OptionSet parsed = parser.parse(args);
        if ( parsed.has("help") )
        {
            parser.printHelpOn(System.out);
            return;
        }

        try ( final Tester tester = new Tester(pathQty.value(parsed), opsPerSecond.value(parsed), deletePercent.value(parsed), nodesPerPath.value(parsed), clientQty.value(parsed), serverQty.value(parsed), connectionString.value(parsed)) )
        {
            ExecutorService service = Executors.newSingleThreadExecutor();
            Runnable runner = new Runnable()
            {
                @Override
                public void run()
                {
                    try
                    {
                        tester.start();
                        TimeUnit.SECONDS.sleep(testLength.value(parsed));
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                    }
                    catch ( Throwable e )
                    {
                        LoggerFactory.getLogger(Main.class).error("Internal error", e);
                    }
                }
            };
            service.submit(runner);
            service.shutdown();
            service.awaitTermination(testLength.value(parsed), TimeUnit.SECONDS);
        }
    }
}
