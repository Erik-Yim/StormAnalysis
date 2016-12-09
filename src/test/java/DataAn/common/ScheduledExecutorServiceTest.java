package DataAn.common;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class ScheduledExecutorServiceTest {

	public static void main(String[] args) {
		ScheduledExecutorService delayService=Executors.newScheduledThreadPool(1);
		delayService.schedule(new Runnable() {
			@Override
			public void run() {
				try {
					System.out.println("...");
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}, 10, TimeUnit.SECONDS);
	}
}
