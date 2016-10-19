package DataAn.storm.zookeeper;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@SuppressWarnings("serial")
public class TaskSelected implements  Serializable{

	private final int id;
	
	private final ExecutorService executorService; 
	
	private NodeWorker nodeWorker=null;

	public class SelectedThread extends Thread{
		
		public SelectedThread(Runnable runnable,String name) {
			super(runnable,name);
		}
		
	}
	
	TaskSelected(NodeWorker nodeWorker) {
		this.id = nodeWorker.getId();
		executorService=Executors.newFixedThreadPool(1,new ThreadFactory() {
			@Override
			public Thread newThread(Runnable r) {
				return new SelectedThread(r,"task-selected-"+id);
			}
		});
		this.nodeWorker=nodeWorker;
	}

	public void release(){
		executorService.execute(new Runnable() {
			@Override
			public void run() {
				try {
					nodeWorker.release();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}
	
	public void acquire(){
		executorService.execute(new Runnable() {
			@Override
			public void run() {
				try {
					nodeWorker.acquire();
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					wakeup();
				}
			}
		});
		while(true){
			try {
				synchronized (this) {
					wait();
				}
				break;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public boolean acquire(final long time, final TimeUnit unit) throws Exception{
		final AtomicBoolean atomicBoolean=new AtomicBoolean(false);
		executorService.execute(new Runnable() {
			@Override
			public void run() {
				try {
					if(nodeWorker.acquire(time,unit)){
						atomicBoolean.set(true);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}finally{
					wakeup();
				}
			}
		});
		while(true){
			try {
				synchronized (this) {
					wait();
				}
				break;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return atomicBoolean.get();
	}
	
	
	private void wakeup(){
		synchronized (this) {
			notifyAll();
		}
	}
	
}
